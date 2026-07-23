from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
import unittest
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from cdw_extract import (
    ArtifactDescriptor,
    CancellationEnvelope,
    CancellationToken,
    CdwEngine,
    ExecutionContext,
    JobCallbackEvent,
    JobEnvelope,
    JobResult,
    JobStatus,
    ResourceBudget,
    RuntimeServices,
)
from cdw_extract.adapters import legacy_runtime_services
from cdw_extract.adapters.local import LocalArtifactStore
from cdw_extract import duck as duck_module
from cdw_extract.duck import connect


def envelope(job_type: str = "EXTRACT", **overrides) -> JobEnvelope:
    payload = {
        "schemaVersion": 2,
        "jobId": str(uuid.uuid4()),
        "jobType": job_type,
        "idempotencyKey": f"test:{uuid.uuid4()}",
        "command": {},
        "resourceBudget": {
            "cpuThreads": 1,
            "memoryBytes": 64 * 1024 * 1024,
            "tempBytes": 128 * 1024 * 1024,
        },
    }
    payload.update(overrides)
    return JobEnvelope.model_validate(payload)


class RecordingEvents:
    def __init__(self) -> None:
        self.statuses: list[JobStatus] = []
        self.details: list[dict] = []

    def emit(self, _envelope, status, details=None) -> None:
        self.statuses.append(status)
        self.details.append(dict(details or {}))


class StaticSecretProvider:
    @contextmanager
    def resolve(self, reference: str, *, purpose: str):
        if reference != "cdw-connection:profile-1" or purpose != "METADATA_REFRESH":
            raise KeyError(reference)
        yield {"username": "worker-user", "password": "worker-password"}


class ExecutionCoreTest(unittest.TestCase):
    @staticmethod
    def _without_nulls(value):
        if isinstance(value, dict):
            return {
                key: ExecutionCoreTest._without_nulls(item)
                for key, item in value.items()
                if item is not None
            }
        if isinstance(value, list):
            return [ExecutionCoreTest._without_nulls(item) for item in value]
        return value

    def test_transport_contract_uses_boot_uuid_and_camel_case(self):
        job_id = uuid.uuid4()
        parsed = envelope(jobId=str(job_id))
        serialized = parsed.model_dump(by_alias=True, mode="json")

        self.assertEqual(job_id, parsed.job_id)
        self.assertEqual(str(job_id), serialized["jobId"])
        self.assertEqual(2, serialized["schemaVersion"])
        self.assertEqual(1, serialized["resourceBudget"]["cpuThreads"])
        self.assertNotIn("callback", serialized)

    def test_cancellation_and_callback_contracts_use_boot_wire_aliases(self):
        job_id = uuid.uuid4()
        cancellation = CancellationEnvelope(
            jobId=job_id,
            idempotencyKey=f"cancel:{job_id}:1",
            reason="operator request",
        )
        self.assertEqual(
            {
                "schemaVersion": 2,
                "jobId": str(job_id),
                "idempotencyKey": f"cancel:{job_id}:1",
                "reason": "operator request",
            },
            cancellation.transport_dict(),
        )

        result = JobResult(
            jobId=job_id,
            jobType="EXTRACT",
            status="SUCCESS",
            artifacts=(
                ArtifactDescriptor(
                    store="local",
                    key="jobs/result.parquet",
                    sha256="a" * 64,
                    sizeBytes=123,
                    rowCount=7,
                    contentType="application/octet-stream",
                    format="parquet",
                ),
            ),
            metrics={"rowCount": 7, "sizeBytes": 123},
        )
        first = JobCallbackEvent.from_result(result, sequence=9, attempt=2, queue_job_id="queue-7")
        redelivery = JobCallbackEvent.from_result(result, sequence=9, attempt=2, queue_job_id="queue-7")
        payload = first.transport_dict()

        self.assertEqual(first.event_id, redelivery.event_id)
        self.assertEqual(str(job_id), payload["jobId"])
        self.assertEqual("queue-7", payload["queueJobId"])
        self.assertEqual(7, payload["processedRows"])
        self.assertEqual(123, payload["processedBytes"])
        self.assertEqual({"rowCount": 7, "sizeBytes": 123}, payload["metrics"])
        self.assertEqual("LOCAL", payload["artifacts"][0]["store"])
        self.assertNotIn("job_id", payload)
        self.assertEqual(first, JobCallbackEvent.model_validate(payload))

        failed = JobResult(
            jobId=job_id,
            jobType="EXTRACT",
            status="FAILED",
            error={
                "code": "RESOURCE_LIMIT_EXCEEDED",
                "message": "memory budget exceeded",
                "retryable": False,
            },
        )
        failed_callback = JobCallbackEvent.from_result(failed, sequence=10, attempt=2)
        self.assertEqual("RESOURCE", failed_callback.error.category)

    def test_boot_and_python_v2_contract_fixtures_round_trip(self):
        fixtures = Path(__file__).parent / "contracts"
        envelope_payload = json.loads(
            (fixtures / "boot-job-envelope-v2.json").read_text(encoding="utf-8")
        )
        parsed_envelope = JobEnvelope.model_validate(envelope_payload)
        self.assertEqual(envelope_payload, parsed_envelope.transport_dict())
        extract_request = parsed_envelope.command["request"]
        self.assertEqual(1, extract_request["pipeline"]["pipelineVersion"])
        self.assertTrue(extract_request["pipeline"]["steps"])
        self.assertTrue(extract_request["sourceColumns"])
        self.assertTrue(extract_request["expectedPipelineHash"].startswith("sha256:"))
        self.assertTrue(extract_request["expectedSourceSchemaHash"].startswith("sha256:"))
        self.assertEqual("1", extract_request["expectedCompilerVersion"])

        for fixture_name in (
            "boot-success-callback-v2.json",
            "boot-failed-callback-v2.json",
        ):
            with self.subTest(fixture=fixture_name):
                callback_payload = json.loads(
                    (fixtures / fixture_name).read_text(encoding="utf-8")
                )
                parsed_callback = JobCallbackEvent.model_validate(callback_payload)
                self.assertEqual(
                    self._without_nulls(callback_payload),
                    parsed_callback.transport_dict(),
                )

    def test_artifact_descriptor_rejects_absolute_local_paths(self):
        with self.assertRaisesRegex(ValueError, "store-relative"):
            ArtifactDescriptor(
                store="local",
                key="C:/private/data.parquet",
                sha256="0" * 64,
                sizeBytes=1,
                contentType="application/octet-stream",
                format="PARQUET",
            )

    def test_artifact_descriptor_requires_boot_registration_fields(self):
        with self.assertRaisesRegex(ValueError, "sha256"):
            ArtifactDescriptor(
                store="local",
                key="artifacts/data.parquet",
                sizeBytes=1,
                contentType="application/octet-stream",
                format="PARQUET",
            )
        with self.assertRaisesRegex(ValueError, "sha256"):
            ArtifactDescriptor(
                store="local",
                key="artifacts/data.parquet",
                sha256="not-a-checksum",
                sizeBytes=1,
                contentType="application/octet-stream",
                format="PARQUET",
            )
        with self.assertRaisesRegex(ValueError, "format"):
            ArtifactDescriptor(
                store="local",
                key="artifacts/data.parquet",
                sha256="A" * 64,
                sizeBytes=1,
                contentType="application/octet-stream",
            )

    def test_wire_integer_bounds_and_terminal_job_result_status_match_boot(self):
        java_long_max = (1 << 63) - 1
        java_integer_max = (1 << 31) - 1
        job_id = uuid.uuid4()

        budget = ResourceBudget(
            memoryBytes=java_long_max,
            tempBytes=java_long_max,
            inputBytes=java_long_max,
            outputBytes=java_long_max,
            rowLimit=java_long_max,
        )
        artifact = ArtifactDescriptor(
            store="LOCAL",
            key="jobs/result.parquet",
            sha256="a" * 64,
            sizeBytes=java_long_max,
            rowCount=java_long_max,
            contentType="application/octet-stream",
            format="PARQUET",
        )
        callback = JobCallbackEvent(
            eventId=uuid.uuid4(),
            sequence=java_long_max,
            jobId=job_id,
            status="SUCCESS",
            attempt=java_integer_max,
            processedRows=java_long_max,
            processedBytes=java_long_max,
            artifacts=(artifact,),
        )

        self.assertEqual(java_long_max, budget.memory_bytes)
        self.assertEqual(java_long_max, artifact.size_bytes)
        self.assertEqual(java_long_max, callback.sequence)
        self.assertEqual(java_integer_max, callback.attempt)

        def make_artifact(**overrides):
            payload = {
                "store": "LOCAL",
                "key": "jobs/result.parquet",
                "sha256": "a" * 64,
                "sizeBytes": 1,
                "contentType": "application/octet-stream",
                "format": "PARQUET",
            }
            payload.update(overrides)
            return ArtifactDescriptor.model_validate(payload)

        def make_callback(**overrides):
            payload = {
                "eventId": str(uuid.uuid4()),
                "sequence": 1,
                "jobId": str(job_id),
                "status": "SUCCESS",
                "attempt": 1,
            }
            payload.update(overrides)
            return JobCallbackEvent.model_validate(payload)

        overflow_cases = {
            "memoryBytes": lambda: ResourceBudget(memoryBytes=java_long_max + 1),
            "tempBytes": lambda: ResourceBudget(tempBytes=java_long_max + 1),
            "inputBytes": lambda: ResourceBudget(inputBytes=java_long_max + 1),
            "outputBytes": lambda: ResourceBudget(outputBytes=java_long_max + 1),
            "rowLimit": lambda: ResourceBudget(rowLimit=java_long_max + 1),
            "artifact.sizeBytes": lambda: make_artifact(sizeBytes=java_long_max + 1),
            "artifact.rowCount": lambda: make_artifact(rowCount=java_long_max + 1),
            "callback.sequence": lambda: make_callback(sequence=java_long_max + 1),
            "callback.attempt": lambda: make_callback(attempt=java_integer_max + 1),
            "callback.processedRows": lambda: make_callback(processedRows=java_long_max + 1),
            "callback.processedBytes": lambda: make_callback(processedBytes=java_long_max + 1),
        }
        for field_name, factory in overflow_cases.items():
            with self.subTest(field=field_name), self.assertRaises(ValueError):
                factory()

        self.assertEqual(
            JobStatus.SUCCESS,
            JobResult(jobId=job_id, jobType="EXTRACT", status="SUCCESS").status,
        )
        self.assertEqual(
            JobStatus.CANCELLED,
            JobResult(jobId=job_id, jobType="EXTRACT", status="CANCELLED").status,
        )
        self.assertEqual(
            JobStatus.FAILED,
            JobResult(
                jobId=job_id,
                jobType="EXTRACT",
                status="FAILED",
                error={"code": "FAILED", "message": "failed"},
            ).status,
        )
        for non_terminal in (
            "DISPATCH_PENDING",
            "QUEUED",
            "RUNNING",
            "CANCEL_REQUESTED",
        ):
            with self.subTest(status=non_terminal), self.assertRaisesRegex(
                ValueError,
                "must be SUCCESS, FAILED, or CANCELLED",
            ):
                JobResult(jobId=job_id, jobType="EXTRACT", status=non_terminal)

    def test_engine_applies_per_job_duckdb_budget_and_temp_workspace(self):
        observed: dict[str, object] = {}
        events = RecordingEvents()

        def handler(_envelope, _context, _services):
            connection = connect(operation="core-test", operation_id="budget")
            try:
                observed["settings"] = connection.execute(
                    "SELECT current_setting('threads'), current_setting('memory_limit'), "
                    "current_setting('max_temp_directory_size')"
                ).fetchone()
                observed["temp"] = connection.temp_directory
                self.assertTrue(connection.temp_directory.exists())
            finally:
                connection.close()
            return {"metrics": {"rowCount": 3}}

        with tempfile.TemporaryDirectory() as workspace:
            engine = CdwEngine(
                RuntimeServices(
                    handlers={"EXTRACT": handler},
                    workspace_root=Path(workspace),
                    events=events,
                )
            )
            result = engine.execute(envelope())

        self.assertEqual(JobStatus.SUCCESS, result.status)
        self.assertEqual({"rowCount": 3}, result.metrics)
        self.assertEqual((1, "64.0 MiB", "128.0 MiB"), observed["settings"])
        self.assertIn("duckdb", Path(observed["temp"]).parts)
        self.assertFalse(Path(observed["temp"]).exists())
        self.assertEqual([JobStatus.RUNNING, JobStatus.SUCCESS], events.statuses)
        self.assertEqual([1, 2], [item["eventSequence"] for item in events.details])
        self.assertEqual([1, 1], [item["attempt"] for item in events.details])

    def test_engine_resumes_global_event_sequence_and_emits_success_artifacts(self):
        events = RecordingEvents()
        artifact = ArtifactDescriptor(
            store="LOCAL",
            key="jobs/result.parquet",
            sha256="b" * 64,
            sizeBytes=10,
            rowCount=2,
            contentType="application/octet-stream",
            format="PARQUET",
        )
        engine = CdwEngine(
            RuntimeServices(
                handlers={"EXTRACT": lambda *_: {"artifacts": (artifact,), "metrics": {"rowCount": 2}}},
                events=events,
            )
        )

        result = engine.execute(envelope(), ExecutionContext(attempt=3, event_sequence_start=40))

        self.assertEqual(JobStatus.SUCCESS, result.status)
        self.assertEqual([41, 42], [item["eventSequence"] for item in events.details])
        self.assertEqual([3, 3], [item["attempt"] for item in events.details])
        self.assertEqual(2, events.details[-1]["metrics"]["rowCount"])
        self.assertEqual("jobs/result.parquet", events.details[-1]["artifacts"][0]["key"])

    def test_duckdb_budget_is_capped_by_aggregate_host_limits(self):
        environment = {
            "DUCKDB_TOTAL_THREADS": "2",
            "DUCKDB_TOTAL_MEMORY_BYTES": str(64 * 1024 * 1024),
            "DUCKDB_TOTAL_TEMP_BYTES": str(128 * 1024 * 1024),
            "DUCKDB_MAX_CONCURRENT_OPERATIONS": "4",
        }
        budget = ResourceBudget(
            cpuThreads=8,
            memoryBytes=256 * 1024 * 1024,
            tempBytes=512 * 1024 * 1024,
        )
        with tempfile.TemporaryDirectory() as data_root, patch.object(
            duck_module, "_resource_governor", None
        ), patch.object(
            duck_module, "_operation_slots", threading.BoundedSemaphore(4)
        ), patch.dict(
            "os.environ", environment
        ):
            connection = connect(data_root, "budget-cap", "one", budget=budget)
            try:
                settings = connection.execute(
                    "SELECT current_setting('threads'), current_setting('memory_limit'), "
                    "current_setting('max_temp_directory_size')"
                ).fetchone()
                self.assertEqual(2, connection.effective_threads)
                self.assertEqual(64 * 1024 * 1024, connection.effective_memory_bytes)
                self.assertEqual(128 * 1024 * 1024, connection.effective_temp_bytes)
                self.assertEqual((2, "64.0 MiB", "128.0 MiB"), settings)
            finally:
                connection.close()

    def test_cancelled_context_does_not_invoke_handler(self):
        called = False

        def handler(_envelope, _context, _services):
            nonlocal called
            called = True

        token = CancellationToken()
        token.cancel()
        result = CdwEngine(RuntimeServices(handlers={"EXTRACT": handler})).execute(
            envelope(),
            ExecutionContext(cancellation=token),
        )

        self.assertFalse(called)
        self.assertEqual(JobStatus.CANCELLED, result.status)
        self.assertIsNone(result.error)

    def test_cancellation_registry_applies_a_pre_execution_tombstone(self):
        called = False
        job = envelope()

        def handler(_envelope, _context, _services):
            nonlocal called
            called = True

        services = RuntimeServices(handlers={"EXTRACT": handler})
        command = CancellationEnvelope(
            jobId=job.job_id,
            idempotencyKey=f"cancel:{job.job_id}:1",
            reason="cancelled while queued",
        )
        self.assertTrue(services.cancellations.cancel(command))
        self.assertFalse(services.cancellations.cancel(command))

        result = CdwEngine(services).execute(job)

        self.assertFalse(called)
        self.assertEqual(JobStatus.CANCELLED, result.status)
        services.cancellations.forget(job.job_id)

    def test_cancellation_token_interrupts_duckdb_and_returns_cancelled(self):
        token = CancellationToken()
        started = threading.Event()
        holder: dict[str, object] = {}

        def handler(_envelope, _context, _services):
            connection = connect(operation="core-cancel", operation_id="running-query")
            try:
                started.set()
                connection.execute(
                    "SELECT sum(i * j) FROM range(100000000) AS left_side(i), "
                    "range(100) AS right_side(j)"
                ).fetchone()
            finally:
                connection.close()

        engine = CdwEngine(RuntimeServices(handlers={"EXTRACT": handler}))
        runner = threading.Thread(
            target=lambda: holder.update(
                result=engine.execute(envelope(), ExecutionContext(cancellation=token))
            )
        )
        runner.start()
        self.assertTrue(started.wait(2), "handler did not start its DuckDB query")
        token.cancel()
        runner.join(5)

        self.assertFalse(runner.is_alive())
        self.assertEqual(JobStatus.CANCELLED, holder["result"].status)

    def test_execution_context_rejects_mismatched_job_correlation(self):
        job = envelope()
        context = ExecutionContext(job_id=uuid.uuid4(), attempt=2)
        result = CdwEngine(RuntimeServices(handlers={"EXTRACT": lambda *_: None})).execute(
            job,
            context,
        )

        self.assertEqual(JobStatus.FAILED, result.status)
        self.assertEqual("ValueError", result.error.code)
        self.assertIn("does not match", result.error.message)

    def test_deadline_and_handler_failures_become_typed_results(self):
        expired = envelope(
            resourceBudget={
                "cpuThreads": 1,
                "memoryBytes": 64 * 1024 * 1024,
                "tempBytes": 64 * 1024 * 1024,
                "deadline": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
            }
        )
        engine = CdwEngine(RuntimeServices(handlers={"EXTRACT": lambda *_: None}))
        deadline_result = engine.execute(expired)
        self.assertEqual(JobStatus.FAILED, deadline_result.status)
        self.assertEqual("DEADLINE_EXCEEDED", deadline_result.error.code)

        failed = CdwEngine(
            RuntimeServices(
                handlers={"EXTRACT": lambda *_: (_ for _ in ()).throw(ValueError("invalid command"))}
            )
        ).execute(envelope())
        self.assertEqual(JobStatus.FAILED, failed.status)
        self.assertEqual("ValueError", failed.error.code)
        self.assertEqual("invalid command", failed.error.message)

    def test_output_byte_and_row_budgets_fail_closed(self):
        artifact = ArtifactDescriptor(
            store="LOCAL",
            key="jobs/large.parquet",
            sha256="c" * 64,
            sizeBytes=101,
            rowCount=11,
            contentType="application/octet-stream",
            format="PARQUET",
        )
        engine = CdwEngine(
            RuntimeServices(handlers={"EXTRACT": lambda *_: {"artifacts": (artifact,)}})
        )
        byte_limited = envelope(
            resourceBudget={
                "cpuThreads": 1,
                "memoryBytes": 64 * 1024 * 1024,
                "tempBytes": 64 * 1024 * 1024,
                "outputBytes": 100,
            }
        )
        row_limited = envelope(
            resourceBudget={
                "cpuThreads": 1,
                "memoryBytes": 64 * 1024 * 1024,
                "tempBytes": 64 * 1024 * 1024,
                "rowLimit": 10,
            }
        )

        for job in (byte_limited, row_limited):
            with self.subTest(limit=job.resource_budget):
                result = engine.execute(job)
                self.assertEqual(JobStatus.FAILED, result.status)
                self.assertEqual("RESOURCE_LIMIT_EXCEEDED", result.error.code)
                self.assertEqual((), result.artifacts)

    def test_saved_pipeline_rejects_a_different_compiler_version(self):
        from cdw_extract.transforms.runtime import compile_pipeline_request

        with self.assertRaisesRegex(ValueError, "PIPELINE_COMPILER_VERSION_MISMATCH"):
            compile_pipeline_request(
                "connection-1",
                {"expectedCompilerVersion": "2"},
                ".",
                object(),
            )

    def test_handler_returned_failed_result_emits_structured_error(self):
        events = RecordingEvents()
        job = envelope()
        failed = JobResult(
            jobId=job.job_id,
            jobType=job.job_type,
            status="FAILED",
            error={"code": "SOURCE_UNAVAILABLE", "message": "source is offline", "retryable": True},
        )
        result = CdwEngine(
            RuntimeServices(handlers={"EXTRACT": lambda *_: failed}, events=events)
        ).execute(job)

        self.assertEqual(JobStatus.FAILED, result.status)
        self.assertEqual("SOURCE_UNAVAILABLE", events.details[-1]["error"]["code"])
        self.assertTrue(events.details[-1]["retryable"])

    def test_legacy_extract_adapter_returns_store_key_not_absolute_path(self):
        with tempfile.TemporaryDirectory() as data_root:
            job = envelope(
                command={
                    "connectionId": "connection-1",
                    "request": {
                        "requestId": "request-1",
                        "sourceType": "table",
                        "tableId": "table-1",
                        "outputFormat": "parquet",
                    },
                }
            )
            services = legacy_runtime_services(data_root)
            with patch("cdw_extract.extract.final_query", return_value="SELECT 1 AS patient_id"):
                result = CdwEngine(services).execute(job)

            self.assertEqual(JobStatus.SUCCESS, result.status)
            self.assertEqual(1, len(result.artifacts))
            artifact = result.artifacts[0]
            self.assertEqual("LOCAL", artifact.store)
            self.assertFalse(Path(artifact.key).is_absolute())
            self.assertEqual(1, artifact.row_count)
            self.assertTrue((Path(data_root) / artifact.key).is_file())
            self.assertNotIn("filePath", result.metrics)

    def test_dataset_convert_materializes_a_verified_input_artifact(self):
        with tempfile.TemporaryDirectory() as data_root:
            root = Path(data_root)
            source = root / "incoming" / "patients.csv"
            source.parent.mkdir(parents=True)
            source.write_text("patient_id,name\n1,Alice\n2,Bob\n", encoding="utf-8")
            services = legacy_runtime_services(root)
            input_artifact = services.artifact_store.describe("incoming/patients.csv")
            job = envelope(
                job_type="DATASET_CONVERT",
                command={
                    "input": input_artifact.transport_dict(),
                    "request": {
                        "userId": "user-1",
                        "userDatasetId": "dataset-1",
                        "userDatasetFileId": "file-1",
                        "originalFileName": "patients.csv",
                        "fileType": "CSV",
                        "headerYn": True,
                        "delimiter": ",",
                        "fileEncoding": "UTF-8",
                    },
                },
            )

            result = CdwEngine(services).execute(job)

            self.assertEqual(JobStatus.SUCCESS, result.status, result.error)
            self.assertEqual(2, result.metrics["rowCount"])
            self.assertEqual(1, len(result.artifacts))
            self.assertTrue((root / result.artifacts[0].key).is_file())

    def test_dataset_convert_rejects_a_tampered_input_descriptor(self):
        with tempfile.TemporaryDirectory() as data_root:
            root = Path(data_root)
            source = root / "incoming" / "patients.csv"
            source.parent.mkdir(parents=True)
            source.write_text("patient_id\n1\n", encoding="utf-8")
            services = legacy_runtime_services(root)
            descriptor = services.artifact_store.describe("incoming/patients.csv").model_copy(
                update={"sha256": "0" * 64}
            )
            job = envelope(
                job_type="DATASET_CONVERT",
                command={
                    "input": descriptor.transport_dict(),
                    "request": {
                        "userId": "user-1",
                        "userDatasetId": "dataset-1",
                        "userDatasetFileId": "file-1",
                        "originalFileName": "patients.csv",
                        "fileType": "CSV",
                    },
                },
            )

            result = CdwEngine(services).execute(job)

            self.assertEqual(JobStatus.FAILED, result.status)
            self.assertIn("checksum or size", result.error.message)

    def test_metadata_refresh_resolves_credentials_only_at_execution_time(self):
        with tempfile.TemporaryDirectory() as data_root:
            services = legacy_runtime_services(data_root)
            services.secret_provider = StaticSecretProvider()
            job = envelope(
                job_type="METADATA_REFRESH",
                command={
                    "connectionId": "connection-1",
                    "secretRef": "cdw-connection:profile-1",
                    "request": {
                        "sourceConnection": {
                            "vendor": "postgresql",
                            "host": "db.internal",
                            "database": "warehouse",
                            "username": None,
                            "password": None,
                        },
                        "tables": [{"tableId": "patients", "tableName": "patients"}],
                    },
                },
            )
            observed_request = {}

            def refresh(_connection_id, request, _root, _job_id, _budget=None):
                observed_request.update(request)
                return {"tables": [], "tableCount": 0, "rowCount": 0}

            with patch("cdw_extract.refresh.refresh_tables_impl", side_effect=refresh):
                result = CdwEngine(services).execute(job)

            self.assertEqual(JobStatus.SUCCESS, result.status, result.error)
            self.assertEqual("worker-user", observed_request["sourceConnection"]["username"])
            self.assertEqual("worker-password", observed_request["sourceConnection"]["password"])
            self.assertNotIn("worker-password", job.transport_json())

    def test_metadata_refresh_rejects_inline_credentials(self):
        with tempfile.TemporaryDirectory() as data_root:
            services = legacy_runtime_services(data_root)
            services.secret_provider = StaticSecretProvider()
            job = envelope(
                job_type="METADATA_REFRESH",
                command={
                    "connectionId": "connection-1",
                    "secretRef": "cdw-connection:profile-1",
                    "request": {
                        "sourceConnection": {
                            "username": "inline-user",
                            "password": "inline-password",
                        },
                        "tables": [{"tableId": "patients", "tableName": "patients"}],
                    },
                },
            )

            result = CdwEngine(services).execute(job)

            self.assertEqual(JobStatus.FAILED, result.status)
            self.assertIn("inline credentials", result.error.message)

    def test_local_artifact_publish_never_overwrites_concurrent_winner(self):
        with tempfile.TemporaryDirectory() as root:
            root_path = Path(root)
            left = root_path / "left.part"
            right = root_path / "right.part"
            left.write_bytes(b"left-content")
            right.write_bytes(b"right-content")
            store = LocalArtifactStore(root_path)
            barrier = threading.Barrier(2)
            successes = []
            errors = []

            def publish(source: Path) -> None:
                barrier.wait()
                try:
                    successes.append(
                        store.publish(
                            source,
                            "artifacts/result.parquet",
                            idempotency_key="same-key",
                        )
                    )
                except Exception as exc:
                    errors.append(exc)

            workers = [
                threading.Thread(target=publish, args=(left,)),
                threading.Thread(target=publish, args=(right,)),
            ]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(3)

            self.assertEqual(1, len(successes))
            self.assertEqual(1, len(errors))
            self.assertIsInstance(errors[0], FileExistsError)
            self.assertIn(
                (root_path / "artifacts" / "result.parquet").read_bytes(),
                {b"left-content", b"right-content"},
            )
            self.assertEqual([], list((root_path / "artifacts").glob("*.tmp")))

    def test_analysis_artifact_executes_through_core_without_http_callback(self):
        from tests.test_analytics import USER_ID, create_source, request_payload

        with tempfile.TemporaryDirectory() as data_root:
            create_source(data_root)
            job_id = uuid.uuid4()
            query = request_payload(
                "BAR",
                {
                    "category": {"column": "category"},
                    "value": {"column": "value", "aggregation": "SUM"},
                },
            )
            request = {
                "schemaVersion": 1,
                "jobId": str(job_id),
                "requestId": "core-analysis-artifact",
                "analysisArtifactId": "artifact-from-core",
                "analysisId": "analysis-from-core",
                "userId": USER_ID,
                "name": "Core dashboard",
                "format": "PNG",
                "spec": {
                    "specVersion": 2,
                    "title": "Core dashboard",
                    "dashboard": {
                        "charts": [{"chartId": "chart-1", "options": {}}],
                    },
                    "queries": [
                        {
                            "chartId": "chart-1",
                            "title": "Totals",
                            "query": query,
                            "layout": {
                                "chartId": "chart-1",
                                "x": 0,
                                "y": 0,
                                "w": 6,
                                "h": 4,
                            },
                        }
                    ],
                },
                "callback": {
                    "url": "http://boot.invalid/callback",
                    "timeoutSeconds": 1,
                },
            }
            job = envelope(
                job_type="ANALYSIS_ARTIFACT",
                jobId=str(job_id),
                command={"request": request},
            )
            with patch("cdw_extract.analytics_artifacts.requests.post") as post:
                result = CdwEngine(legacy_runtime_services(data_root)).execute(job)

            self.assertEqual(JobStatus.SUCCESS, result.status, result.error)
            self.assertEqual(1, len(result.artifacts))
            artifact = result.artifacts[0]
            self.assertEqual("LOCAL", artifact.store)
            self.assertEqual("PNG", artifact.format)
            self.assertRegex(artifact.sha256, r"^[0-9a-f]{64}$")
            output = Path(data_root) / artifact.key
            self.assertTrue(output.read_bytes().startswith(b"\x89PNG\r\n\x1a\n"))
            self.assertEqual(1, result.metrics["chartCount"])
            self.assertEqual("artifact-from-core", result.metrics["analysisArtifactId"])
            self.assertNotIn("filePath", result.metrics)
            post.assert_not_called()

    def test_package_import_is_lazy_and_distribution_finds_subpackages(self):
        command = [
            sys.executable,
            "-c",
            "import sys, cdw_extract; "
            "assert 'cdw_extract.jobs' not in sys.modules; "
            "assert 'cdw_extract.callback' not in sys.modules; "
            "print(cdw_extract.CdwEngine.__name__)",
        ]
        completed = subprocess.run(
            command,
            cwd=Path(__file__).parents[1],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual("CdwEngine", completed.stdout.strip())
        project_root = Path(__file__).parents[1]
        packaging = (project_root / "pyproject.toml").read_text(encoding="utf-8")
        self.assertIn("[tool.setuptools.packages.find]", packaging)
        self.assertIn('include = ["cdw_extract*"]', packaging)
        self.assertTrue((project_root / "cdw_extract" / "transforms" / "__init__.py").is_file())
        self.assertTrue((project_root / "cdw_extract" / "adapters" / "__init__.py").is_file())


if __name__ == "__main__":
    unittest.main()
