from __future__ import annotations

import csv
import importlib
import tempfile
import threading
import unittest
import uuid
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb

from cdw_extract import (
    CancellationToken,
    CdwEngine,
    ExecutionContext,
    JobEnvelope,
    JobStatus,
    ResourceBudget,
)
from cdw_extract.adapters import legacy_runtime_services
from cdw_extract.analytics import _apply_connection_limits
from cdw_extract.clickhouse import write_clickhouse_table_parquet
from cdw_extract.errors import JobCancelled, ResourceLimitExceeded
from cdw_extract.execution_scope import ExecutionResources, execution_resource_scope
from cdw_extract.manifest import load_connection_manifest
from cdw_extract.refresh import refresh_tables_impl
from cdw_extract.user_dataset import write_normalized_csv


duck_module = importlib.import_module("cdw_extract.duck")
extract_module = importlib.import_module("cdw_extract.extract")


class _RecordingConnection:
    def __init__(self) -> None:
        self.connection = duckdb.connect(database=":memory:")

    def execute(self, sql, parameters=None):
        return self.connection.execute(sql, parameters or [])

    def close(self) -> None:
        self.connection.close()


class ResourceSafetyV2Test(unittest.TestCase):
    def test_clickhouse_stream_enforces_byte_cap_and_removes_partial_file(self) -> None:
        class Response:
            text = ""

            def __init__(self) -> None:
                self.closed = False

            def __enter__(self):
                return self

            def __exit__(self, _exc_type, _exc, _traceback) -> None:
                self.close()

            def close(self) -> None:
                self.closed = True

            def iter_content(self, chunk_size):
                self.asserted_chunk_size = chunk_size
                yield b"1234"
                yield b"5678"

        response = Response()
        with tempfile.TemporaryDirectory() as root, execution_resource_scope(
            ExecutionResources(
                budget=ResourceBudget(
                    cpuThreads=1,
                    memoryBytes=64 * 1024 * 1024,
                    tempBytes=64 * 1024 * 1024,
                    outputBytes=6,
                ),
                temp_root=Path(root),
                cancellation=CancellationToken(),
            )
        ), patch(
            "cdw_extract.clickhouse.post_query", return_value=response
        ) as post:
            output = Path(root) / "patients.parquet"
            with self.assertRaises(ResourceLimitExceeded):
                write_clickhouse_table_parquet(
                    {"database": "test"},
                    {"tableName": "patients"},
                    output,
                )

            self.assertFalse(output.exists())
            self.assertTrue(response.closed)
            self.assertEqual(1, post.call_count)

    def test_clickhouse_stream_observes_execution_scope_cancellation(self) -> None:
        token = CancellationToken()

        class Response:
            text = ""

            def __enter__(self):
                return self

            def __exit__(self, _exc_type, _exc, _traceback) -> None:
                self.close()

            def close(self) -> None:
                pass

            def iter_content(self, chunk_size):
                yield b"first"
                token.cancel()
                yield b"second"

        with tempfile.TemporaryDirectory() as root, execution_resource_scope(
            ExecutionResources(
                budget=ResourceBudget(
                    cpuThreads=1,
                    memoryBytes=64 * 1024 * 1024,
                    tempBytes=64 * 1024 * 1024,
                ),
                temp_root=Path(root),
                cancellation=token,
            )
        ), patch("cdw_extract.clickhouse.post_query", return_value=Response()) as post:
            output = Path(root) / "patients.parquet"
            with self.assertRaises(JobCancelled):
                write_clickhouse_table_parquet(
                    {"database": "test"},
                    {"tableName": "patients"},
                    output,
                )

            self.assertFalse(output.exists())
            self.assertEqual(1, post.call_count)

    def test_analytics_request_limits_cannot_exceed_the_governor_lease(self) -> None:
        class Connection:
            effective_threads = 2
            effective_memory_bytes = 64 * 1024 * 1024

            def __init__(self) -> None:
                self.statements: list[str] = []

            def execute(self, statement: str) -> None:
                self.statements.append(statement)

        connection = Connection()
        _apply_connection_limits(
            connection,
            SimpleNamespace(threads=8, memory_limit_mb=2048),
        )

        self.assertEqual(
            ["SET memory_limit='67108864B'", "SET threads=2"],
            connection.statements,
        )

    def test_cancellation_interrupts_a_job_waiting_for_a_duckdb_slot(self) -> None:
        slots = threading.BoundedSemaphore(1)
        slots.acquire()
        token = CancellationToken()
        entered = threading.Event()
        holder: dict[str, object] = {}
        job = JobEnvelope(
            jobId=uuid.uuid4(),
            jobType="EXTRACT",
            idempotencyKey="queued-cancel",
            command={},
            resourceBudget={
                "cpuThreads": 1,
                "memoryBytes": 64 * 1024 * 1024,
                "tempBytes": 64 * 1024 * 1024,
            },
        )

        def handler(_envelope, _context, _services):
            entered.set()
            connection = duck_module.connect(operation="queued-cancel")
            connection.close()

        services = legacy_runtime_services(tempfile.gettempdir())
        services.handlers = {"EXTRACT": handler}
        runner = threading.Thread(
            target=lambda: holder.update(
                result=CdwEngine(services).execute(job, ExecutionContext(cancellation=token))
            )
        )
        try:
            with patch.object(duck_module, "_operation_slots", slots), patch.dict(
                "os.environ", {"DUCKDB_OPERATION_QUEUE_TIMEOUT_SECONDS": "10"}
            ):
                runner.start()
                self.assertTrue(entered.wait(2))
                token.cancel()
                runner.join(2)
        finally:
            slots.release()
            if runner.is_alive():
                runner.join(2)

        self.assertFalse(runner.is_alive())
        self.assertEqual(JobStatus.CANCELLED, holder["result"].status)

    def test_normalized_csv_uses_disk_spooling_and_preserves_widest_row(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "normalized.csv"
            write_normalized_csv(
                iter((("first", "second"), ("a", "b", "c"), ("d",))),
                output,
                header=True,
            )
            with output.open(newline="", encoding="utf-8") as stream:
                rows = list(csv.reader(stream))

            self.assertEqual(
                [["first", "second", "column_3"], ["a", "b", "c"], ["d", "", ""]],
                rows,
            )
            self.assertEqual([], list(Path(root).glob(".*.rows")))

    def test_normalized_csv_stops_before_exceeding_its_temp_budget(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / "bounded.csv"
            with self.assertRaises(ResourceLimitExceeded):
                write_normalized_csv(
                    iter((("header",), ("x" * 128,))),
                    output,
                    header=True,
                    max_temporary_bytes=32,
                )
            self.assertFalse(output.exists())
            self.assertEqual([], list(Path(root).glob(".*.rows")))

    def test_cancellation_removes_a_job_waiting_for_aggregate_resources(self) -> None:
        token = CancellationToken()
        entered = threading.Event()
        holder: dict[str, object] = {}
        budget = ResourceBudget(
            cpuThreads=1,
            memoryBytes=64 * 1024 * 1024,
            tempBytes=64 * 1024 * 1024,
        )
        job = JobEnvelope(
            jobId=uuid.uuid4(),
            jobType="EXTRACT",
            idempotencyKey="governor-cancel",
            command={},
            resourceBudget=budget.transport_dict(),
        )

        with tempfile.TemporaryDirectory() as root, patch.object(
            duck_module, "_resource_governor", None
        ), patch.object(
            duck_module, "_operation_slots", threading.BoundedSemaphore(2)
        ), patch.dict(
            "os.environ",
            {
                "DUCKDB_TOTAL_THREADS": "1",
                "DUCKDB_TOTAL_MEMORY_BYTES": str(64 * 1024 * 1024),
                "DUCKDB_TOTAL_TEMP_BYTES": str(64 * 1024 * 1024),
                "DUCKDB_MAX_CONCURRENT_OPERATIONS": "2",
                "DUCKDB_OPERATION_QUEUE_TIMEOUT_SECONDS": "10",
            },
        ):
            held = duck_module.connect(root, "held", budget=budget)

            def handler(_envelope, _context, _services):
                entered.set()
                connection = duck_module.connect(root, "waiting")
                connection.close()

            services = legacy_runtime_services(root)
            services.handlers = {"EXTRACT": handler}
            runner = threading.Thread(
                target=lambda: holder.update(
                    result=CdwEngine(services).execute(
                        job,
                        ExecutionContext(cancellation=token),
                    )
                )
            )
            try:
                runner.start()
                self.assertTrue(entered.wait(2))
                token.cancel()
                runner.join(2)
            finally:
                held.close()
                if runner.is_alive():
                    runner.join(2)

        self.assertFalse(runner.is_alive())
        self.assertEqual(JobStatus.CANCELLED, holder["result"].status)

    def test_extract_budget_is_enforced_before_atomic_publication(self) -> None:
        request = {
            "requestId": "bounded-copy",
            "sourceType": "table",
            "tableId": "unused",
            "outputFormat": "parquet",
            "pipeline": {"pipelineVersion": 1, "steps": []},
        }
        compiled = SimpleNamespace(
            sql="SELECT i AS value FROM range(10) AS source(i)",
            parameters=[],
            output_schema=[],
        )
        connection = _RecordingConnection()
        job_id = "11111111-1111-4111-8111-111111111111"

        with tempfile.TemporaryDirectory() as root, patch.object(
            extract_module, "connect", return_value=connection
        ), patch.object(
            extract_module, "compile_pipeline_request", return_value=compiled
        ):
            with self.assertRaises(ResourceLimitExceeded):
                extract_module.execute_extract(
                    "connection-1",
                    request,
                    root,
                    job_id,
                    budget=ResourceBudget(
                        cpuThreads=1,
                        memoryBytes=64 * 1024 * 1024,
                        tempBytes=64 * 1024 * 1024,
                        rowLimit=2,
                    ),
                )
            self.assertFalse((Path(root) / "jobs" / job_id / "result.parquet").exists())

    def test_metadata_refresh_publishes_immutable_snapshots(self) -> None:
        request = {
            "sourceConnection": {"vendor": "clickhouse"},
            "tables": [
                {
                    "tableId": "patients",
                    "schemaName": "public",
                    "tableName": "patients",
                }
            ],
        }
        generation = 0

        def write_table(_source, _table, output):
            nonlocal generation
            generation += 1
            Path(output).parent.mkdir(parents=True, exist_ok=True)
            Path(output).write_bytes(f"PAR1-generation-{generation}".encode())
            return generation

        with tempfile.TemporaryDirectory() as root, patch(
            "cdw_extract.refresh.write_clickhouse_table_parquet",
            side_effect=write_table,
        ):
            first = refresh_tables_impl("connection-1", request, root, str(uuid.uuid4()))
            first_path = Path(root) / "connections" / "connection-1" / first["tables"][0]["path"]
            first_bytes = first_path.read_bytes()
            second = refresh_tables_impl("connection-1", request, root, str(uuid.uuid4()))
            second_path = Path(root) / "connections" / "connection-1" / second["tables"][0]["path"]
            manifest = load_connection_manifest("connection-1", root)

            self.assertNotEqual(first_path, second_path)
            self.assertEqual(first_bytes, first_path.read_bytes())
            self.assertTrue(second_path.is_file())
            self.assertEqual(second["tables"][0]["path"], manifest["tables"][0]["path"])

    def test_clickhouse_refresh_passes_remaining_byte_budget_per_table(self) -> None:
        request = {
            "sourceConnection": {"vendor": "clickhouse"},
            "tables": [
                {"tableId": "first", "tableName": "first"},
                {"tableId": "second", "tableName": "second"},
            ],
        }
        observed_limits: list[int | None] = []

        def write_table(_source, _table, output, *, maximum_bytes=None):
            observed_limits.append(maximum_bytes)
            Path(output).write_bytes(b"x" * min(4, maximum_bytes))
            return 1

        with tempfile.TemporaryDirectory() as root, patch(
            "cdw_extract.refresh.write_clickhouse_table_parquet",
            side_effect=write_table,
        ):
            refresh_tables_impl(
                "connection-1",
                request,
                root,
                str(uuid.uuid4()),
                ResourceBudget(
                    cpuThreads=1,
                    memoryBytes=64 * 1024 * 1024,
                    tempBytes=64 * 1024 * 1024,
                    outputBytes=6,
                ),
            )

        self.assertEqual([6, 2], observed_limits)

    def test_clickhouse_refresh_does_not_use_temp_budget_as_output_cap(self) -> None:
        observed_limits: list[int | None] = []

        def write_table(_source, _table, output, *, maximum_bytes=None):
            observed_limits.append(maximum_bytes)
            Path(output).write_bytes(b"parquet-snapshot")
            return 1

        with tempfile.TemporaryDirectory() as root, patch(
            "cdw_extract.refresh.write_clickhouse_table_parquet",
            side_effect=write_table,
        ):
            refresh_tables_impl(
                "connection-1",
                {
                    "sourceConnection": {"vendor": "clickhouse"},
                    "tables": [{"tableId": "patients", "tableName": "patients"}],
                },
                root,
                str(uuid.uuid4()),
                ResourceBudget(
                    cpuThreads=1,
                    memoryBytes=64 * 1024 * 1024,
                    tempBytes=64 * 1024 * 1024,
                ),
            )

        self.assertEqual([None], observed_limits)

    def test_duplicate_refresh_execution_uses_isolated_staging_directories(self) -> None:
        request = {
            "sourceConnection": {"vendor": "clickhouse"},
            "tables": [
                {
                    "tableId": "patients",
                    "schemaName": "public",
                    "tableName": "patients",
                }
            ],
        }
        barrier = threading.Barrier(2)
        lock = threading.Lock()
        staged_paths: list[Path] = []

        def write_table(_source, _table, output):
            path = Path(output)
            with lock:
                staged_paths.append(path)
            barrier.wait(timeout=5)
            path.write_bytes(threading.current_thread().name.encode())
            return 1

        results: list[dict] = []
        failures: list[BaseException] = []

        with tempfile.TemporaryDirectory() as root, patch(
            "cdw_extract.refresh.write_clickhouse_table_parquet",
            side_effect=write_table,
        ):
            def run_refresh() -> None:
                try:
                    results.append(
                        refresh_tables_impl("connection-1", request, root, "duplicate-job")
                    )
                except BaseException as exc:  # captured for an assertion in the parent thread
                    failures.append(exc)

            workers = [threading.Thread(target=run_refresh) for _ in range(2)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(10)

            self.assertFalse(any(worker.is_alive() for worker in workers))
            self.assertEqual([], failures)
            self.assertEqual(2, len(results))
            self.assertEqual(2, len(set(staged_paths)))
            published_paths = {
                Path(root) / "connections" / "connection-1" / result["tables"][0]["path"]
                for result in results
            }
            self.assertEqual(2, len(published_paths))
            self.assertTrue(all(path.is_file() for path in published_paths))

    def test_metadata_refresh_quotes_source_column_identifiers(self) -> None:
        class RefreshConnection:
            def __init__(self) -> None:
                self.statements: list[tuple[str, list | None]] = []

            def execute(self, statement, parameters=None):
                self.statements.append((statement, parameters))
                if statement.startswith("COPY "):
                    Path(parameters[0]).write_bytes(b"PAR1")
                return self

            def fetchone(self):
                return (1,)

            def close(self) -> None:
                pass

        request = {
            "sourceConnection": {"vendor": "postgresql"},
            "tables": [
                {
                    "tableId": "patients",
                    "schemaName": "public",
                    "tableName": "patients",
                    "columns": [{"name": 'patient"name'}],
                }
            ],
        }
        connection = RefreshConnection()
        with tempfile.TemporaryDirectory() as root, patch(
            "cdw_extract.refresh.connect", return_value=connection
        ), patch(
            "cdw_extract.refresh.source_attach_sql",
            return_value=("postgres", "ATTACH source"),
        ), patch(
            "cdw_extract.refresh.source_table_sql",
            return_value='"source"."patients"',
        ):
            refresh_tables_impl("connection-1", request, root, str(uuid.uuid4()))

        copy_statement = next(
            statement for statement, _parameters in connection.statements
            if statement.startswith("COPY ")
        )
        self.assertIn('SELECT "patient""name" FROM "source"."patients"', copy_statement)

    def test_metadata_secret_is_redacted_from_engine_failures(self) -> None:
        class SecretProvider:
            def __init__(self) -> None:
                self.purposes: list[str] = []

            @contextmanager
            def resolve(self, _reference: str, *, purpose: str):
                self.purposes.append(purpose)
                yield {"username": "alice", "password": "super-secret-value"}

        with tempfile.TemporaryDirectory() as root:
            services = legacy_runtime_services(root)
            provider = SecretProvider()
            services.secret_provider = provider
            job = JobEnvelope(
                jobId=uuid.uuid4(),
                jobType="METADATA_REFRESH",
                idempotencyKey="secret-redaction",
                command={
                    "connectionId": "connection-1",
                    "secretRef": "cdw-connection:connection-1",
                    "request": {
                        "sourceConnection": {
                            "vendor": "postgresql",
                            "host": "db.internal",
                            "database": "source",
                        }
                    },
                },
                resourceBudget={
                    "cpuThreads": 1,
                    "memoryBytes": 64 * 1024 * 1024,
                    "tempBytes": 64 * 1024 * 1024,
                },
            )
            with patch(
                "cdw_extract.refresh.refresh_tables_impl",
                side_effect=RuntimeError(
                    "attach failed user=alice password=super-secret-value"
                ),
            ):
                result = CdwEngine(services).execute(job)

        self.assertEqual(JobStatus.FAILED, result.status)
        self.assertEqual(["METADATA_REFRESH"], provider.purposes)
        self.assertNotIn("alice", result.error.message)
        self.assertNotIn("super-secret-value", result.error.message)
        self.assertIn("[REDACTED]", result.error.message)


if __name__ == "__main__":
    unittest.main()
