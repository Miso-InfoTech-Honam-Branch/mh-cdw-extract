from __future__ import annotations

import copy
import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

import duckdb

from cdw_extract.adapters.local import LocalArtifactStore
from cdw_extract.contracts import (
    ArtifactDescriptor,
    JobCallbackEvent,
    JobResult,
)
from cdw_extract.errors import PipelineSnapshotMismatch, PipelineSourceSchemaChanged
from cdw_extract.paths import connection_root, table_file_name
from cdw_extract.query import SourceResolver
from cdw_extract.refresh import refresh_tables_impl
from cdw_extract.transforms.compiler import canonical_hash
from cdw_extract.transforms.runtime import compile_pipeline_request, validate_pipeline_request


def _artifact(key: str = "incoming/patients.csv") -> ArtifactDescriptor:
    return ArtifactDescriptor(
        store="LOCAL",
        key=key,
        sha256="a" * 64,
        sizeBytes=1,
        contentType="text/csv",
        format="CSV",
    )


def _pipeline_request(source_sql: str) -> tuple[dict, str]:
    pipeline = {
        "pipelineVersion": 1,
        "steps": [
            {
                "stepId": "pivot",
                "type": "PIVOT",
                "config": {
                    "groupColumnIds": [],
                    "pivotColumnId": "src:category",
                    "values": [],
                    "aggregates": [
                        {
                            "aggregateId": "amount",
                            "op": "SUM",
                            "columnId": "src:amount",
                            "label": "Amount",
                        }
                    ],
                },
            },
            {"stepId": "output", "type": "OUTPUT", "config": {}},
        ],
    }
    request = {
        "sourceType": "table",
        "pipeline": pipeline,
        "sourceColumns": [
            {
                "columnId": "src:category",
                "physicalName": "category",
                "label": "Category",
                "nullable": False,
            },
            {
                "columnId": "src:amount",
                "physicalName": "amount",
                "label": "Amount",
                "nullable": False,
            },
        ],
    }
    return request, source_sql


class PathSafetyTest(unittest.TestCase):
    def test_connection_identity_and_manifest_path_cannot_escape_data_root(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / "data"
            root.mkdir()
            with self.assertRaisesRegex(ValueError, "connectionId"):
                connection_root(root, "../../outside")

            connection = connection_root(root, "connection-1")
            connection.mkdir(parents=True)
            (connection / "manifest.json").write_text(
                json.dumps(
                    {
                        "connectionId": "connection-1",
                        "tables": [
                            {
                                "tableId": "table-1",
                                "path": "tables/../../outside.parquet",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "must not traverse"):
                SourceResolver("connection-1", root).table_path({"tableId": "table-1"})

    def test_refresh_uses_safe_hashed_file_name_but_preserves_database_metadata(self):
        table = {
            "tableId": "table-1",
            "schemaName": "unsafe/schema",
            "tableName": "..\\..\\patients",
            "columns": [],
        }
        file_name = table_file_name(table)
        self.assertRegex(file_name, r"^[A-Za-z0-9_-]+-[0-9a-f]{16}\.parquet$")
        self.assertNotIn("/", file_name)
        self.assertNotIn("\\", file_name)

        def write_table(_source, _table, output):
            path = Path(output)
            path.parent.mkdir(parents=True, exist_ok=True)
            connection = duckdb.connect()
            try:
                connection.execute(
                    "COPY (SELECT range AS patient_id FROM range(3)) "
                    "TO ? (FORMAT PARQUET)",
                    [path.as_posix()],
                )
            finally:
                connection.close()
            return 3

        with tempfile.TemporaryDirectory() as temporary, patch(
            "cdw_extract.refresh.write_clickhouse_table_parquet",
            side_effect=write_table,
        ):
            result = refresh_tables_impl(
                "connection-1",
                {"sourceConnection": {"vendor": "clickhouse"}, "tables": [table]},
                temporary,
                str(uuid.uuid4()),
            )

            artifact = result["tables"][0]
            self.assertEqual(table["schemaName"], artifact["schemaName"])
            self.assertEqual(table["tableName"], artifact["tableName"])
            self.assertTrue(artifact["path"].startswith("connections/connection-1/"))
            self.assertFalse(Path(artifact["path"]).is_absolute())
            published = (Path(temporary) / artifact["path"]).resolve()
            self.assertTrue(published.is_file())
            self.assertTrue(
                published.is_relative_to(
                    (connection_root(temporary, "connection-1") / "tables").resolve()
                )
            )

    def test_connection_table_symlink_cannot_escape_store(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            root = base / "data"
            connection = connection_root(root, "connection-1")
            connection.mkdir(parents=True)
            outside = base / "outside"
            outside.mkdir()
            (outside / "secret.parquet").write_bytes(b"secret")
            try:
                os.symlink(outside, connection / "tables", target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"directory symlinks are unavailable: {exc}")
            (connection / "manifest.json").write_text(
                json.dumps(
                    {
                        "connectionId": "connection-1",
                        "tables": [{"tableId": "table-1", "path": "tables/secret.parquet"}],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "tables directory"):
                SourceResolver("connection-1", root).table_path({"tableId": "table-1"})


class LocalArtifactSafetyTest(unittest.TestCase):
    def test_materialize_produces_verified_immutable_workspace_copy(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            store_root = base / "store"
            source = store_root / "incoming" / "patients.csv"
            source.parent.mkdir(parents=True)
            source.write_text("patient_id\n1\n", encoding="utf-8")
            store = LocalArtifactStore(store_root)
            descriptor = store.describe("incoming/patients.csv")
            workspace = base / "workspace"

            with store.materialize(descriptor, workspace) as materialized:
                self.assertNotEqual(source.resolve(), materialized)
                self.assertTrue(materialized.is_relative_to(workspace.resolve()))
                source.write_text("patient_id\n999\n", encoding="utf-8")
                self.assertEqual("patient_id\n1\n", materialized.read_text(encoding="utf-8"))
            self.assertFalse(materialized.exists())

    def test_open_revalidates_descriptor_and_real_containment_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            store_root = base / "store"
            source = store_root / "incoming" / "patients.csv"
            source.parent.mkdir(parents=True)
            source.write_text("id\n1\n", encoding="utf-8")
            store = LocalArtifactStore(store_root)
            descriptor = store.describe("incoming/patients.csv")
            source.write_text("id\n2\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "checksum or size"):
                store.open(descriptor)

            outside = base / "outside"
            outside.mkdir()
            (outside / "secret.csv").write_text("secret", encoding="utf-8")
            try:
                os.symlink(outside, store_root / "linked", target_is_directory=True)
            except OSError as exc:
                self.skipTest(f"directory symlinks are unavailable: {exc}")
            with self.assertRaisesRegex(ValueError, "outside the local store"):
                store.describe("linked/secret.csv")


class ImmutablePipelineContractTest(unittest.TestCase):
    def test_validation_returns_full_resolved_pipeline_without_mutating_request(self):
        request, source = _pipeline_request(
            "(VALUES ('A', 10), ('B', 20)) AS source(category, amount)"
        )
        original_request = copy.deepcopy(request)

        with patch(
            "cdw_extract.transforms.runtime.connect",
            side_effect=lambda *_args, **_kwargs: duckdb.connect(),
        ), patch("cdw_extract.transforms.runtime._source_sql", return_value=source):
            result = validate_pipeline_request("connection-1", request, ".")

        resolved = result["resolvedPipeline"]
        self.assertEqual(original_request, request)
        self.assertEqual(2, len(resolved["steps"]))
        self.assertEqual("OUTPUT", resolved["steps"][-1]["type"])
        self.assertEqual(
            ["A", "B"],
            [item["value"] for item in resolved["steps"][0]["config"]["values"]],
        )
        self.assertEqual(canonical_hash(resolved), result["pipelineHash"])
        self.assertNotEqual(canonical_hash(request["pipeline"]), result["pipelineHash"])

    def test_saved_resolved_pipeline_and_hash_can_be_used_for_execution(self):
        request, source = _pipeline_request(
            "(VALUES ('A', 10), ('B', 20)) AS source(category, amount)"
        )
        with patch(
            "cdw_extract.transforms.runtime.connect",
            side_effect=lambda *_args, **_kwargs: duckdb.connect(),
        ), patch("cdw_extract.transforms.runtime._source_sql", return_value=source):
            validation = validate_pipeline_request("connection-1", request, ".")

        execution_request = copy.deepcopy(request)
        execution_request["pipeline"] = copy.deepcopy(validation["resolvedPipeline"])
        execution_request["expectedPipelineHash"] = validation["pipelineHash"]
        execution_request["expectedCompilerVersion"] = validation["compilerVersion"]
        changed_source = "(VALUES ('A', 10), ('C', 30)) AS source(category, amount)"
        connection = duckdb.connect()
        try:
            with patch("cdw_extract.transforms.runtime._source_sql", return_value=changed_source):
                compiled = compile_pipeline_request(
                    "connection-1", execution_request, ".", connection
                )
            self.assertEqual(validation["pipelineHash"], compiled.pipeline_hash)
            self.assertEqual(validation["resolvedPipeline"], compiled.resolved_pipeline)
            self.assertEqual(
                [(10, None)],
                connection.execute(compiled.sql, compiled.parameters).fetchall(),
            )
        finally:
            connection.close()

    def test_fixed_pivot_validation_preserves_supplied_values_and_hash(self):
        request, source = _pipeline_request(
            "(VALUES ('A', 10), ('B', 20)) AS source(category, amount)"
        )
        request["pipeline"]["steps"][0]["config"]["values"] = [
            {"valueId": "fixed-a", "value": "A", "label": "Fixed A", "sort": 1}
        ]
        expected_pipeline = copy.deepcopy(request["pipeline"])

        with patch(
            "cdw_extract.transforms.runtime.connect",
            side_effect=lambda *_args, **_kwargs: duckdb.connect(),
        ), patch("cdw_extract.transforms.runtime._source_sql", return_value=source):
            result = validate_pipeline_request("connection-1", request, ".")

        self.assertEqual(expected_pipeline, result["resolvedPipeline"])
        self.assertEqual(canonical_hash(expected_pipeline), result["pipelineHash"])

    def test_automatic_pivot_hash_includes_resolved_output_values(self):
        first_request, first_source = _pipeline_request(
            "(VALUES ('A', 10), ('B', 20)) AS source(category, amount)"
        )
        first_connection = duckdb.connect()
        try:
            with patch("cdw_extract.transforms.runtime._source_sql", return_value=first_source):
                first = compile_pipeline_request("connection-1", first_request, ".", first_connection)
        finally:
            first_connection.close()

        changed_request, changed_source = _pipeline_request(
            "(VALUES ('A', 10), ('C', 20)) AS source(category, amount)"
        )
        changed_request["expectedPipelineHash"] = first.pipeline_hash
        changed_connection = duckdb.connect()
        try:
            with patch("cdw_extract.transforms.runtime._source_sql", return_value=changed_source):
                with self.assertRaises(PipelineSnapshotMismatch):
                    compile_pipeline_request(
                        "connection-1", changed_request, ".", changed_connection
                    )
        finally:
            changed_connection.close()

    def test_source_schema_change_has_stable_typed_error(self):
        request = {
            "sourceType": "table",
            "pipeline": {
                "pipelineVersion": 1,
                "steps": [{"stepId": "output", "type": "OUTPUT", "config": {}}],
            },
            "sourceColumns": [
                {"columnId": "src:value", "physicalName": "value", "label": "Value"}
            ],
            "expectedSourceSchemaHash": "sha256:not-the-current-schema",
        }
        connection = duckdb.connect()
        try:
            with patch(
                "cdw_extract.transforms.runtime._source_sql",
                return_value="(VALUES (1)) AS source(value)",
            ):
                with self.assertRaises(PipelineSourceSchemaChanged) as raised:
                    compile_pipeline_request("connection-1", request, ".", connection)
        finally:
            connection.close()
        self.assertEqual("PIPELINE_SOURCE_SCHEMA_CHANGED", raised.exception.code)

    def test_non_success_result_cannot_carry_artifacts_and_schema_error_is_validation(self):
        job_id = uuid.uuid4()
        with self.assertRaisesRegex(ValueError, "only when status is SUCCESS"):
            JobResult(
                jobId=job_id,
                jobType="EXTRACT",
                status="FAILED",
                artifacts=(_artifact(),),
                error={"code": "FAILED", "message": "failed"},
            )

        failed = JobResult(
            jobId=job_id,
            jobType="EXTRACT",
            status="FAILED",
            error={
                "code": "PIPELINE_SOURCE_SCHEMA_CHANGED",
                "message": "source changed",
            },
        )
        callback = JobCallbackEvent.from_result(failed, sequence=1, attempt=1)
        self.assertEqual("VALIDATION", callback.error.category)


if __name__ == "__main__":
    unittest.main()
