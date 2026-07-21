from __future__ import annotations

import importlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import BackgroundTasks

import app as api_app
from cdw_extract.jobs import cancel_job, load_job
from cdw_extract.user_dataset import (
    dataset_file_manifest_path,
    dataset_file_parquet_path,
    load_dataset_file_manifest,
)


extract_module = importlib.import_module("cdw_extract.extract")


def extract_request(output_format: str = "parquet") -> dict:
    return {
        "requestId": "request-1",
        "sourceType": "table",
        "tableId": "table-1",
        "outputFormat": output_format,
    }


def extract_result_request(output_format: str = "parquet") -> dict:
    return {
        **extract_request(output_format),
        "datasetId": "extract-dataset-1",
        "runId": "extract-run-1",
        "resultTarget": {
            "kind": "USER_DATST",
            "userId": "user-1",
            "userDatasetId": "result-dataset-1",
            "userDatasetFileId": "result-file-1",
            "idempotencyKey": "EXPORT:extract-run-1",
        },
        "callback": {"url": "http://boot.test/callback"},
    }


class FakeConnection:
    def __init__(self, row_count: int = 3, fail_copy: bool = False):
        self.row_count = row_count
        self.fail_copy = fail_copy
        self.closed = False

    def execute(self, sql: str, parameters: list[str]):
        if sql.startswith("COPY"):
            Path(parameters[0]).write_text("partial" if self.fail_copy else "result", encoding="utf-8")
            if self.fail_copy:
                raise RuntimeError("copy failed")
        return self

    def fetchone(self):
        return (self.row_count,)

    def close(self):
        self.closed = True


class AsyncExtractTest(unittest.TestCase):
    def test_prepare_persists_accepted_job(self):
        with tempfile.TemporaryDirectory() as data_root:
            accepted = extract_module.prepare_extract_job("connection-1", extract_request(), data_root)
            job = load_job(data_root, accepted["jobId"])

            self.assertEqual("ACCEPTED", accepted["state"])
            self.assertEqual("EXPORT", accepted["jobType"])
            self.assertEqual("connection-1", accepted["connectionId"])
            self.assertEqual("request-1", accepted["requestId"])
            self.assertEqual("ACCEPTED", job["state"])
            self.assertEqual("parquet", job["outputFormat"])

    def test_prepare_rejects_invalid_output_format_without_creating_job(self):
        with tempfile.TemporaryDirectory() as data_root:
            with self.assertRaisesRegex(ValueError, "outputFormat"):
                extract_module.prepare_extract_job("connection-1", extract_request("xlsx"), data_root)

            jobs_root = Path(data_root) / "jobs"
            self.assertFalse(jobs_root.exists())

    def test_prepare_preserves_extract_result_correlation_and_rejects_csv_target(self):
        with tempfile.TemporaryDirectory() as data_root:
            request = extract_result_request()
            accepted = extract_module.prepare_extract_job("connection-1", request, data_root)
            job = load_job(data_root, accepted["jobId"])

            self.assertEqual("extract-dataset-1", accepted["datasetId"])
            self.assertEqual("extract-run-1", accepted["runId"])
            self.assertEqual(request["resultTarget"], accepted["resultTarget"])
            self.assertEqual("user-1", job["resultUserId"])
            self.assertEqual("result-dataset-1", job["resultUserDatasetId"])
            self.assertEqual("result-file-1", job["resultUserDatasetFileId"])

        with tempfile.TemporaryDirectory() as data_root:
            with self.assertRaisesRegex(ValueError, "outputFormat=parquet"):
                extract_module.prepare_extract_job("connection-1", extract_result_request("csv"), data_root)

    def test_runner_transitions_to_completed_and_preserves_created_at(self):
        with tempfile.TemporaryDirectory() as data_root:
            accepted = extract_module.prepare_extract_job("connection-1", extract_request(), data_root)
            created_at = load_job(data_root, accepted["jobId"])["createdAt"]
            connection = FakeConnection(row_count=7)

            with patch.object(extract_module, "final_query", return_value="SELECT 1"), patch.object(
                extract_module, "connect", return_value=connection
            ):
                extract_module.run_extract_job("connection-1", extract_request(), data_root, accepted["jobId"])

            job = load_job(data_root, accepted["jobId"])
            self.assertEqual("COMPLETED", job["state"])
            self.assertEqual(7, job["rowCount"])
            self.assertEqual(created_at, job["createdAt"])
            self.assertTrue(Path(job["filePath"]).exists())
            self.assertTrue(connection.closed)

    def test_runner_completes_with_real_duckdb_parquet_source(self):
        with tempfile.TemporaryDirectory() as data_root:
            connection_root = Path(data_root) / "connections" / "connection-1"
            source_file = connection_root / "tables" / "patient.parquet"
            source_file.parent.mkdir(parents=True)
            connection = extract_module.connect()
            try:
                connection.execute(
                    "COPY (SELECT 1 AS patient_id, 'Kim' AS patient_name "
                    "UNION ALL SELECT 2, 'Lee') TO ? (FORMAT PARQUET)",
                    [source_file.as_posix()],
                )
            finally:
                connection.close()
            (connection_root / "manifest.json").write_text(
                json.dumps(
                    {
                        "connectionId": "connection-1",
                        "status": "COMPLETED",
                        "tables": [
                            {
                                "tableId": "table-1",
                                "schemaName": "public",
                                "tableName": "patient",
                                "path": "tables/patient.parquet",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            request = {
                **extract_request(),
                "tableName": "patient",
                "columns": [
                    {"name": "patient_id", "alias": "patient_id"},
                    {"name": "patient_name", "alias": "patient_name"},
                ],
            }
            accepted = extract_module.prepare_extract_job("connection-1", request, data_root)

            extract_module.run_extract_job("connection-1", request, data_root, accepted["jobId"])

            job = load_job(data_root, accepted["jobId"])
            self.assertEqual("COMPLETED", job["state"])
            self.assertEqual(2, job["rowCount"])
            self.assertTrue(Path(job["filePath"]).exists())

    def test_runner_applies_saved_pipeline_to_real_parquet_output(self):
        with tempfile.TemporaryDirectory() as data_root:
            connection_root = Path(data_root) / "connections" / "connection-1"
            source_file = connection_root / "tables" / "sales.parquet"
            source_file.parent.mkdir(parents=True)
            connection = extract_module.connect()
            try:
                connection.execute(
                    "COPY (SELECT ' Seoul ' AS city, 100::BIGINT AS amount "
                    "UNION ALL SELECT 'Busan', 50) TO ? (FORMAT PARQUET)",
                    [source_file.as_posix()],
                )
            finally:
                connection.close()
            (connection_root / "manifest.json").write_text(
                json.dumps({"connectionId": "connection-1", "status": "COMPLETED", "tables": [{
                    "tableId": "table-1", "schemaName": "public", "tableName": "sales",
                    "path": "tables/sales.parquet"
                }]}), encoding="utf-8"
            )
            request = {
                **extract_request(), "tableName": "sales",
                "columns": [{"name": "city", "alias": "city"}, {"name": "amount", "alias": "amount"}],
                "sourceColumns": [
                    {"columnId": "src:city", "physicalName": "city", "label": "지역", "dataType": "STRING"},
                    {"columnId": "src:amount", "physicalName": "amount", "label": "금액", "dataType": "INT64"},
                ],
                "pipeline": {"pipelineVersion": 1, "steps": [
                    {"stepId": "trim-city", "type": "TRIM", "config": {
                        "columnId": "src:city", "mode": "BOTH", "outputId": "city", "label": "지역"
                    }},
                    {"stepId": "filter-amount", "type": "FILTER", "config": {"conditions": [{
                        "columnId": "src:amount", "operator": "GTE", "values": [100]
                    }]}},
                    {"stepId": "output", "type": "OUTPUT", "config": {}}
                ]}
            }
            accepted = extract_module.prepare_extract_job("connection-1", request, data_root)
            extract_module.run_extract_job("connection-1", request, data_root, accepted["jobId"])
            job = load_job(data_root, accepted["jobId"])
            self.assertEqual("COMPLETED", job["state"])
            self.assertEqual(1, job["rowCount"])
            connection = extract_module.connect()
            try:
                result = connection.execute(
                    "SELECT * FROM read_parquet(?)", [job["filePath"]]
                ).fetchone()
            finally:
                connection.close()
            self.assertEqual((" Seoul ", 100, "Seoul"), result)
            self.assertEqual([], list(Path(job["filePath"]).parent.glob("*.tmp")))

    def test_runner_publishes_reusable_user_dataset_and_sends_exact_callback(self):
        with tempfile.TemporaryDirectory() as data_root:
            request = extract_result_request()
            accepted = extract_module.prepare_extract_job("connection-1", request, data_root)
            callback_payloads = []

            with patch.object(
                extract_module,
                "final_query",
                return_value="SELECT 1 AS patient_id, 'Kim' AS patient_name UNION ALL SELECT 2, 'Lee'",
            ), patch.object(
                extract_module,
                "post_callback",
                side_effect=lambda _request, payload: callback_payloads.append(payload) or {"url": "callback", "statusCode": 200},
            ):
                extract_module.run_extract_job("connection-1", request, data_root, accepted["jobId"])

            job = load_job(data_root, accepted["jobId"])
            parquet_path = dataset_file_parquet_path(
                data_root,
                "user-1",
                "result-dataset-1",
                "result-file-1",
            )
            manifest_path = dataset_file_manifest_path(
                data_root,
                "user-1",
                "result-dataset-1",
                "result-file-1",
            )
            manifest = load_dataset_file_manifest(
                data_root,
                "user-1",
                "result-dataset-1",
                "result-file-1",
            )

            self.assertEqual("COMPLETED", job["state"])
            self.assertEqual(2, job["rowCount"])
            self.assertEqual(parquet_path.as_posix(), job["filePath"])
            self.assertTrue(parquet_path.exists())
            self.assertTrue(manifest_path.exists())
            self.assertTrue(manifest_path.with_name("schema.json").exists())
            self.assertEqual("EXTRACT_RESULT", manifest["artifactKind"])
            self.assertEqual("EXPORT:extract-run-1", manifest["idempotencyKey"])
            self.assertEqual(["patient_id", "patient_name"], [column["name"] for column in manifest["columns"]])
            self.assertEqual(1, len(callback_payloads))
            callback = callback_payloads[0]
            self.assertEqual("COMPLETED", callback["status"])
            self.assertEqual("extract-dataset-1", callback["datasetId"])
            self.assertEqual("extract-run-1", callback["runId"])
            self.assertEqual("result-dataset-1", callback["resultUserDatasetId"])
            self.assertEqual(["patient_id", "patient_name"], [column["name"] for column in callback["resultColumns"]])

            reused = extract_module.extract(
                "user-dataset",
                {
                    "requestId": "reuse-request-1",
                    "sourceType": "table",
                    "sourceKind": "USER_DATST",
                    "userId": "user-1",
                    "userDatasetId": "result-dataset-1",
                    "userDatasetFileId": "result-file-1",
                    "columns": [
                        {"name": "patient_id", "alias": "patient_id"},
                        {"name": "patient_name", "alias": "patient_name"},
                    ],
                    "outputFormat": "parquet",
                },
                data_root,
            )
            self.assertEqual("COMPLETED", reused["state"])
            self.assertEqual(2, reused["rowCount"])

    def test_callback_failure_does_not_change_completed_extract_result(self):
        with tempfile.TemporaryDirectory() as data_root:
            request = extract_result_request()
            accepted = extract_module.prepare_extract_job("connection-1", request, data_root)

            with patch.object(extract_module, "final_query", return_value="SELECT 1 AS patient_id"), patch.object(
                extract_module,
                "post_callback",
                side_effect=RuntimeError("callback unavailable"),
            ):
                extract_module.run_extract_job("connection-1", request, data_root, accepted["jobId"])

            job = load_job(data_root, accepted["jobId"])
            self.assertEqual("COMPLETED", job["state"])
            self.assertEqual("RuntimeError", job["callbackError"]["errorCode"])
            self.assertIn("callback unavailable", job["callbackError"]["message"])
            self.assertTrue(
                dataset_file_parquet_path(
                    data_root,
                    "user-1",
                    "result-dataset-1",
                    "result-file-1",
                ).exists()
            )

    def test_runner_records_failure_without_raising_and_removes_partial_file(self):
        with tempfile.TemporaryDirectory() as data_root:
            accepted = extract_module.prepare_extract_job("connection-1", extract_request(), data_root)
            connection = FakeConnection(fail_copy=True)

            with patch.object(extract_module, "final_query", return_value="SELECT 1"), patch.object(
                extract_module, "connect", return_value=connection
            ):
                extract_module.run_extract_job("connection-1", extract_request(), data_root, accepted["jobId"])

            job = load_job(data_root, accepted["jobId"])
            self.assertEqual("FAILED", job["state"])
            self.assertEqual("RuntimeError", job["errorCode"])
            self.assertEqual("copy failed", job["message"])
            self.assertEqual([], list((Path(data_root) / "jobs" / accepted["jobId"]).glob("*.tmp")))
            self.assertTrue(connection.closed)

    def test_runner_honors_cancellation_before_execution(self):
        with tempfile.TemporaryDirectory() as data_root:
            accepted = extract_module.prepare_extract_job("connection-1", extract_request(), data_root)
            cancel_job(data_root, accepted["jobId"])

            with patch.object(extract_module, "connect") as connect_mock:
                extract_module.run_extract_job("connection-1", extract_request(), data_root, accepted["jobId"])

            job = load_job(data_root, accepted["jobId"])
            self.assertEqual("CANCELLED", job["state"])
            self.assertTrue(job["cancelSupported"])
            connect_mock.assert_not_called()

    def test_sync_extract_function_remains_supported(self):
        with tempfile.TemporaryDirectory() as data_root:
            connection = FakeConnection(row_count=2)
            with patch.object(extract_module, "final_query", return_value="SELECT 1"), patch.object(
                extract_module, "connect", return_value=connection
            ):
                result = extract_module.extract("connection-1", extract_request("csv"), data_root)

            self.assertEqual("COMPLETED", result["state"])
            self.assertEqual("csv", result["outputFormat"])
            self.assertEqual(2, result["rowCount"])

    def test_http_route_declares_202_and_registers_background_runner(self):
        route = next(
            route
            for route in api_app.app.routes
            if getattr(route, "path", None) == "/api/v1/connections/{connection_id}/extracts"
        )
        self.assertEqual(202, route.status_code)

        accepted = {
            "jobId": "14f1ed4e-9358-4be1-bad3-590946aff562",
            "jobType": "EXPORT",
            "connectionId": "connection-1",
            "requestId": "request-1",
            "state": "ACCEPTED",
        }
        background_tasks = BackgroundTasks()
        with patch.object(api_app, "root_path", return_value="C:/data"), patch.object(
            api_app, "prepare_extract_job", return_value=accepted
        ):
            response = api_app.extract_connection_route("connection-1", background_tasks, extract_request())

        self.assertEqual(accepted, response)
        self.assertEqual(1, len(background_tasks.tasks))
        task = background_tasks.tasks[0]
        self.assertIs(api_app.run_extract_job, task.func)
        self.assertEqual("connection-1", task.args[0])
        self.assertEqual(accepted["jobId"], task.args[3])


if __name__ == "__main__":
    unittest.main()
