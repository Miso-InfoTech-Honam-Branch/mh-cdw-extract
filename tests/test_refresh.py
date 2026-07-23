from __future__ import annotations

import importlib
import json
import tempfile
import unittest
from unittest.mock import call, patch

import requests
from fastapi import BackgroundTasks, HTTPException

import app as api_app
from cdw_extract.jobs import load_job


refresh_module = importlib.import_module("cdw_extract.refresh")


def refresh_request(job_id: str | None = None, callback: bool = False) -> dict:
    request = {
        "requestId": "metadata-refresh-1",
        "sourceConnection": {"vendor": "clickhouse"},
        "tables": [
            {
                "tableId": "table-1",
                "schemaName": "testdb",
                "tableName": "patients",
                "columns": [{"columnId": "column-1", "name": "patient_id"}],
            }
        ],
    }
    if job_id is not None:
        request["jobId"] = job_id
    if callback:
        request["callback"] = {"url": "http://boot.test/table-refresh/status"}
    return request


def refresh_result(job_id: str) -> dict:
    return {
        "jobId": job_id,
        "jobType": "TABLE_REFRESH",
        "connectionId": "connection-1",
        "state": "COMPLETED",
        "message": "Table Parquet refresh completed successfully.",
        "tableCount": 1,
        "rowCount": 12,
        "tables": [
            {
                "tableId": "table-1",
                "schemaName": "testdb",
                "tableName": "patients",
                "path": "connections/connection-1/tables/testdb.patients.parquet",
                "rowCount": 12,
                "columns": [{"columnId": "column-1", "name": "patient_id"}],
                "sizeBytes": 1024,
                "sha256Checksum": "a" * 64,
                "schemaHash": "b" * 64,
            }
        ],
    }


class FakeResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class TableRefreshTest(unittest.TestCase):
    def test_async_route_declares_202_and_honors_supplied_job_id(self):
        route = next(
            route
            for route in api_app.app.routes
            if getattr(route, "path", None) == "/api/v1/connections/{connection_id}/tables/refresh"
        )
        self.assertEqual(202, route.status_code)

        supplied_job_id = "11111111-1111-1111-1111-111111111111"
        request = refresh_request(supplied_job_id)
        background_tasks = BackgroundTasks()
        with tempfile.TemporaryDirectory() as data_root, patch.object(
            api_app, "root_path", return_value=data_root
        ):
            accepted = api_app.refresh_connection_alias("connection-1", background_tasks, request)
            saved = load_job(data_root, supplied_job_id)

        self.assertEqual(supplied_job_id, accepted["jobId"])
        self.assertEqual(supplied_job_id, saved["jobId"])
        self.assertEqual("ACCEPTED", saved["state"])
        self.assertEqual(1, len(background_tasks.tasks))
        self.assertEqual(supplied_job_id, background_tasks.tasks[0].args[3])

    def test_supplied_job_id_is_validated_by_job_storage(self):
        with tempfile.TemporaryDirectory() as data_root:
            with self.assertRaisesRegex(ValueError, "jobId must be a UUID"):
                refresh_module.prepare_refresh_tables_job(
                    "connection-1",
                    refresh_request("not-a-uuid"),
                    data_root,
                )

    def test_callback_retries_network_and_non_2xx_failures_with_backoff(self):
        request = refresh_request(callback=True)
        payload = {"jobId": "job-1", "state": "COMPLETED"}
        outcomes = [
            requests.ConnectionError("temporarily unavailable"),
            FakeResponse(503, "busy"),
            FakeResponse(204),
        ]

        with patch.object(refresh_module.requests, "post", side_effect=outcomes) as post_mock, patch.object(
            refresh_module.time, "sleep"
        ) as sleep_mock:
            delivery = refresh_module.post_refresh_callback(request, payload)

        self.assertEqual(3, post_mock.call_count)
        self.assertEqual(3, delivery["attempts"])
        self.assertEqual(204, delivery["statusCode"])
        self.assertEqual(
            [
                call(refresh_module.CALLBACK_INITIAL_BACKOFF_SECONDS),
                call(refresh_module.CALLBACK_INITIAL_BACKOFF_SECONDS * 2),
            ],
            sleep_mock.call_args_list,
        )

    def test_completed_job_keeps_tables_and_records_structured_callback_error(self):
        callback_user = "callback-user-secret"
        callback_password = "callback-password-secret"
        request = refresh_request(callback=True)
        with tempfile.TemporaryDirectory() as data_root:
            accepted = refresh_module.prepare_refresh_tables_job("connection-1", request, data_root)
            result = refresh_result(accepted["jobId"])
            callback_exception = requests.ConnectionError(
                "callback failed with url: "
                f"http://boot.test/status?user={callback_user}&password={callback_password}"
            )

            with patch.object(refresh_module, "refresh_tables_impl", return_value=result), patch.object(
                refresh_module.requests,
                "post",
                side_effect=callback_exception,
            ) as post_mock, patch.object(refresh_module.time, "sleep"), self.assertLogs(
                "cdw_extract.refresh", level="ERROR"
            ) as captured_logs:
                refresh_module.run_refresh_tables_job(
                    "connection-1", request, data_root, accepted["jobId"]
                )

            job = load_job(data_root, accepted["jobId"])

        serialized_job = json.dumps(job, ensure_ascii=False)
        serialized_logs = "\n".join(captured_logs.output)
        self.assertEqual("COMPLETED", job["state"])
        self.assertEqual("connection-1", job["connectionId"])
        self.assertEqual(result["message"], job["message"])
        self.assertEqual(result["tables"], job["tables"])
        self.assertEqual(12, job["rowCount"])
        self.assertEqual("ConnectionError", job["callbackError"]["errorCode"])
        self.assertEqual(3, job["callbackError"]["attempts"])
        self.assertIn("occurredAt", job["callbackError"])
        self.assertEqual(3, post_mock.call_count)
        for secret in (callback_user, callback_password):
            self.assertNotIn(secret, serialized_job)
            self.assertNotIn(secret, serialized_logs)

    def test_failed_job_callback_and_status_redact_source_and_callback_credentials(self):
        source_user = "source-user-secret"
        source_password = "source-password-secret"
        source_token = "source-token-secret"
        callback_user = "callback-user-secret"
        callback_password = "callback-password-secret"
        source_exception = RuntimeError(
            "ClickHouse failed at "
            f"http://embedded-user:embedded-password@172.27.11.40:8123/"
            f"?database=testdb&user={source_user}&password={source_password}&token={source_token}"
        )
        callback_exception = requests.ConnectionError(
            "callback failed with url: "
            f"http://boot.test/status?user={callback_user}&password={callback_password}"
        )
        request = refresh_request(callback=True)

        with tempfile.TemporaryDirectory() as data_root:
            accepted = refresh_module.prepare_refresh_tables_job("connection-1", request, data_root)
            with patch.object(
                refresh_module, "refresh_tables_impl", side_effect=source_exception
            ), patch.object(
                refresh_module.requests, "post", side_effect=callback_exception
            ) as post_mock, patch.object(refresh_module.time, "sleep"), self.assertLogs(
                "cdw_extract.refresh", level="ERROR"
            ) as captured_logs:
                refresh_module.run_refresh_tables_job(
                    "connection-1", request, data_root, accepted["jobId"]
                )

            job = load_job(data_root, accepted["jobId"])

        serialized_job = json.dumps(job, ensure_ascii=False)
        serialized_logs = "\n".join(captured_logs.output)
        callback_payloads = [entry.kwargs["json"] for entry in post_mock.call_args_list]
        serialized_callbacks = json.dumps(callback_payloads, ensure_ascii=False)
        self.assertEqual("FAILED", job["state"])
        self.assertEqual("RuntimeError", job["errorCode"])
        self.assertEqual("ConnectionError", job["callbackError"]["errorCode"])
        self.assertEqual(3, job["callbackError"]["attempts"])
        self.assertEqual(3, post_mock.call_count)
        self.assertIn(refresh_module.REDACTED, serialized_job)
        for secret in (
            "embedded-user",
            "embedded-password",
            source_user,
            source_password,
            source_token,
            callback_user,
            callback_password,
        ):
            self.assertNotIn(secret, serialized_job)
            self.assertNotIn(secret, serialized_logs)
            self.assertNotIn(secret, serialized_callbacks)

    def test_refresh_sync_http_error_detail_is_sanitized(self):
        source_user = "sync-user-secret"
        source_password = "sync-password-secret"
        source_exception = requests.ConnectionError(
            "failed with url: "
            f"http://172.27.11.40:8123/?database=testdb&user={source_user}&password={source_password}"
        )
        request = refresh_request()

        with tempfile.TemporaryDirectory() as data_root, patch.object(
            api_app, "root_path", return_value=data_root
        ), patch.object(refresh_module, "refresh_tables_impl", side_effect=source_exception):
            with self.assertRaises(HTTPException) as raised:
                api_app.refresh_connection_sync("connection-1", request)

        detail = str(raised.exception.detail)
        self.assertNotIn(source_user, detail)
        self.assertNotIn(source_password, detail)
        self.assertIn(refresh_module.REDACTED, detail)


if __name__ == "__main__":
    unittest.main()
