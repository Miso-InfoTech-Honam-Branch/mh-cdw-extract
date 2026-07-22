from __future__ import annotations

import io
import importlib
import json
import multiprocessing
import tempfile
import threading
import time
import unittest
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from fastapi import BackgroundTasks, UploadFile

import app as api_app
import cdw_extract.duck as duck_module
from cdw_extract.contracts import ResourceBudget
from cdw_extract.jobs import cancel_job, load_job, save_job


extract_module = importlib.import_module("cdw_extract.extract")


def save_job_from_process(arguments: tuple[str, str, int]) -> None:
    data_root, job_id, index = arguments
    save_job(data_root, {"jobId": job_id, f"processField{index}": index})


class BlockingExtractConnection:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.interrupted = threading.Event()
        self.closed = False

    def execute(self, sql: str, parameters: list[str]):
        if sql.startswith("COPY"):
            self.started.set()
            if not self.interrupted.wait(5):
                Path(parameters[0]).write_text("completed", encoding="utf-8")
                return self
            raise RuntimeError("duckdb operation interrupted")
        return self

    def fetchone(self):
        return (1,)

    def interrupt(self) -> None:
        self.interrupted.set()

    def close(self) -> None:
        self.closed = True


class WorkerRuntimeTest(unittest.TestCase):
    def test_running_extract_is_interrupted_and_terminally_cancelled(self):
        with tempfile.TemporaryDirectory() as data_root:
            request = {
                "requestId": "cancel-running",
                "sourceType": "table",
                "tableId": "table-1",
                "outputFormat": "parquet",
                "callback": {"url": "http://boot.test/callback"},
            }
            accepted = extract_module.prepare_extract_job("connection-1", request, data_root)
            connection = BlockingExtractConnection()
            callbacks: list[dict] = []

            with patch.object(extract_module, "final_query", return_value="SELECT 1"), patch.object(
                extract_module, "connect", return_value=connection
            ), patch.object(
                extract_module,
                "post_callback",
                side_effect=lambda _request, payload: callbacks.append(payload) or {"statusCode": 200},
            ):
                runner = threading.Thread(
                    target=extract_module.run_extract_job,
                    args=("connection-1", request, data_root, accepted["jobId"]),
                )
                runner.start()
                self.assertTrue(connection.started.wait(2), "extract did not reach DuckDB COPY")
                response = cancel_job(data_root, accepted["jobId"])
                runner.join(3)

            self.assertFalse(runner.is_alive())
            self.assertEqual("CANCELLED", response["state"])
            self.assertTrue(response["cancelSupported"])
            job = load_job(data_root, accepted["jobId"])
            self.assertEqual("CANCELLED", job["state"])
            self.assertTrue(connection.interrupted.is_set())
            self.assertTrue(connection.closed)
            self.assertEqual(["CANCELLED"], [payload["state"] for payload in callbacks])
            self.assertFalse((Path(data_root) / "jobs" / accepted["jobId"] / "result.parquet").exists())

    def test_terminal_cancel_is_idempotent_and_never_regresses(self):
        with tempfile.TemporaryDirectory() as data_root:
            job_id = str(uuid.uuid4())
            save_job(data_root, {"jobId": job_id, "jobType": "EXPORT", "state": "CANCELLED"})
            response = cancel_job(data_root, job_id)
            save_job(data_root, {"jobId": job_id, "state": "COMPLETED", "rowCount": 99})

            self.assertEqual("CANCELLED", response["state"])
            self.assertFalse(response["cancelSupported"])
            terminal = load_job(data_root, job_id)
            self.assertEqual("CANCELLED", terminal["state"])
            self.assertNotIn("rowCount", terminal)

    def test_cancel_cleanup_never_deletes_a_reused_result_target(self):
        with tempfile.TemporaryDirectory() as data_root:
            target = Path(data_root) / "user-datasets" / "u" / "d" / "files" / "f" / "parquet" / "data.parquet"
            target.parent.mkdir(parents=True)
            target.write_text("existing-good-artifact", encoding="utf-8")

            extract_module._discard_extract_result(
                {
                    "filePath": target.as_posix(),
                    "_resultTarget": True,
                    "_publishedNew": False,
                }
            )

            self.assertEqual("existing-good-artifact", target.read_text(encoding="utf-8"))

            ordinary = Path(data_root) / "jobs" / str(uuid.uuid4()) / "result.csv"
            ordinary.parent.mkdir(parents=True)
            ordinary.write_text("cancelled-output", encoding="utf-8")
            extract_module._discard_extract_result({"filePath": ordinary.as_posix()})
            self.assertFalse(ordinary.exists())

    def test_idempotent_publish_is_detected_as_reused_before_cancel_cleanup(self):
        with tempfile.TemporaryDirectory() as data_root:
            request = {
                "requestId": "request-reused",
                "datasetId": "extract-dataset",
                "runId": "extract-run",
                "sourceType": "table",
                "tableId": "table-1",
                "outputFormat": "parquet",
                "resultTarget": {
                    "kind": "USER_DATST",
                    "userId": "user-1",
                    "userDatasetId": "dataset-1",
                    "userDatasetFileId": "file-1",
                    "idempotencyKey": "EXPORT:extract-run",
                },
            }
            with patch.object(extract_module, "final_query", return_value="SELECT 1 AS id"):
                first = extract_module.execute_extract(
                    "connection-1", request, data_root, str(uuid.uuid4())
                )
                second = extract_module.execute_extract(
                    "connection-1", request, data_root, str(uuid.uuid4())
                )

            self.assertTrue(first["_publishedNew"])
            self.assertFalse(second["_publishedNew"])
            extract_module._discard_extract_result(second)
            self.assertTrue(Path(first["filePath"]).exists())

    def test_concurrent_job_saves_are_valid_merged_and_leave_no_temp_files(self):
        with tempfile.TemporaryDirectory() as data_root:
            job_ids = [str(uuid.uuid4()) for _ in range(4)]
            for job_id in job_ids:
                save_job(data_root, {"jobId": job_id, "jobType": "EXPORT", "state": "RUNNING"})

            def save(index: int) -> None:
                job_id = job_ids[index % len(job_ids)]
                save_job(data_root, {"jobId": job_id, f"field{index}": index})

            with ThreadPoolExecutor(max_workers=24) as executor:
                list(executor.map(save, range(240)))

            for job_index, job_id in enumerate(job_ids):
                job = load_job(data_root, job_id)
                json.loads((Path(data_root) / "jobs" / job_id / "job.json").read_text(encoding="utf-8"))
                expected = range(job_index, 240, len(job_ids))
                self.assertTrue(all(job[f"field{index}"] == index for index in expected))
            self.assertEqual([], list((Path(data_root) / "jobs").rglob("*.tmp")))

    def test_cross_process_job_updates_share_the_os_file_lock(self):
        with tempfile.TemporaryDirectory() as data_root:
            job_id = str(uuid.uuid4())
            save_job(data_root, {"jobId": job_id, "jobType": "EXPORT", "state": "RUNNING"})
            arguments = [(data_root, job_id, index) for index in range(40)]
            context = multiprocessing.get_context("spawn")
            with ProcessPoolExecutor(max_workers=4, mp_context=context) as executor:
                list(executor.map(save_job_from_process, arguments))

            job = load_job(data_root, job_id)
            self.assertTrue(all(job[f"processField{index}"] == index for index in range(40)))
            self.assertEqual([], list((Path(data_root) / "jobs").rglob("*.tmp")))

    def test_duckdb_operations_are_bounded_isolated_and_cleaned(self):
        with tempfile.TemporaryDirectory() as data_root, patch.object(
            duck_module, "_operation_slots", threading.BoundedSemaphore(1)
        ), patch.dict(
            "os.environ",
            {"DUCKDB_OPERATION_QUEUE_TIMEOUT_SECONDS": "2"},
        ):
            first = duck_module.connect(data_root, "analytics", "first")
            first_temp = first.temp_directory
            acquired = threading.Event()
            second_done = threading.Event()
            second_temp: list[Path] = []

            def open_second() -> None:
                second = duck_module.connect(data_root, "extract", "second")
                second_temp.append(second.temp_directory)
                acquired.set()
                second.close()
                second_done.set()

            waiter = threading.Thread(target=open_second)
            waiter.start()
            time.sleep(0.1)
            self.assertFalse(acquired.is_set(), "second DuckDB operation bypassed the global bound")
            self.assertTrue(first_temp.exists())
            first.close()
            self.assertTrue(second_done.wait(2))
            waiter.join(2)

            self.assertNotEqual(first_temp, second_temp[0])
            self.assertFalse(first_temp.exists())
            self.assertFalse(second_temp[0].exists())
            temp_root = Path(data_root) / "_tmp" / "duckdb"
            self.assertEqual([], list(temp_root.iterdir()))

    def test_duckdb_queue_timeout_releases_slots_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as data_root, patch.object(
            duck_module, "_operation_slots", threading.BoundedSemaphore(1)
        ), patch.dict(
            "os.environ",
            {"DUCKDB_OPERATION_QUEUE_TIMEOUT_SECONDS": "0.05"},
        ):
            first = duck_module.connect(data_root, "extract", "held")
            with self.assertRaisesRegex(TimeoutError, "worker is busy"):
                duck_module.connect(data_root, "preview", "waiting")
            first.close()

            recovered = duck_module.connect(data_root, "preview", "recovered")
            recovered_temp = recovered.temp_directory
            recovered.close()
            self.assertFalse(recovered_temp.exists())

    def test_duckdb_weighted_memory_gate_serializes_jobs_that_exceed_host_total(self):
        environment = {
            "DUCKDB_TOTAL_THREADS": "2",
            "DUCKDB_TOTAL_MEMORY_BYTES": str(64 * 1024 * 1024),
            "DUCKDB_TOTAL_TEMP_BYTES": str(128 * 1024 * 1024),
            "DUCKDB_MAX_CONCURRENT_OPERATIONS": "2",
            "DUCKDB_OPERATION_QUEUE_TIMEOUT_SECONDS": "2",
        }
        budget = ResourceBudget(
            cpuThreads=1,
            memoryBytes=64 * 1024 * 1024,
            tempBytes=64 * 1024 * 1024,
        )
        with tempfile.TemporaryDirectory() as data_root, patch.object(
            duck_module, "_resource_governor", None
        ), patch.object(
            duck_module, "_operation_slots", threading.BoundedSemaphore(2)
        ), patch.dict(
            "os.environ", environment
        ):
            first = duck_module.connect(data_root, "extract", "memory-heavy-1", budget=budget)
            second_acquired = threading.Event()

            def open_second() -> None:
                second = duck_module.connect(data_root, "extract", "memory-heavy-2", budget=budget)
                second_acquired.set()
                second.close()

            waiter = threading.Thread(target=open_second)
            waiter.start()
            time.sleep(0.1)
            self.assertFalse(
                second_acquired.is_set(),
                "second job bypassed the aggregate memory reservation",
            )
            first.close()
            self.assertTrue(second_acquired.wait(2))
            waiter.join(2)
            self.assertFalse(waiter.is_alive())

    def test_managed_duckdb_connection_interrupts_a_real_running_query(self):
        with tempfile.TemporaryDirectory() as data_root:
            connection = duck_module.connect(data_root, "cancel-test", "real-query")
            started = threading.Event()
            errors: list[Exception] = []

            def execute() -> None:
                started.set()
                try:
                    connection.execute(
                        "SELECT sum(i * j) FROM range(100000000) AS left_side(i), "
                        "range(100) AS right_side(j)"
                    ).fetchone()
                except Exception as exc:
                    errors.append(exc)

            runner = threading.Thread(target=execute)
            runner.start()
            self.assertTrue(started.wait(1))
            time.sleep(0.05)
            connection.interrupt()
            runner.join(5)
            connection.close()

            self.assertFalse(runner.is_alive())
            self.assertEqual(1, len(errors))
            self.assertIn("Interrupt", type(errors[0]).__name__)

    def test_boot_job_id_is_echoed_and_duplicate_upload_is_not_scheduled(self):
        with tempfile.TemporaryDirectory() as data_root:
            job_id = str(uuid.uuid4())
            request = json.dumps(
                {
                    "jobId": job_id,
                    "requestId": "file-1",
                    "userId": "user-1",
                    "userDatasetId": "dataset-1",
                    "userDatasetFileId": "file-1",
                    "fileType": "CSV",
                }
            )
            first_tasks = BackgroundTasks()
            second_tasks = BackgroundTasks()
            with patch.object(api_app, "root_path", return_value=data_root):
                first = api_app.convert_user_dataset_file_route(
                    "dataset-1",
                    "file-1",
                    first_tasks,
                    UploadFile(io.BytesIO(b"id,name\n1,Kim\n"), filename="first.csv"),
                    request,
                )
                second = api_app.convert_user_dataset_file_route(
                    "dataset-1",
                    "file-1",
                    second_tasks,
                    UploadFile(io.BytesIO(b"id,name\n2,Lee\n"), filename="second.csv"),
                    request,
                )

            self.assertEqual(job_id, first["jobId"])
            self.assertEqual(job_id, second["jobId"])
            self.assertEqual("ACCEPTED", second["state"])
            self.assertEqual(1, len(first_tasks.tasks))
            self.assertEqual(0, len(second_tasks.tasks))
            upload_path = Path(first_tasks.tasks[0].args[0])
            self.assertEqual(b"id,name\n1,Kim\n", upload_path.read_bytes())


if __name__ == "__main__":
    unittest.main()
