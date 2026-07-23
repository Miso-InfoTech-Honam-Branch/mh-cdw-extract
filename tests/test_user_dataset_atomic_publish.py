from __future__ import annotations

import errno
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cdw_extract.user_dataset import (
    convert_user_dataset_file_from_path,
    dataset_file_manifest_path,
    dataset_file_parquet_path,
    dataset_file_relative_path,
    dataset_file_root,
    file_sha256,
    load_dataset_file_manifest,
    publish_dataset_file_artifact,
)


class UserDatasetAtomicPublishTest(unittest.TestCase):
    @staticmethod
    def _request(*, job_id: str = "job-1") -> dict:
        return {
            "requestId": "request-1",
            "jobId": job_id,
            "userId": "user-1",
            "userDatasetId": "dataset-1",
            "userDatasetFileId": "file-1",
            "originalFileName": "patients.csv",
            "fileType": "CSV",
            "headerYn": True,
            "delimiter": ",",
            "fileEncoding": "utf-8",
        }

    @staticmethod
    def _write_csv(path: Path, rows: str = "patient_id,name\n1,Kim\n2,Lee\n") -> None:
        path.write_text(rows, encoding="utf-8")

    def test_duplicate_delivery_reuses_complete_generation_without_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_root = root / "data"
            upload = root / "patients.csv"
            self._write_csv(upload)
            request = self._request()

            with patch(
                "cdw_extract.user_dataset.utc_now",
                side_effect=["2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00"],
            ):
                first = convert_user_dataset_file_from_path(upload, data_root, request)
                first_checksum = file_sha256(
                    dataset_file_parquet_path(data_root, "user-1", "dataset-1", "file-1")
                )
                second = convert_user_dataset_file_from_path(upload, data_root, request)

            manifest = load_dataset_file_manifest(
                data_root, "user-1", "dataset-1", "file-1"
            )
            published_path = dataset_file_parquet_path(
                data_root, "user-1", "dataset-1", "file-1"
            )
            self.assertEqual("SUCCESS", first["status"])
            self.assertEqual(first, second)
            self.assertEqual(
                dataset_file_relative_path("user-1", "dataset-1", "file-1"),
                first["resultPath"],
            )
            self.assertFalse(Path(first["resultPath"]).is_absolute())
            self.assertEqual(published_path.stat().st_size, first["resultSizeBytes"])
            self.assertEqual(file_sha256(published_path), first["resultSha256"])
            self.assertRegex(first["schemaHash"], r"^[0-9a-f]{64}$")
            self.assertEqual(first["resultSizeBytes"], manifest["sizeBytes"])
            self.assertEqual(first["resultSha256"], manifest["sha256Checksum"])
            self.assertEqual(first["schemaHash"], manifest["schemaHash"])
            self.assertEqual("2026-01-01T00:00:00+00:00", manifest["createdAt"])
            self.assertEqual(
                first_checksum,
                file_sha256(
                    dataset_file_parquet_path(
                        data_root, "user-1", "dataset-1", "file-1"
                    )
                ),
            )
            self.assertTrue(
                dataset_file_manifest_path(
                    data_root, "user-1", "dataset-1", "file-1"
                ).with_name("schema.json").is_file()
            )

    def test_extract_result_retry_keeps_its_idempotency_and_checksum_policy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_root = root / "data"

            def stage(run: str) -> Path:
                parquet = root / run / "artifact" / "parquet" / "data.parquet"
                parquet.parent.mkdir(parents=True)
                parquet.write_bytes(b"same-extract-result")
                return parquet

            first = publish_dataset_file_artifact(
                stage("run-1"),
                data_root,
                "user-1",
                "dataset-1",
                "file-1",
                {
                    "idempotencyKey": "EXPORT:run-1",
                    "requestId": "first-request",
                    "columns": [{"name": "patient_id"}],
                },
            )
            retried = publish_dataset_file_artifact(
                stage("run-2"),
                data_root,
                "user-1",
                "dataset-1",
                "file-1",
                {
                    "idempotencyKey": "EXPORT:run-1",
                    "requestId": "retry-request",
                    "columns": [{"name": "changed-only-in-retry-manifest"}],
                },
            )

            self.assertEqual(first, retried)
            self.assertEqual("first-request", retried["requestId"])
            self.assertEqual("PARQUET", retried["fileType"])
            self.assertEqual(
                file_sha256(
                    dataset_file_parquet_path(
                        data_root, "user-1", "dataset-1", "file-1"
                    )
                ),
                retried["sha256Checksum"],
            )

    def test_metadata_write_failure_never_exposes_partial_generation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_root = root / "data"
            upload = root / "patients.csv"
            self._write_csv(upload)
            original_write_text = Path.write_text

            def fail_schema_write(path: Path, data: str, *args, **kwargs):
                if path.name == "schema.json":
                    raise OSError("injected schema write failure")
                return original_write_text(path, data, *args, **kwargs)

            with patch.object(Path, "write_text", fail_schema_write):
                with self.assertRaisesRegex(OSError, "injected schema write failure"):
                    convert_user_dataset_file_from_path(
                        upload, data_root, self._request()
                    )

            final_root = dataset_file_root(
                data_root, "user-1", "dataset-1", "file-1"
            )
            self.assertFalse(final_root.exists())
            self.assertEqual(
                [],
                list(final_root.parent.glob(f".{final_root.name}.*.staging")),
            )

            result = convert_user_dataset_file_from_path(
                upload, data_root, self._request()
            )
            self.assertEqual("SUCCESS", result["status"])
            self.assertTrue(
                dataset_file_manifest_path(
                    data_root, "user-1", "dataset-1", "file-1"
                ).is_file()
            )
            self.assertTrue(
                dataset_file_manifest_path(
                    data_root, "user-1", "dataset-1", "file-1"
                ).with_name("schema.json").is_file()
            )

    def test_different_delivery_cannot_replace_reserved_generation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_root = root / "data"
            first_upload = root / "patients.csv"
            second_upload = root / "changed.csv"
            self._write_csv(first_upload)
            self._write_csv(second_upload, "patient_id,name\n3,Park\n")

            convert_user_dataset_file_from_path(
                first_upload, data_root, self._request(job_id="job-1")
            )
            parquet_path = dataset_file_parquet_path(
                data_root, "user-1", "dataset-1", "file-1"
            )
            original_checksum = file_sha256(parquet_path)

            with self.assertRaisesRegex(FileExistsError, "target already exists"):
                convert_user_dataset_file_from_path(
                    second_upload, data_root, self._request(job_id="job-2")
                )

            self.assertEqual(original_checksum, file_sha256(parquet_path))
            self.assertEqual(
                "job-1",
                load_dataset_file_manifest(
                    data_root, "user-1", "dataset-1", "file-1"
                )["jobId"],
            )

    def test_cross_filesystem_workspace_is_copied_to_same_parent_before_publish(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data_root = root / "data"
            upload = root / "patients.csv"
            workspace = root / "external-workspace"
            self._write_csv(upload)
            final_root = dataset_file_root(
                data_root, "user-1", "dataset-1", "file-1"
            )
            original_replace = Path.replace
            replacements: list[tuple[Path, Path]] = []

            def record_replace(path: Path, target: Path):
                replacements.append((path, Path(target)))
                return original_replace(path, target)

            with patch(
                "cdw_extract.user_dataset.os.link",
                side_effect=OSError(errno.EXDEV, "injected cross-device link"),
            ), patch.object(Path, "replace", record_replace):
                result = convert_user_dataset_file_from_path(
                    upload,
                    data_root,
                    self._request(),
                    workspace=workspace,
                )

            publication_renames = [
                (source, target)
                for source, target in replacements
                if target == final_root
            ]
            self.assertEqual("SUCCESS", result["status"])
            self.assertEqual(1, len(publication_renames))
            publication_staging, published_target = publication_renames[0]
            self.assertEqual(published_target.parent, publication_staging.parent)
            self.assertTrue(publication_staging.name.endswith(".staging"))
            self.assertTrue(
                dataset_file_parquet_path(
                    data_root, "user-1", "dataset-1", "file-1"
                ).is_file()
            )


if __name__ == "__main__":
    unittest.main()
