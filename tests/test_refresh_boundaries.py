from __future__ import annotations

import importlib
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from cdw_extract.manifest import load_connection_manifest, save_connection_manifest
from cdw_extract.refresh import refresh_tables_impl


refresh_module = importlib.import_module("cdw_extract.refresh")


class RefreshBoundaryTest(unittest.TestCase):
    def test_failed_export_removes_staging_without_replacing_manifest(self) -> None:
        request = {
            "sourceConnection": {"vendor": "clickhouse"},
            "tables": [
                {"tableId": "first", "tableName": "first"},
                {"tableId": "second", "tableName": "second"},
            ],
        }
        prior_manifest = {
            "connectionId": "connection-1",
            "status": "COMPLETED",
            "snapshotId": "prior-snapshot",
            "tables": [],
            "updatedAt": "2026-07-22T00:00:00+00:00",
        }
        generation = uuid.UUID("11111111-2222-4333-8444-555555555555")
        snapshot_id = f"refresh-job-{generation.hex[:12]}"

        def write_table(_source, table, output, **_kwargs):
            Path(output).write_bytes(b"partial-parquet")
            if table["tableId"] == "second":
                raise RuntimeError("upstream export failed")
            return 1

        with tempfile.TemporaryDirectory() as data_root:
            save_connection_manifest("connection-1", data_root, prior_manifest)
            with (
                patch.object(
                    refresh_module.uuid,
                    "uuid4",
                    return_value=generation,
                ),
                patch.object(
                    refresh_module,
                    "write_clickhouse_table_parquet",
                    side_effect=write_table,
                ),
            ):
                with self.assertRaisesRegex(RuntimeError, "upstream export failed"):
                    refresh_tables_impl(
                        "connection-1",
                        request,
                        data_root,
                        "refresh-job",
                    )

            connection_root = Path(data_root) / "connections" / "connection-1"
            self.assertFalse((connection_root / "_tmp" / snapshot_id).exists())
            self.assertFalse((connection_root / "tables" / snapshot_id).exists())
            self.assertEqual(
                prior_manifest,
                load_connection_manifest("connection-1", data_root),
            )

    def test_source_attach_failure_closes_connection_and_removes_staging(self) -> None:
        class FailingConnection:
            def __init__(self) -> None:
                self.closed = False

            def execute(self, statement, _parameters=None):
                if statement == "ATTACH source":
                    raise RuntimeError("attach failed")
                return self

            def close(self) -> None:
                self.closed = True

        connection = FailingConnection()
        generation = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee")
        snapshot_id = f"refresh-job-{generation.hex[:12]}"
        request = {
            "sourceConnection": {"vendor": "postgresql"},
            "tables": [{"tableId": "patients", "tableName": "patients"}],
        }

        with (
            tempfile.TemporaryDirectory() as data_root,
            patch.object(
                refresh_module.uuid,
                "uuid4",
                return_value=generation,
            ),
            patch.object(
                refresh_module,
                "connect",
                return_value=connection,
            ),
            patch.object(
                refresh_module,
                "source_attach_sql",
                return_value=("postgres", "ATTACH source"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "attach failed"):
                refresh_tables_impl(
                    "connection-1",
                    request,
                    data_root,
                    "refresh-job",
                )

            staging = (
                Path(data_root) / "connections" / "connection-1" / "_tmp" / snapshot_id
            )
            self.assertTrue(connection.closed)
            self.assertFalse(staging.exists())


if __name__ == "__main__":
    unittest.main()
