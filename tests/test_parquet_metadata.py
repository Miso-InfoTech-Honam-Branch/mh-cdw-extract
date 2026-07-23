from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import duckdb

from cdw_extract.parquet_metadata import parquet_file_metadata


class ParquetMetadataTest(unittest.TestCase):
    def test_schema_hash_is_stable_for_same_physical_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = root / "first.parquet"
            second = root / "second.parquet"
            changed = root / "changed.parquet"
            connection = duckdb.connect()
            try:
                connection.execute(
                    "COPY (SELECT 1::BIGINT AS patient_id, 'Kim'::VARCHAR AS name) "
                    "TO ? (FORMAT PARQUET)",
                    [first.as_posix()],
                )
                connection.execute(
                    "COPY (SELECT 2::BIGINT AS patient_id, 'Lee'::VARCHAR AS name) "
                    "TO ? (FORMAT PARQUET)",
                    [second.as_posix()],
                )
                connection.execute(
                    "COPY (SELECT '2'::VARCHAR AS patient_id, 'Lee'::VARCHAR AS name) "
                    "TO ? (FORMAT PARQUET)",
                    [changed.as_posix()],
                )

                first_metadata = parquet_file_metadata(first, connection)
                second_metadata = parquet_file_metadata(second, connection)
                changed_metadata = parquet_file_metadata(changed, connection)
            finally:
                connection.close()

            self.assertEqual(
                first_metadata["schemaHash"],
                second_metadata["schemaHash"],
            )
            self.assertNotEqual(
                first_metadata["schemaHash"],
                changed_metadata["schemaHash"],
            )
            self.assertRegex(first_metadata["schemaHash"], r"^[0-9a-f]{64}$")
            self.assertEqual(first.stat().st_size, first_metadata["sizeBytes"])
            self.assertRegex(first_metadata["sha256Checksum"], r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main()
