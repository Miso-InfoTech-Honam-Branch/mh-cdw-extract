from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import duckdb

from cdw_extract.transforms.runtime import compile_pipeline_request, preview_pipeline


class PipelineBaseQueryTest(unittest.TestCase):
    def test_output_only_pipeline_applies_base_contains_filter_and_sort(self) -> None:
        with tempfile.TemporaryDirectory() as data_root:
            connection_root = Path(data_root) / "connections" / "connection-1"
            source_file = connection_root / "tables" / "facilities.parquet"
            source_file.parent.mkdir(parents=True)

            writer = duckdb.connect()
            try:
                writer.execute(
                    """
                    COPY (
                        SELECT '서울내과' AS mci_nm, 2::INTEGER AS sort_no
                        UNION ALL SELECT '부산외과', 99
                        UNION ALL SELECT '광주내과', 3
                    ) TO ? (FORMAT PARQUET)
                    """,
                    [source_file.as_posix()],
                )
            finally:
                writer.close()

            (connection_root / "manifest.json").write_text(
                json.dumps(
                    {
                        "connectionId": "connection-1",
                        "status": "COMPLETED",
                        "tables": [
                            {
                                "tableId": "table-1",
                                "schemaName": "public",
                                "tableName": "facilities",
                                "path": "tables/facilities.parquet",
                            }
                        ],
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            request = {
                "requestId": "pipeline-base-query",
                "sourceType": "table",
                "tableId": "table-1",
                "schemaName": "public",
                "tableName": "facilities",
                "columns": [
                    {"name": "mci_nm", "alias": "요양기관명"},
                    {"name": "sort_no", "alias": "우선순위"},
                ],
                "filters": [
                    {"column": "요양기관명", "op": "contains", "value": "내과"}
                ],
                "sorts": [{"column": "우선순위", "direction": "desc"}],
                "sourceColumns": [
                    {
                        "columnId": "src:name",
                        "physicalName": "요양기관명",
                        "label": "요양기관명",
                        "dataType": "STRING",
                    },
                    {
                        "columnId": "src:sort",
                        "physicalName": "우선순위",
                        "label": "우선순위",
                        "dataType": "INT32",
                    },
                ],
                "pipeline": {
                    "pipelineVersion": 1,
                    "steps": [
                        {
                            "stepId": "output",
                            "type": "OUTPUT",
                            "enabled": True,
                            "config": {},
                        }
                    ],
                },
                "limit": 100,
            }

            connection = duckdb.connect()
            try:
                compiled = compile_pipeline_request(
                    "connection-1", request, data_root, connection
                )
                compiled_rows = connection.execute(
                    compiled.sql, compiled.parameters
                ).fetchall()
            finally:
                connection.close()

            self.assertEqual([("광주내과", 3), ("서울내과", 2)], compiled_rows)

            preview = preview_pipeline("connection-1", request, data_root)

            self.assertEqual(
                [
                    {"요양기관명": "광주내과", "우선순위": 3},
                    {"요양기관명": "서울내과", "우선순위": 2},
                ],
                preview["rows"],
            )


if __name__ == "__main__":
    unittest.main()
