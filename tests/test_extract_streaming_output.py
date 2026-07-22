from __future__ import annotations

import importlib
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import duckdb


extract_module = importlib.import_module("cdw_extract.extract")


class RecordingConnection:
    def __init__(self) -> None:
        self.connection = duckdb.connect(database=":memory:")
        self.statements: list[str] = []

    def execute(self, sql: str, parameters: list[object] | None = None):
        self.statements.append(sql)
        return self.connection.execute(sql, parameters or [])

    def close(self) -> None:
        self.connection.close()


class ExtractStreamingOutputTest(unittest.TestCase):
    def test_pipeline_is_copied_directly_without_materializing_a_temp_table(self) -> None:
        request = {
            "requestId": "direct-copy",
            "sourceType": "table",
            "tableId": "unused-by-compiled-plan",
            "outputFormat": "parquet",
            "pipeline": {"pipelineVersion": 1, "steps": []},
        }
        compiled = SimpleNamespace(
            sql="SELECT ?::INTEGER AS value FROM range(?)",
            parameters=[7, 3],
            output_schema=[],
        )
        connection = RecordingConnection()

        with tempfile.TemporaryDirectory() as data_root, patch.object(
            extract_module, "connect", return_value=connection
        ), patch.object(
            extract_module, "compile_pipeline_request", return_value=compiled
        ):
            result = extract_module.execute_extract(
                "connection-1", request, data_root, "11111111-1111-4111-8111-111111111111"
            )

        self.assertEqual(3, result["rowCount"])
        self.assertTrue(any(statement.startswith("COPY (") for statement in connection.statements))
        self.assertFalse(
            any("CREATE TEMP TABLE" in statement.upper() for statement in connection.statements)
        )


if __name__ == "__main__":
    unittest.main()
