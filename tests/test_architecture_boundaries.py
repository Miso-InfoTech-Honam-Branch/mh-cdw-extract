from __future__ import annotations

import ast
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path

from cdw_extract.extract import (
    ExtractResultTarget,
    ValidatedExtractRequest,
    normalize_result_target,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "cdw_extract"
FORBIDDEN_HOST_IMPORTS = {
    "app",
    "arq",
    "celery",
    "dramatiq",
    "fastapi",
    "huey",
    "kombu",
    "rq",
}


class ArchitectureBoundaryTest(unittest.TestCase):
    def test_core_package_does_not_import_http_or_queue_hosts(self):
        violations: list[str] = []
        for source_path in PACKAGE_ROOT.rglob("*.py"):
            tree = ast.parse(source_path.read_text(encoding="utf-8"), source_path.as_posix())
            for node in ast.walk(tree):
                imported_modules: list[str] = []
                if isinstance(node, ast.Import):
                    imported_modules.extend(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                    imported_modules.append(node.module)
                for module_name in imported_modules:
                    top_level_name = module_name.split(".", 1)[0]
                    if top_level_name in FORBIDDEN_HOST_IMPORTS:
                        relative_path = source_path.relative_to(PROJECT_ROOT).as_posix()
                        violations.append(
                            f"{relative_path}:{node.lineno} imports {module_name}"
                        )

        self.assertEqual(
            [],
            violations,
            "cdw_extract core must remain independent from HTTP and queue hosts",
        )

    def test_extract_contract_is_normalized_once_into_immutable_snake_case_values(self):
        raw_request = {
            "requestId": "request-1",
            "sourceType": "TABLE",
            "outputFormat": "PARQUET",
            "datasetId": "extract-dataset-1",
            "runId": "run-1",
            "resultTarget": {
                "kind": "user_datst",
                "userId": "user-1",
                "userDatasetId": "dataset-1",
                "userDatasetFileId": "file-1",
                "idempotencyKey": "EXPORT:run-1",
            },
        }

        validated = ValidatedExtractRequest.from_raw(" connection-1 ", raw_request)

        self.assertEqual("connection-1", validated.connection_id)
        self.assertEqual("table", validated.source_type)
        self.assertEqual("parquet", validated.output_format)
        self.assertEqual("user-1", validated.result_target.user_id)
        expected_target = {**raw_request["resultTarget"], "kind": "USER_DATST"}
        self.assertEqual(expected_target, validated.result_target.transport_dict())
        self.assertEqual(expected_target, normalize_result_target(raw_request))

        raw_request["resultTarget"]["userId"] = "mutated-after-validation"
        self.assertEqual("user-1", validated.result_target.user_id)
        with self.assertRaises(FrozenInstanceError):
            validated.result_target.user_id = "cannot-mutate"

    def test_result_target_rejects_incomplete_cross_service_correlation(self):
        request = {
            "sourceType": "table",
            "outputFormat": "parquet",
            "resultTarget": {
                "userId": "user-1",
                "userDatasetId": "dataset-1",
                "userDatasetFileId": "file-1",
                "idempotencyKey": "EXPORT:run-1",
            },
        }

        with self.assertRaisesRegex(ValueError, "datasetId"):
            ExtractResultTarget.from_request(request)


if __name__ == "__main__":
    unittest.main()
