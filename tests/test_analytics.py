from __future__ import annotations

import json
import tempfile
import unittest
from unittest.mock import patch

from pydantic import ValidationError

import app as api_app
from cdw_extract.analytics import run_analytics_query
from cdw_extract.analytics_models import AnalyticsQueryRequest, AnalyticsQueryResponse
from cdw_extract.duck import connect
from cdw_extract.user_dataset import (
    dataset_file_manifest_path,
    dataset_file_parquet_path,
    dataset_file_relative_path,
)


USER_ID = "user-analytics"
DATASET_ID = "dataset-analytics"


def create_source(data_root: str, file_id: str = "file-upload", artifact_kind: str = "UPLOAD") -> None:
    parquet_path = dataset_file_parquet_path(data_root, USER_ID, DATASET_ID, file_id)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect()
    try:
        connection.execute(
            """
            COPY (
                SELECT data.*,
                    TIME '12:00:00' AS clock_time,
                    CASE CAST(x AS INTEGER)
                        WHEN 1 THEN CAST('1000000000000000000.000000000000000000' AS DECIMAL(38,18))
                        WHEN 2 THEN CAST('1000000000000000000.000000000000000001' AS DECIMAL(38,18))
                        WHEN 3 THEN CAST('1000000000000000000.000000000000000002' AS DECIMAL(38,18))
                        WHEN 4 THEN CAST('1000000000000000000.000000000000000003' AS DECIMAL(38,18))
                        WHEN 5 THEN CAST('1000000000000000000.000000000000000100' AS DECIMAL(38,18))
                        ELSE CAST('1000000000000000000.000000000000000000' AS DECIMAL(38,18))
                    END AS precise_value,
                    CASE CAST(x AS INTEGER)
                        WHEN 1 THEN 1
                        WHEN 2 THEN 10
                        WHEN 3 THEN 2
                        ELSE 20 + CAST(x AS INTEGER)
                    END AS numeric_category,
                    [1, 2]::INTEGER[] AS list_value
                FROM (VALUES
                    ('A', 'S1', DATE '2024-01-02', 1.0, 1.0, 10.0, 'G1', 'Visit',     'Start', 'Middle', 'R1', 'D1', 'L1',   1.0, true,  CAST('NaN' AS DOUBLE)),
                    ('A', 'S1', DATE '2024-01-20', 2.0, 4.0, 20.0, 'G1', 'Visit',     'Start', 'Middle', 'R1', 'D1', 'L2',   2.0, true,  1.0),
                    ('B', 'S2', DATE '2024-02-10', 3.0, 9.0, 30.0, 'G1', 'Treat',     'Middle','End',    'R1', 'D2', 'L3',   3.0, false, 1.0),
                    ('B', 'S2', DATE '2024-03-15', 4.0,16.0, 40.0, 'G1', 'Treat',     'Middle','End',    'R1', 'D2', 'L4',   4.0, true,  1.0),
                    ('C', 'S2', DATE '2024-04-01', 5.0,25.0, 50.0, 'G1', 'Discharge', 'End',   'End',    'R2', 'D3', 'L5', 100.0, true,  1.0),
                    ('D', 'S3', DATE '2024-04-15', 6.0,36.0, 60.0, 'G2', 'Discharge', 'Start', 'End',    'R2', 'D3', 'L6',   6.0, false, 1.0),
                    (NULL,'S3', DATE '2024-05-01', 7.0,49.0, NULL, 'G2', NULL,        NULL,    'End',    'R2', 'D4', 'L7',   7.0, true,  1.0)
                ) AS data(
                    category, series, event_date, x, y, size, box_group, stage,
                    source_node, target_node, level1, level2, level3, value, active, json_number
                )
            ) TO ? (FORMAT PARQUET)
            """,
            [parquet_path.as_posix()],
        )
    finally:
        connection.close()
    manifest_path = dataset_file_manifest_path(data_root, USER_ID, DATASET_ID, file_id)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "requestId": f"request-{file_id}",
                "jobId": f"job-{file_id}",
                "userId": USER_ID,
                "userDatasetId": DATASET_ID,
                "userDatasetFileId": file_id,
                "path": dataset_file_relative_path(USER_ID, DATASET_ID, file_id),
                "status": "SUCCESS",
                "artifactKind": artifact_kind,
                "createdAt": "2026-07-14T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )


def request_payload(
    chart_type: str,
    encoding: dict,
    *,
    file_id: str = "file-upload",
    filters: list[dict] | None = None,
    sorts: list[dict] | None = None,
    limit: int = 100,
    options: dict | None = None,
) -> dict:
    return {
        "schemaVersion": 1,
        "requestId": f"analytics-{chart_type.lower()}",
        "source": {
            "sourceKind": "USER_DATST",
            "userId": USER_ID,
            "userDatasetId": DATASET_ID,
            "userDatasetFileId": file_id,
        },
        "chartType": chart_type,
        "encoding": encoding,
        "filters": filters or [],
        "sorts": sorts or [],
        "limit": limit,
        "options": options or {},
    }


def run(data_root: str, payload: dict) -> AnalyticsQueryResponse:
    return run_analytics_query(AnalyticsQueryRequest.model_validate(payload), data_root)


class AnalyticsQueryTest(unittest.TestCase):
    def test_parse_date_and_number_calculated_fields_are_queryable(self):
        with tempfile.TemporaryDirectory() as data_root:
            create_source(data_root)
            payload = request_payload(
                "LINE",
                {
                    "category": {"derivedFieldId": "parsedDate", "timeGrain": "MONTH"},
                    "value": {"derivedFieldId": "parsedNumber", "aggregation": "SUM"},
                },
            )
            payload["calculatedFields"] = [
                {
                    "id": "parsedDate", "name": "Parsed date", "dataType": "DATE",
                    "formula": {
                        "op": "PARSE_DATE", "args": [{"op": "COLUMN", "column": "event_date"}],
                        "format": "YYYY-MM-DD", "onError": "NULL",
                    },
                },
                {
                    "id": "parsedNumber", "name": "Parsed number", "dataType": "NUMBER",
                    "formula": {
                        "op": "PARSE_NUMBER", "args": [{"op": "COLUMN", "column": "value"}],
                        "format": "PLAIN", "onError": "NULL",
                    },
                },
            ]

            response = run(data_root, payload)

            self.assertTrue(response.rows)
            self.assertTrue(all(row["value"] is not None for row in response.rows))

    def test_ten_chart_happy_paths_use_actual_parquet(self):
        with tempfile.TemporaryDirectory() as data_root:
            create_source(data_root)
            cases = {
                "KPI": {
                    "value": {"column": "value", "aggregation": "SUM"},
                },
                "TABLE": {
                    "category": {"column": "category"},
                    "value": {"column": "value", "aggregation": "SUM"},
                    "series": {"column": "series"},
                },
                "BAR": {
                    "category": {"column": "category"},
                    "value": {"column": "value", "aggregation": "SUM"},
                },
                "PIE": {
                    "category": {"column": "category"},
                    "value": {"aggregation": "COUNT"},
                },
                "LINE": {
                    "category": {"column": "event_date", "timeGrain": "MONTH"},
                    "value": {"column": "value", "aggregation": "AVG"},
                    "series": {"column": "series"},
                },
                "SCATTER": {
                    "x": {"column": "x"},
                    "y": {"column": "y"},
                    "size": {"column": "size"},
                    "series": {"column": "series"},
                },
                "BOXPLOT": {
                    "value": {"column": "value"},
                    "group": {"column": "box_group"},
                },
                "FUNNEL": {
                    "stage": {"column": "stage"},
                    "value": {"aggregation": "COUNT"},
                },
                "SANKEY": {
                    "source": {"column": "source_node"},
                    "target": {"column": "target_node"},
                    "value": {"column": "value", "aggregation": "SUM"},
                },
                "TREEMAP": {
                    "hierarchy": [
                        {"column": "level1"},
                        {"column": "level2"},
                        {"column": "level3"},
                    ],
                    "value": {"column": "value", "aggregation": "SUM"},
                },
            }
            expected_keys = {
                "KPI": {"value"},
                "TABLE": {"category", "value", "series"},
                "BAR": {"category", "value"},
                "PIE": {"category", "value"},
                "LINE": {"category", "value", "series"},
                "SCATTER": {"x", "y", "size", "series"},
                "BOXPLOT": {
                    "category", "count", "min", "q1", "median", "q3", "max",
                    "lowerFence", "upperFence", "outlierCount",
                },
                "FUNNEL": {"category", "value"},
                "SANKEY": {"source", "target", "value"},
                "TREEMAP": {"level0", "level1", "level2", "value"},
            }
            for chart_type, encoding in cases.items():
                with self.subTest(chart_type=chart_type):
                    response = run(data_root, request_payload(chart_type, encoding))
                    self.assertGreater(response.row_count, 0)
                    self.assertEqual(expected_keys[chart_type], set(response.rows[0]))
                    self.assertTrue(response.source_version.startswith("sha256:"))

            kpi = run(data_root, request_payload("KPI", cases["KPI"]))
            self.assertEqual([{"value": 123}], kpi.rows)
            self.assertFalse(kpi.truncated)

            box = run(data_root, request_payload("BOXPLOT", cases["BOXPLOT"]))
            group_one = next(row for row in box.rows if row["category"] == "G1")
            self.assertEqual(5, group_one["count"])
            self.assertEqual(4.0, group_one["max"])
            self.assertEqual(1, group_one["outlierCount"])

            precise_box = run(
                data_root,
                request_payload(
                    "BOXPLOT",
                    {
                        "value": {"column": "precise_value"},
                        "group": {"column": "box_group"},
                    },
                ),
            )
            precise_group_one = next(row for row in precise_box.rows if row["category"] == "G1")
            self.assertEqual(1, precise_group_one["outlierCount"])

    def test_kpi_contract_and_table_category_semantics(self):
        with tempfile.TemporaryDirectory() as data_root:
            create_source(data_root)

            filtered_kpi = run(
                data_root,
                request_payload(
                    "KPI",
                    {"value": {"aggregation": "COUNT"}},
                    filters=[{"column": "active", "operator": "EQ", "value": True}],
                ),
            )
            self.assertEqual([{"value": 5}], filtered_kpi.rows)

            invalid_kpis = [
                request_payload("KPI", {}),
                request_payload("KPI", {"value": {"column": "value"}}),
                request_payload(
                    "KPI",
                    {
                        "category": {"column": "category"},
                        "value": {"column": "value", "aggregation": "SUM"},
                    },
                ),
            ]
            for payload in invalid_kpis:
                with self.subTest(payload=payload):
                    with self.assertRaises(ValueError):
                        run(data_root, payload)

            table_payload = request_payload(
                "TABLE",
                {
                    "category": {"column": "category"},
                    "value": {"column": "value", "aggregation": "SUM"},
                },
                sorts=[{"field": "category", "direction": "ASC"}],
            )
            table = run(data_root, table_payload)
            self.assertEqual(["A", "B", "C", "D"], [row["category"] for row in table.rows])
            self.assertEqual([3, 7, 100, 6], [row["value"] for row in table.rows])

            table_payload["topN"] = {
                "enabled": True,
                "count": 2,
                "by": "value",
                "direction": "DESC",
                "includeOthers": False,
            }
            top_table = run(data_root, table_payload)
            self.assertEqual(["C", "B"], [row["category"] for row in top_table.rows])
            self.assertTrue(top_table.truncated)

    def test_upload_and_extract_artifacts_share_user_datst_contract(self):
        with tempfile.TemporaryDirectory() as data_root:
            create_source(data_root, "file-upload", "UPLOAD")
            create_source(data_root, "file-extract", "EXTRACT_RESULT")
            encoding = {
                "category": {"column": "category"},
                "value": {"column": "value", "aggregation": "SUM"},
            }
            upload = run(data_root, request_payload("BAR", encoding, file_id="file-upload"))
            extract = run(data_root, request_payload("BAR", encoding, file_id="file-extract"))
            self.assertEqual(upload.rows, extract.rows)
            self.assertNotEqual(upload.source_version, extract.source_version)

    def test_filters_time_grain_aggregations_top_n_and_others(self):
        with tempfile.TemporaryDirectory() as data_root:
            create_source(data_root)
            filtered = run(
                data_root,
                request_payload(
                    "BAR",
                    {
                        "category": {"column": "category"},
                        "value": {"column": "value", "aggregation": "AVG"},
                    },
                    filters=[
                        {"column": "x", "operator": "BETWEEN", "values": [1, 4]},
                        {"column": "category", "operator": "IN", "values": ["A", "B"]},
                    ],
                ),
            )
            self.assertEqual({"A": 1.5, "B": 3.5}, {row["category"]: row["value"] for row in filtered.rows})

            quarters = run(
                data_root,
                request_payload(
                    "LINE",
                    {
                        "category": {"column": "event_date", "timeGrain": "QUARTER"},
                        "value": {"aggregation": "COUNT"},
                    },
                ),
            )
            self.assertEqual(["2024-01-01", "2024-04-01"], [row["category"][:10] for row in quarters.rows])
            self.assertEqual([4, 3], [row["value"] for row in quarters.rows])

            top = run(
                data_root,
                request_payload(
                    "BAR",
                    {
                        "category": {"column": "category"},
                        "value": {"column": "value", "aggregation": "SUM"},
                    },
                    sorts=[{"field": "value", "direction": "DESC"}],
                    limit=2,
                ),
            )
            self.assertTrue(top.truncated)
            self.assertEqual(2, top.row_count)
            self.assertTrue(any("omitted" in warning for warning in top.warnings))

            others = run(
                data_root,
                request_payload(
                    "BAR",
                    {
                        "category": {"column": "category"},
                        "value": {"column": "value", "aggregation": "SUM"},
                    },
                    sorts=[{"field": "category", "direction": "ASC"}],
                    limit=3,
                    options={"includeOthers": True, "othersLabel": "기타"},
                ),
            )
            self.assertFalse(others.truncated)
            self.assertLessEqual(others.row_count, 3)
            self.assertIn("기타", {row["category"] for row in others.rows})
            self.assertEqual(["A", "B", "기타"], [row["category"] for row in others.rows])

            numeric_others = run(
                data_root,
                request_payload(
                    "BAR",
                    {
                        "category": {"column": "numeric_category"},
                        "value": {"aggregation": "COUNT"},
                    },
                    sorts=[{"field": "category", "direction": "ASC"}],
                    limit=3,
                    options={"includeOthers": True, "othersLabel": "기타"},
                ),
            )
            self.assertEqual(["1", "2", "기타"], [row["category"] for row in numeric_others.rows])

            with self.assertRaisesRegex(ValueError, "othersLabel conflicts"):
                run(
                    data_root,
                    request_payload(
                        "BAR",
                        {
                            "category": {"column": "category"},
                            "value": {"column": "value", "aggregation": "SUM"},
                        },
                        limit=3,
                        options={"includeOthers": True, "othersLabel": "B"},
                    ),
                )

    def test_invalid_roles_columns_types_operators_and_sql_identifier_are_rejected(self):
        with tempfile.TemporaryDirectory() as data_root:
            create_source(data_root)
            invalid_payloads = [
                request_payload("BAR", {"value": {"aggregation": "COUNT"}}),
                request_payload(
                    "BAR",
                    {
                        "category": {"column": "category"},
                        "x": {"column": "x"},
                        "value": {"aggregation": "COUNT"},
                    },
                ),
                request_payload(
                    "BAR",
                    {"category": {"column": "missing"}, "value": {"aggregation": "COUNT"}},
                ),
                request_payload("SCATTER", {"x": {"column": "category"}, "y": {"column": "y"}}),
                request_payload(
                    "BAR",
                    {"category": {"column": "list_value"}, "value": {"aggregation": "COUNT"}},
                ),
                request_payload(
                    "BAR",
                    {
                        "category": {"column": "category\" OR 1=1 --"},
                        "value": {"aggregation": "COUNT"},
                    },
                ),
            ]
            for payload in invalid_payloads:
                with self.subTest(payload=payload):
                    with self.assertRaises(ValueError):
                        run(data_root, payload)

            bad_operator = request_payload(
                "BAR",
                {"category": {"column": "category"}, "value": {"aggregation": "COUNT"}},
                filters=[{"column": "category", "operator": "RAW_SQL", "value": "A"}],
            )
            with self.assertRaises(ValidationError):
                AnalyticsQueryRequest.model_validate(bad_operator)

            glob_source = request_payload(
                "BAR",
                {"category": {"column": "category"}, "value": {"aggregation": "COUNT"}},
            )
            glob_source["source"]["userId"] = "*"
            with self.assertRaises(ValidationError):
                AnalyticsQueryRequest.model_validate(glob_source)

            bad_type = request_payload(
                "BAR",
                {"category": {"column": "category"}, "value": {"aggregation": "COUNT"}},
                filters=[{"column": "x", "operator": "GT", "value": "not-a-number"}],
            )
            with self.assertRaisesRegex(ValueError, "requires a number"):
                run(data_root, bad_type)

            unsupported_time_grain = request_payload(
                "LINE",
                {
                    "category": {"column": "clock_time", "timeGrain": "DAY"},
                    "value": {"aggregation": "COUNT"},
                },
            )
            with self.assertRaisesRegex(ValueError, "DATE or TIMESTAMP"):
                run(data_root, unsupported_time_grain)

            mixed_sankey = run(
                data_root,
                request_payload(
                    "SANKEY",
                    {
                        "source": {"column": "source_node"},
                        "target": {"column": "x"},
                        "value": {"aggregation": "COUNT"},
                    },
                ),
            )
            self.assertGreater(mixed_sankey.row_count, 0)
            self.assertTrue(
                all(isinstance(row["source"], str) and isinstance(row["target"], str) for row in mixed_sankey.rows)
            )

    def test_closed_filter_and_aggregation_enums_execute_without_raw_sql(self):
        with tempfile.TemporaryDirectory() as data_root:
            create_source(data_root)
            encoding = {
                "category": {"column": "category"},
                "value": {"aggregation": "COUNT"},
            }
            filters = [
                {"column": "category", "operator": "EQ", "value": "A"},
                {"column": "category", "operator": "NE", "value": "Z"},
                {"column": "x", "operator": "GT", "value": 0},
                {"column": "x", "operator": "GTE", "value": 1},
                {"column": "x", "operator": "LT", "value": 8},
                {"column": "x", "operator": "LTE", "value": 7},
                {"column": "category", "operator": "IN", "values": ["A", "B"]},
                {"column": "category", "operator": "CONTAINS", "value": "A"},
                {"column": "x", "operator": "BETWEEN", "values": [1, 7]},
                {"column": "size", "operator": "IS_NOT_NULL"},
            ]
            response = run(data_root, request_payload("BAR", encoding, filters=filters))
            self.assertEqual([{"category": "A", "value": 2}], response.rows)

            nulls = run(
                data_root,
                request_payload(
                    "BAR",
                    encoding,
                    filters=[{"column": "size", "operator": "IS_NULL"}],
                    options={"nullPolicy": "INCLUDE"},
                ),
            )
            self.assertEqual([{"category": None, "value": 1}], nulls.rows)

            injected = run(
                data_root,
                request_payload(
                    "BAR",
                    encoding,
                    filters=[{"column": "category", "operator": "EQ", "value": "A' OR 1=1 --"}],
                ),
            )
            self.assertEqual([], injected.rows)

            for aggregation in ["COUNT", "COUNT_DISTINCT", "SUM", "AVG", "MIN", "MAX", "MEDIAN"]:
                with self.subTest(aggregation=aggregation):
                    value = (
                        {"column": "value", "aggregation": aggregation}
                        if aggregation != "COUNT"
                        else {"aggregation": "COUNT"}
                    )
                    aggregated = run(
                        data_root,
                        request_payload(
                            "BAR",
                            {"category": {"column": "category"}, "value": value},
                        ),
                    )
                    self.assertGreater(aggregated.row_count, 0)
                    self.assertTrue(all(isinstance(row["value"], (int, float)) for row in aggregated.rows))

    def test_caps_truncation_sampling_json_types_and_route_contract(self):
        long_label_payload = request_payload(
            "BAR",
            {
                "category": {"column": "category", "label": "가" * 255},
                "value": {"aggregation": "COUNT"},
            },
        )
        AnalyticsQueryRequest.model_validate(long_label_payload)
        long_label_payload["encoding"]["category"]["label"] = "가" * 256
        with self.assertRaises(ValidationError):
            AnalyticsQueryRequest.model_validate(long_label_payload)

        with tempfile.TemporaryDirectory() as data_root:
            create_source(data_root)
            scatter = run(
                data_root,
                request_payload(
                    "SCATTER",
                    {"x": {"column": "x"}, "y": {"column": "y"}},
                    limit=10,
                    options={"scatterSampleSize": 2},
                ),
            )
            self.assertEqual(2, scatter.row_count)
            self.assertTrue(scatter.truncated)
            self.assertTrue(any("reservoir" in warning for warning in scatter.warnings))

            nonfinite = run(
                data_root,
                request_payload(
                    "BAR",
                    {
                        "category": {"column": "category"},
                        "value": {"column": "json_number", "aggregation": "SUM"},
                    },
                ),
            )
            category_a = next(row for row in nonfinite.rows if row["category"] == "A")
            self.assertIsNone(category_a["value"])
            self.assertTrue(any("Non-finite" in warning for warning in nonfinite.warnings))
            finite_values = [row["value"] for row in nonfinite.rows if row["value"] is not None]
            self.assertTrue(all(isinstance(value, (int, float)) for value in finite_values))

            route = next(
                route
                for route in api_app.app.routes
                if getattr(route, "path", None) == "/api/v1/analytics/query"
            )
            self.assertEqual(200, route.status_code)
            self.assertIs(AnalyticsQueryResponse, route.response_model)

            with patch.dict("os.environ", {"ANALYTICS_MAX_SOURCE_BYTES": "1"}):
                full_scan = run(
                    data_root,
                    request_payload(
                        "KPI",
                        {"value": {"aggregation": "COUNT"}},
                    ),
                )
                self.assertEqual([{"value": 7}], full_scan.rows)
                self.assertFalse(full_scan.truncated)
                self.assertNotIn("sourceTruncated", full_scan.metadata)
                self.assertNotIn("analyzedSourceRows", full_scan.metadata)
                self.assertFalse(any("rows were analyzed" in warning for warning in full_scan.warnings))


if __name__ == "__main__":
    unittest.main()
