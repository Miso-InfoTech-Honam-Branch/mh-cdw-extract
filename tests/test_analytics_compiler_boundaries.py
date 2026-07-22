from __future__ import annotations

import unittest

from cdw_extract.analytics_compiler import AnalyticsCompiler
from cdw_extract.analytics_models import AnalyticsQueryRequest


SOURCE = {
    "sourceKind": "USER_DATST",
    "userId": "user-1",
    "userDatasetId": "dataset-1",
    "userDatasetFileId": "file-1",
}
PARQUET_PATH = "C:/data/source.parquet"


class AnalyticsCompilerBoundaryTest(unittest.TestCase):
    def test_nested_calculation_and_others_keep_sql_and_parameter_order(self):
        request = AnalyticsQueryRequest.model_validate(
            {
                "schemaVersion": 1,
                "requestId": "compiler-boundary",
                "source": SOURCE,
                "chartType": "BAR",
                "encoding": {
                    "category": {"column": "category"},
                    "value": {
                        "derivedFieldId": "adjusted",
                        "aggregation": "SUM",
                    },
                },
                "filters": [{"column": "category", "operator": "EQ", "value": "A"}],
                "limit": 4,
                "topN": {
                    "enabled": True,
                    "count": 3,
                    "includeOthers": True,
                },
                "options": {"othersLabel": "Other bucket"},
                "calculatedFields": [
                    {
                        "id": "adjusted",
                        "name": "Adjusted",
                        "dataType": "NUMBER",
                        "formula": {
                            "op": "CASE",
                            "branches": [
                                {
                                    "when": {
                                        "op": "GT",
                                        "args": [
                                            {"op": "COLUMN", "column": "value"},
                                            {"op": "LITERAL", "value": 10},
                                        ],
                                    },
                                    "then": {
                                        "op": "ADD",
                                        "args": [
                                            {"op": "COLUMN", "column": "value"},
                                            {"op": "LITERAL", "value": 2},
                                        ],
                                    },
                                }
                            ],
                            "else": {
                                "op": "COALESCE",
                                "args": [
                                    {"op": "COLUMN", "column": "value"},
                                    {"op": "LITERAL", "value": 0},
                                ],
                            },
                        },
                    }
                ],
            }
        )

        compiled = AnalyticsCompiler(
            request,
            PARQUET_PATH,
            [("category", "VARCHAR"), ("value", "DOUBLE")],
        ).compile()

        self.assertEqual(
            'WITH aggregated AS (SELECT "category" AS "category", '
            'sum("__calculated_0") AS "value" FROM '
            '(SELECT "__raw".*, (CASE WHEN ("value" > ?) THEN ("value" + ?) '
            'ELSE coalesce("value", ?) END) AS "__calculated_0" '
            'FROM read_parquet(?) AS "__raw") AS "__src" '
            'WHERE "category" = ? AND "category" IS NOT NULL GROUP BY 1), '
            'ranked AS (SELECT "category", "value", '
            'CAST("category" AS VARCHAR) = ? AS "__label_collision", '
            'row_number() OVER (ORDER BY "value" DESC NULLS LAST) AS "__rank" '
            "FROM aggregated), folded AS "
            '(SELECT CASE WHEN "__rank" <= 3 THEN CAST("category" AS VARCHAR) '
            'ELSE ? END AS "category", "__rank" > 3 AS "__is_others", '
            'min("__rank") AS "__order", '
            'bool_or("__label_collision") AS "__label_collision", '
            'sum("value") AS "value" FROM ranked GROUP BY 1, 2) '
            'SELECT "category", "value", "__label_collision" FROM folded '
            'ORDER BY "__is_others" ASC, "__order" ASC',
            compiled.sql,
        )
        self.assertEqual(
            [10, 2, 0, PARQUET_PATH, "A", "Other bucket", "Other bucket"],
            compiled.parameters,
        )
        self.assertEqual(
            [
                ("category", "category", "STRING"),
                ("value", "Adjusted", "NUMBER"),
            ],
            [(column.key, column.label, column.type) for column in compiled.columns],
        )

    def test_running_total_comparison_keeps_cte_and_parameter_order(self):
        request = AnalyticsQueryRequest.model_validate(
            {
                "schemaVersion": 1,
                "requestId": "compiler-line",
                "source": SOURCE,
                "chartType": "LINE",
                "encoding": {
                    "category": {"column": "event_date", "timeGrain": "MONTH"},
                    "value": {"column": "value", "aggregation": "SUM"},
                    "series": {"column": "series"},
                },
                "filters": [{"column": "active", "operator": "EQ", "value": True}],
                "limit": 25,
                "options": {"valueTransform": "RUNNING_TOTAL"},
                "comparison": {
                    "enabled": True,
                    "mode": "PREVIOUS_PERIOD",
                    "periodUnit": "MONTH",
                    "offset": -1,
                },
            }
        )

        compiled = AnalyticsCompiler(
            request,
            PARQUET_PATH,
            [
                ("event_date", "DATE"),
                ("value", "DOUBLE"),
                ("series", "VARCHAR"),
                ("active", "BOOLEAN"),
            ],
        ).compile()

        self.assertEqual(
            "WITH aggregated AS (SELECT date_trunc('month', \"event_date\") "
            'AS "category", sum("value") AS "value", "series" AS "series" '
            'FROM read_parquet(?) AS "__src" WHERE "active" = ? '
            'AND "event_date" IS NOT NULL AND "series" IS NOT NULL GROUP BY 1, 3), '
            'transformed AS (SELECT "category", sum("value") OVER '
            '(PARTITION BY "series" ORDER BY "category" ASC '
            'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS "value", '
            '"series" FROM aggregated), compared AS '
            '(SELECT "category", "value", "series", lag("value", 1) OVER '
            '(PARTITION BY "series" ORDER BY "category" ASC) AS "previousValue" '
            'FROM transformed) SELECT "category", "value", "series", '
            '"previousValue", ("value" - "previousValue") AS "change", '
            '100.0 * ("value" - "previousValue") / '
            'NULLIF(abs("previousValue"), 0) AS "changeRate" FROM compared '
            'ORDER BY "category" ASC NULLS LAST, "series" ASC NULLS LAST LIMIT 26',
            compiled.sql,
        )
        self.assertEqual([PARQUET_PATH, True], compiled.parameters)
        self.assertEqual(
            (
                "PREVIOUS_PERIOD uses the previous available bucket; "
                "a missing calendar bucket is not synthesized.",
            ),
            compiled.warnings,
        )


if __name__ == "__main__":
    unittest.main()
