import unittest
from datetime import date
from decimal import Decimal

import duckdb

from cdw_extract.transforms.compiler import compile_pipeline
from cdw_extract.transforms.schema import ColumnSchema, normalize_type


SOURCE_SCHEMA = [
    ColumnSchema(
        "src:department",
        "department",
        "Department",
        "STRING",
        False,
        ("src:department",),
    ),
    ColumnSchema(
        "src:amount",
        "amount",
        "Amount",
        "DECIMAL(12,2)",
        True,
        ("src:amount",),
    ),
    ColumnSchema(
        "src:patient",
        "patient_id",
        "Patient",
        "STRING",
        False,
        ("src:patient",),
    ),
]

SOURCE_SQL = """
SELECT *
FROM (
    VALUES
        ('outpatient', CAST(10 AS DECIMAL(12,2)), 'p1'),
        ('inpatient', CAST(20 AS DECIMAL(12,2)), 'p1'),
        ('outpatient', CAST(5 AS DECIMAL(12,2)), 'p2')
) source(department, amount, patient_id)
"""


def execute(pipeline, source_sql=SOURCE_SQL, source_schema=SOURCE_SCHEMA):
    compiled = compile_pipeline(source_sql, source_schema, pipeline)
    rows = duckdb.connect().execute(compiled.sql, compiled.parameters).fetchall()
    return compiled, rows


class WindowAggregateTest(unittest.TestCase):
    def test_partitioned_sum_appends_a_column_without_collapsing_rows(self):
        pipeline = {
            "steps": [
                {
                    "stepId": "patient-total",
                    "type": "WINDOW_AGGREGATE",
                    "config": {
                        "partitionBy": ["src:patient"],
                        "aggregate": {
                            "aggregateId": "total",
                            "op": "SUM",
                            "columnId": "src:amount",
                            "label": "Patient total",
                        },
                    },
                },
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]
        }

        compiled, rows = execute(pipeline)

        self.assertEqual(3, len(rows))
        self.assertEqual(
            [
                ("inpatient", Decimal("20.00"), "p1", Decimal("30.00")),
                ("outpatient", Decimal("5.00"), "p2", Decimal("5.00")),
                ("outpatient", Decimal("10.00"), "p1", Decimal("30.00")),
            ],
            sorted(rows),
        )
        self.assertEqual(
            [
                "src:department",
                "src:amount",
                "src:patient",
                "out:patient-total:total",
            ],
            [column.column_id for column in compiled.output_schema],
        )
        self.assertEqual("Patient total", compiled.output_schema[-1].label)

    def test_sequential_window_steps_keep_detail_and_add_each_result(self):
        pipeline = {
            "steps": [
                {
                    "stepId": "patient-average",
                    "type": "WINDOW_AGGREGATE",
                    "config": {
                        "partitionBy": ["src:patient"],
                        "aggregate": {
                            "aggregateId": "average",
                            "op": "AVG",
                            "columnId": "src:amount",
                        },
                    },
                },
                {
                    "stepId": "patient-count",
                    "type": "WINDOW_AGGREGATE",
                    "config": {
                        "partitionBy": ["src:patient"],
                        "aggregate": {
                            "aggregateId": "count",
                            "op": "COUNT_ROWS",
                        },
                    },
                },
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]
        }

        compiled, rows = execute(pipeline)

        self.assertEqual(3, len(rows))
        by_amount = {row[1]: row for row in rows}
        self.assertEqual(Decimal("15.000000"), by_amount[Decimal("10.00")][-2])
        self.assertEqual(2, by_amount[Decimal("10.00")][-1])
        self.assertEqual(Decimal("5.000000"), by_amount[Decimal("5.00")][-2])
        self.assertEqual(1, by_amount[Decimal("5.00")][-1])
        self.assertEqual(
            ["out:patient-average:average", "out:patient-count:count"],
            [column.column_id for column in compiled.output_schema[-2:]],
        )

    def test_every_supported_window_aggregate_executes(self):
        cases = {
            "COUNT_ROWS": (None, 3),
            "COUNT": ("src:amount", 3),
            "COUNT_DISTINCT": ("src:amount", 3),
            "SUM": ("src:amount", Decimal("35.00")),
            "AVG": ("src:amount", Decimal("11.666667")),
            "MIN": ("src:amount", Decimal("5.00")),
            "MAX": ("src:amount", Decimal("20.00")),
            "MEDIAN": ("src:amount", Decimal("10.00")),
        }
        for op, (column_id, expected) in cases.items():
            with self.subTest(op=op):
                aggregate = {"aggregateId": "value", "op": op}
                if column_id:
                    aggregate["columnId"] = column_id
                pipeline = {
                    "steps": [
                        {
                            "stepId": "window",
                            "type": "WINDOW_AGGREGATE",
                            "config": {
                                "partitionBy": [],
                                "aggregate": aggregate,
                            },
                        },
                        {"stepId": "output", "type": "OUTPUT", "config": {}},
                    ]
                }
                compiled = compile_pipeline(SOURCE_SQL, SOURCE_SCHEMA, pipeline)
                result = duckdb.connect().execute(
                    compiled.sql, compiled.parameters
                )
                rows = result.fetchall()
                self.assertEqual(expected, rows[0][-1])
                self.assertEqual(
                    compiled.output_schema[-1].data_type,
                    normalize_type(str(result.description[-1][1])),
                )

    def test_integer_sum_and_median_have_truthful_decimal_types(self):
        source_schema = [
            ColumnSchema("src:group", "group_name", "Group", "STRING", False),
            ColumnSchema("src:value", "value", "Value", "INT64", False),
        ]
        source_sql = """
        SELECT * FROM (VALUES ('a', 1), ('a', 2), ('b', 4)) source(group_name, value)
        """
        cases = {
            "SUM": ("DECIMAL(38,0)", Decimal("3")),
            "MEDIAN": ("DECIMAL(38,6)", Decimal("1.500000")),
        }
        for op, (expected_type, expected_value) in cases.items():
            with self.subTest(op=op):
                pipeline = {
                    "steps": [
                        {
                            "stepId": "window",
                            "type": "WINDOW_AGGREGATE",
                            "config": {
                                "partitionBy": ["src:group"],
                                "aggregate": {
                                    "aggregateId": "value",
                                    "op": op,
                                    "columnId": "src:value",
                                },
                            },
                        },
                        {"stepId": "output", "type": "OUTPUT", "config": {}},
                    ]
                }
                compiled = compile_pipeline(source_sql, source_schema, pipeline)
                result = duckdb.connect().execute(
                    compiled.sql, compiled.parameters
                )
                rows = result.fetchall()
                self.assertEqual(expected_value, rows[0][-1])
                self.assertEqual(expected_type, compiled.output_schema[-1].data_type)
                self.assertEqual(
                    expected_type,
                    normalize_type(str(result.description[-1][1])),
                )

    def test_numeric_window_aggregate_rejects_a_text_column(self):
        pipeline = {
            "steps": [
                {
                    "stepId": "window",
                    "type": "WINDOW_AGGREGATE",
                    "config": {
                        "partitionBy": [],
                        "aggregate": {
                            "aggregateId": "bad",
                            "op": "SUM",
                            "columnId": "src:department",
                        },
                    },
                },
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]
        }

        with self.assertRaisesRegex(ValueError, "SUM requires numeric column"):
            compile_pipeline(SOURCE_SQL, SOURCE_SCHEMA, pipeline)


class CalculateExpressionTest(unittest.TestCase):
    def test_decimal_literal_arithmetic_has_a_truthful_decimal_type(self):
        pipeline = {
            "steps": [
                {
                    "stepId": "copay",
                    "type": "CALCULATE",
                    "config": {
                        "expression": {
                            "op": "MULTIPLY",
                            "args": [
                                {"op": "COLUMN", "columnId": "src:amount"},
                                {
                                    "op": "LITERAL",
                                    "value": 0.2,
                                    "dataType": "DECIMAL(18,6)",
                                },
                            ],
                        },
                        "outputId": "copay",
                        "targetType": "DECIMAL(31,8)",
                    },
                },
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]
        }

        compiled = compile_pipeline(SOURCE_SQL, SOURCE_SCHEMA, pipeline)
        result = duckdb.connect().execute(compiled.sql, compiled.parameters)
        rows = result.fetchall()

        self.assertEqual([0.2], compiled.parameters)
        self.assertIn("CAST(? AS DECIMAL(18,6))", compiled.sql)
        self.assertEqual(
            [
                Decimal("2.00000000"),
                Decimal("4.00000000"),
                Decimal("1.00000000"),
            ],
            [row[-1] for row in rows],
        )
        self.assertTrue(all(isinstance(row[-1], Decimal) for row in rows))
        self.assertEqual("DECIMAL(31,8)", compiled.output_schema[-1].data_type)
        self.assertEqual("DECIMAL(31,8)", str(result.description[-1][1]))

    def test_explicit_calculation_target_casts_column_arithmetic(self):
        cases = {
            "MULTIPLY": "DECIMAL(25,4)",
            "DIVIDE": "DECIMAL(27,15)",
        }
        for op, target_type in cases.items():
            with self.subTest(op=op):
                pipeline = {
                    "steps": [
                        {
                            "stepId": "calculated",
                            "type": "CALCULATE",
                            "config": {
                                "expression": {
                                    "op": op,
                                    "args": [
                                        {
                                            "op": "COLUMN",
                                            "columnId": "src:amount",
                                        },
                                        {
                                            "op": "COLUMN",
                                            "columnId": "src:amount",
                                        },
                                    ],
                                },
                                "outputId": "value",
                                "targetType": target_type,
                            },
                        },
                        {"stepId": "output", "type": "OUTPUT", "config": {}},
                    ]
                }
                compiled = compile_pipeline(SOURCE_SQL, SOURCE_SCHEMA, pipeline)
                result = duckdb.connect().execute(
                    compiled.sql, compiled.parameters
                )
                rows = result.fetchall()
                self.assertIsInstance(rows[0][-1], Decimal)
                self.assertEqual(target_type, compiled.output_schema[-1].data_type)
                self.assertEqual(target_type, str(result.description[-1][1]))

    def test_null_literal_can_use_an_explicit_output_type(self):
        pipeline = {
            "steps": [
                {
                    "stepId": "empty",
                    "type": "CALCULATE",
                    "config": {
                        "expression": {
                            "op": "LITERAL",
                            "value": None,
                            "dataType": "NULL",
                        },
                        "outputId": "empty",
                        "targetType": "STRING",
                    },
                },
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]
        }

        compiled = compile_pipeline(SOURCE_SQL, SOURCE_SCHEMA, pipeline)
        result = duckdb.connect().execute(compiled.sql, compiled.parameters)
        rows = result.fetchall()

        self.assertEqual([None], compiled.parameters)
        self.assertTrue(all(row[-1] is None for row in rows))
        self.assertEqual("STRING", compiled.output_schema[-1].data_type)
        self.assertEqual("VARCHAR", str(result.description[-1][1]))

    def test_case_supports_comparisons_boolean_logic_and_parameter_order(self):
        pipeline = {
            "steps": [
                {
                    "stepId": "classify",
                    "type": "CALCULATE",
                    "config": {
                        "expression": {
                            "op": "CASE",
                            "branches": [
                                {
                                    "when": {
                                        "op": "AND",
                                        "args": [
                                            {
                                                "op": "GTE",
                                                "args": [
                                                    {
                                                        "op": "COLUMN",
                                                        "columnId": "src:amount",
                                                    },
                                                    {
                                                        "op": "LITERAL",
                                                        "value": 10,
                                                        "dataType": "INT64",
                                                    },
                                                ],
                                            },
                                            {
                                                "op": "NOT",
                                                "args": [
                                                    {
                                                        "op": "EQ",
                                                        "args": [
                                                            {
                                                                "op": "COLUMN",
                                                                "columnId": "src:patient",
                                                            },
                                                            {
                                                                "op": "LITERAL",
                                                                "value": "p2",
                                                                "dataType": "STRING",
                                                            },
                                                        ],
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                    "then": {
                                        "op": "LITERAL",
                                        "value": "high",
                                        "dataType": "STRING",
                                    },
                                }
                            ],
                            "else": {
                                "op": "LITERAL",
                                "value": "normal",
                                "dataType": "STRING",
                            },
                        },
                        "outputId": "category",
                    },
                },
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]
        }

        compiled, rows = execute(pipeline)

        self.assertEqual([10, "p2", "high", "normal"], compiled.parameters)
        self.assertEqual(["high", "high", "normal"], [row[-1] for row in rows])
        self.assertEqual("STRING", compiled.output_schema[-1].data_type)

    def test_date_diff_supports_day_month_and_year(self):
        source_schema = [
            ColumnSchema("src:start", "start_date", "Start", "DATE", False),
            ColumnSchema("src:end", "end_date", "End", "DATE", False),
        ]
        steps = []
        for unit in ("DAY", "MONTH", "YEAR"):
            steps.append(
                {
                    "stepId": f"diff-{unit.lower()}",
                    "type": "CALCULATE",
                    "config": {
                        "expression": {
                            "op": "DATE_DIFF",
                            "unit": unit,
                            "args": [
                                {"op": "COLUMN", "columnId": "src:start"},
                                {"op": "COLUMN", "columnId": "src:end"},
                            ],
                        },
                        "outputId": unit.lower(),
                    },
                }
            )
        steps.append({"stepId": "output", "type": "OUTPUT", "config": {}})

        compiled, rows = execute(
            {"steps": steps},
            "SELECT DATE '2020-01-01' start_date, DATE '2024-01-01' end_date",
            source_schema,
        )

        self.assertEqual(
            (date(2020, 1, 1), date(2024, 1, 1), 1461, 48, 4),
            rows[0],
        )
        self.assertEqual(["INT64", "INT64", "INT64"], [
            column.data_type for column in compiled.output_schema[-3:]
        ])


if __name__ == "__main__":
    unittest.main()
