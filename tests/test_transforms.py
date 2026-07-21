from __future__ import annotations

import unittest
import duckdb

from cdw_extract.transforms.compiler import compile_pipeline
from cdw_extract.transforms.schema import ColumnSchema, common_type, normalize_type


SOURCE = [
    ColumnSchema("src:department", "department", "진료과", "STRING", False, ("src:department",)),
    ColumnSchema("src:amount", "amount", "진료비", "DECIMAL(12,2)", True, ("src:amount",)),
    ColumnSchema("src:patient", "patient_id", "환자번호", "STRING", False, ("src:patient",)),
]


class TransformCompilerTest(unittest.TestCase):
    def test_type_normalization_and_numeric_promotion(self):
        self.assertEqual("STRING", normalize_type("varchar"))
        self.assertEqual("TIMESTAMP_TZ", normalize_type("timestamp with time zone"))
        self.assertEqual("DECIMAL(21,2)", common_type(["INT64", "DECIMAL(12,2)"]))
        self.assertEqual("INT64", common_type(["NULL", "INT64"]))

    def test_negative_filter_includes_null_and_values_are_parameters(self):
        pipeline={"pipelineVersion":1,"steps":[
            {"stepId":"filter","type":"FILTER","config":{"conditions":[{"columnId":"src:amount","operator":"NE","values":[100]}]}},
            {"stepId":"output","type":"OUTPUT","config":{}},
        ]}
        compiled=compile_pipeline("SELECT * FROM source",SOURCE,pipeline)
        self.assertIn('"amount" <> CAST(? AS DECIMAL(12,2)) OR "amount" IS NULL',compiled.sql)
        self.assertEqual([100],compiled.parameters)

    def test_group_and_fixed_pivot_have_stable_schema(self):
        pipeline={"pipelineVersion":1,"steps":[
            {"stepId":"pivot","type":"PIVOT","config":{"groupColumnIds":["src:department"],"pivotColumnId":"src:department","values":[{"valueId":"internal","value":"내과","label":"내과"}],"aggregates":[{"aggregateId":"patients","op":"COUNT_DISTINCT","columnId":"src:patient","label":"환자수"}]}},
            {"stepId":"output","type":"OUTPUT","config":{}},
        ]}
        compiled=compile_pipeline("SELECT * FROM source",SOURCE,pipeline)
        self.assertEqual(2,len(compiled.output_schema))
        self.assertTrue(compiled.output_schema[1].physical_name.startswith("p_"))
        self.assertEqual(["내과"],compiled.parameters)

    def test_output_is_required(self):
        with self.assertRaisesRegex(ValueError,"final active step"):
            compile_pipeline("SELECT * FROM source",SOURCE,{"steps":[]})

    def test_cast_parses_compact_date_and_timestamp_formats(self):
        cases = [
            ("20140522", "DATE", "YYYYMMDD", "2014-05-22"),
            ("20140522153045", "TIMESTAMP", "YYYYMMDDHH24MISS", "2014-05-22 15:30:45"),
        ]
        for raw_value, target_type, input_format, expected in cases:
            with self.subTest(target_type=target_type, input_format=input_format):
                compiled = compile_pipeline(
                    f"SELECT '{raw_value}' AS department, 1 AS amount, 'p1' AS patient_id",
                    SOURCE,
                    {"steps": [
                        {"stepId": "cast", "type": "CAST", "config": {
                            "columnId": "src:department", "targetType": target_type,
                            "inputFormat": input_format, "onError": "NULL"
                        }},
                        {"stepId": "output", "type": "OUTPUT", "config": {}},
                    ]},
                )
                value = duckdb.connect().execute(compiled.sql, compiled.parameters).fetchone()[-1]
                self.assertEqual(expected, str(value))

    def test_cast_rejects_unknown_date_format(self):
        with self.assertRaisesRegex(ValueError, "unsupported CAST inputFormat"):
            compile_pipeline("SELECT * FROM source", SOURCE, {"steps": [
                {"stepId": "cast", "type": "CAST", "config": {
                    "columnId": "src:department", "targetType": "DATE", "inputFormat": "FREE_TEXT"
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]})

    def test_split_column_supports_fixed_lengths(self):
        compiled = compile_pipeline(
            "SELECT '20140522' AS department, 1 AS amount, 'p1' AS patient_id",
            SOURCE,
            {"steps": [
                {"stepId": "split", "type": "SPLIT_COLUMN", "config": {
                    "inputColumnId": "src:department", "mode": "FIXED_LENGTH", "lengths": [4, 2, 2],
                    "outputs": [
                        {"outputId": "year", "label": "진료연도"},
                        {"outputId": "month", "label": "진료월"},
                        {"outputId": "day", "label": "진료일"},
                    ],
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )
        row = duckdb.connect().execute(compiled.sql, compiled.parameters).fetchone()
        self.assertEqual(("2014", "05", "22"), row[-3:])

    def test_split_column_rejects_mismatched_fixed_lengths(self):
        with self.assertRaisesRegex(ValueError, "fixed lengths must match outputs"):
            compile_pipeline("SELECT * FROM source", SOURCE, {"steps": [
                {"stepId": "split", "type": "SPLIT_COLUMN", "config": {
                    "inputColumnId": "src:department", "mode": "FIXED_LENGTH", "lengths": [4, 2],
                    "outputs": [{"outputId": "year"}, {"outputId": "month"}, {"outputId": "day"}],
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]})

    def test_split_column_can_extract_one_slice_per_step(self):
        compiled = compile_pipeline(
            "SELECT '20140522' AS department, 1 AS amount, 'p1' AS patient_id",
            SOURCE,
            {"steps": [
                {"stepId": "year", "type": "SPLIT_COLUMN", "config": {"inputColumnId": "src:department", "mode": "SLICE", "startAt": 1, "length": 4, "outputs": [{"outputId": "year", "label": "진료연도"}]}},
                {"stepId": "month", "type": "SPLIT_COLUMN", "config": {"inputColumnId": "src:department", "mode": "SLICE", "startAt": 5, "length": 2, "outputs": [{"outputId": "month", "label": "진료월"}]}},
                {"stepId": "day", "type": "SPLIT_COLUMN", "config": {"inputColumnId": "src:department", "mode": "SLICE", "startAt": 7, "length": 2, "outputs": [{"outputId": "day", "label": "진료일"}]}},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )
        row = duckdb.connect().execute(compiled.sql, compiled.parameters).fetchone()
        self.assertEqual(("2014", "05", "22"), row[-3:])

    def test_each_supported_transform_compiles_to_executable_duckdb_sql(self):
        source = "SELECT * FROM (VALUES (' A ',10,'x'),(NULL,20,'y'),('B',10,'x')) t(department,amount,patient_id)"
        cases = {
            "FILTER": {"conditions": [{"columnId": "src:amount", "operator": "GTE", "values": [10]}]},
            "CAST": {"columnId": "src:amount", "targetType": "STRING"},
            "FILL_NULL": {"columnId": "src:department", "value": "없음"},
            "TRIM": {"columnId": "src:department", "mode": "BOTH"},
            "CHANGE_CASE": {"columnId": "src:department", "mode": "LOWER"},
            "REPLACE_VALUE": {"columnId": "src:department", "mappings": [{"from": "B", "to": "C"}]},
            "CALCULATE": {"expression": {"op": "ADD", "args": [{"op": "COLUMN", "columnId": "src:amount"}, {"op": "LITERAL", "value": 2, "dataType": "INT64"}]}},
            "CODE_LOOKUP": {"columnId": "src:patient", "values": [{"code": "x", "name": "엑스"}]},
            "SPLIT_COLUMN": {"inputColumnId": "src:department", "delimiter": " ", "outputs": [{"outputId": "a"}, {"outputId": "b"}]},
            "MERGE_COLUMNS": {"inputColumnIds": ["src:department", "src:patient"], "delimiter": "-", "output": {"outputId": "merged"}},
            "ROW_NUMBER": {"orderBy": [{"columnId": "src:amount"}], "output": {"outputId": "rn"}},
            "GROUP_AGGREGATE": {"groups": [{"columnId": "src:patient"}], "aggregates": [{"aggregateId": "sum", "op": "SUM", "columnId": "src:amount"}]},
            "UNPIVOT": {"idColumnIds": ["src:patient"], "valueColumns": [{"columnId": "src:amount", "labelValue": "금액"}]},
            "PIVOT": {"groupColumnIds": ["src:patient"], "pivotColumnId": "src:department", "values": [{"valueId": "b", "value": "B", "label": "B"}], "aggregates": [{"aggregateId": "sum", "op": "SUM", "columnId": "src:amount", "label": "합계"}]},
            "SORT": {"items": [{"columnId": "src:amount", "direction": "DESC"}]},
            "DEDUPLICATE": {"keyColumnIds": ["src:amount"], "orderBy": [{"columnId": "src:patient"}]},
        }
        for step_type, config in cases.items():
            with self.subTest(step_type=step_type):
                compiled = compile_pipeline(source, SOURCE, {"steps": [
                    {"stepId": "work", "type": step_type, "config": config},
                    {"stepId": "output", "type": "OUTPUT", "config": {}},
                ]})
                duckdb.connect().execute(compiled.sql, compiled.parameters).fetchall()


if __name__ == "__main__":
    unittest.main()
