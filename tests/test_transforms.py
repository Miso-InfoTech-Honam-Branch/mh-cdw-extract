from __future__ import annotations

import unittest
import duckdb

from cdw_extract.transforms.compiler import canonical_hash, compile_pipeline
from cdw_extract.transforms.runtime import _resolve_automatic_pivot_values
from cdw_extract.transforms.schema import ColumnSchema, common_type, normalize_type, numeric_result_type


SOURCE = [
    ColumnSchema("src:department", "department", "진료과", "STRING", False, ("src:department",)),
    ColumnSchema("src:amount", "amount", "진료비", "DECIMAL(12,2)", True, ("src:amount",)),
    ColumnSchema("src:patient", "patient_id", "환자번호", "STRING", False, ("src:patient",)),
]


class TransformCompilerTest(unittest.TestCase):
    def test_empty_pivot_values_are_discovered_from_all_rows(self):
        pipeline={"pipelineVersion":1,"steps":[
            {"stepId":"pivot","type":"PIVOT","config":{"groupColumnIds":[],"pivotColumnId":"src:department","values":[],"aggregates":[{"aggregateId":"amount","op":"SUM","columnId":"src:amount","label":"합계"}]}},
            {"stepId":"output","type":"OUTPUT","config":{}},
        ]}
        connection=duckdb.connect()
        resolved,declarative_hash=_resolve_automatic_pivot_values(
            connection,
            "(VALUES ('외래', 10, 'p1'), ('입원', 20, 'p2'), ('외래', 30, 'p3')) AS t(department, amount, patient_id)",
            SOURCE,
            pipeline,
        )
        values=resolved["steps"][0]["config"]["values"]
        self.assertEqual(["외래","입원"],sorted(item["value"] for item in values))
        self.assertEqual(2,len({item["valueId"] for item in values}))
        self.assertEqual(declarative_hash,canonical_hash(pipeline))

    def test_type_normalization_and_numeric_promotion(self):
        self.assertEqual("STRING", normalize_type("varchar"))
        self.assertEqual("TIMESTAMP_TZ", normalize_type("timestamp with time zone"))
        self.assertEqual("DECIMAL(21,2)", common_type(["INT64", "DECIMAL(12,2)"]))
        self.assertEqual("INT64", common_type(["NULL", "INT64"]))
        self.assertEqual(("DECIMAL(13,2)", None), numeric_result_type("DECIMAL(12,2)", "DECIMAL(8,1)", "ADD"))
        self.assertEqual(("DECIMAL(38,19)", "DECIMAL_SCALE_REDUCED"), numeric_result_type("INT64", "INT64", "DIVIDE"))

    def test_negative_filter_includes_null_and_values_are_parameters(self):
        pipeline={"pipelineVersion":1,"steps":[
            {"stepId":"filter","type":"FILTER","config":{"conditions":[{"columnId":"src:amount","operator":"NE","values":[100]}]}},
            {"stepId":"output","type":"OUTPUT","config":{}},
        ]}
        compiled=compile_pipeline("SELECT * FROM source",SOURCE,pipeline)
        self.assertIn('"amount" <> CAST(? AS DECIMAL(12,2)) OR "amount" IS NULL',compiled.sql)
        self.assertEqual([100],compiled.parameters)

    def test_filter_honors_each_condition_connector_in_order(self):
        pipeline={"pipelineVersion":1,"steps":[
            {"stepId":"filter","type":"FILTER","config":{"logic":"AND","conditions":[
                {"columnId":"src:department","operator":"EQ","values":["A"]},
                {"columnId":"src:amount","operator":"GTE","values":[100],"logic":"OR"},
                {"columnId":"src:patient","operator":"IS_NOT_NULL","values":[],"logic":"AND"},
            ]}},
            {"stepId":"output","type":"OUTPUT","config":{}},
        ]}
        compiled=compile_pipeline("SELECT * FROM source",SOURCE,pipeline)
        self.assertIn('(("department" = CAST(? AS VARCHAR) OR "amount" >= CAST(? AS DECIMAL(12,2))) AND "patient_id" IS NOT NULL)',compiled.sql)
        self.assertEqual(["A",100],compiled.parameters)

    def test_replace_value_can_replace_the_whole_value_when_text_is_contained(self):
        compiled = compile_pipeline(
            "SELECT 'A01DAST' AS department, 1 AS amount, 'p1' AS patient_id",
            SOURCE,
            {"steps": [
                {"stepId": "replace", "type": "REPLACE_VALUE", "config": {
                    "columnId": "src:department", "matchMode": "CONTAINS",
                    "mappings": [{"from": "A01", "to": "분류 A"}],
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )
        row = duckdb.connect().execute(compiled.sql, compiled.parameters).fetchone()
        self.assertEqual("A01DAST", row[0])
        self.assertEqual("분류 A", row[-1])

    def test_key_based_deduplicate_keeps_one_row_without_an_order_choice(self):
        compiled = compile_pipeline(
            "SELECT * FROM (VALUES ('A', 10, 'first'), ('A', 20, 'second')) t(department, amount, patient_id)",
            SOURCE,
            {"steps": [
                {"stepId": "deduplicate", "type": "DEDUPLICATE", "config": {"keyColumnIds": ["src:department"]}},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )
        rows = duckdb.connect().execute(compiled.sql, compiled.parameters).fetchall()
        self.assertEqual(1, len(rows))

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

    def test_split_column_casts_numeric_input_to_text_without_changing_source(self):
        compiled = compile_pipeline(
            "SELECT '진료' AS department, 20140522 AS amount, 'p1' AS patient_id",
            SOURCE,
            {"steps": [
                {"stepId": "year", "type": "SPLIT_COLUMN", "config": {
                    "inputColumnId": "src:amount", "mode": "SLICE", "startAt": 1, "length": 4,
                    "outputs": [{"outputId": "year", "label": "진료연도"}],
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )
        row = duckdb.connect().execute(compiled.sql, compiled.parameters).fetchone()
        self.assertEqual(20140522, row[1])
        self.assertEqual("2014", row[-1])

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
