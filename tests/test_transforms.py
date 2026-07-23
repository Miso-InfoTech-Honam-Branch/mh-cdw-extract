from __future__ import annotations

import json
import unittest
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

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
    def test_resolved_pipeline_hash_matches_the_boot_contract_fixture(self):
        fixture = json.loads(
            (Path(__file__).parent / "contracts" / "resolved-pipeline-hash-v1.json")
            .read_text(encoding="utf-8")
        )
        self.assertEqual(
            fixture["pipelineHash"],
            canonical_hash(fixture["resolvedPipeline"]),
        )

    def test_empty_pivot_values_are_discovered_from_all_rows(self):
        pipeline={"pipelineVersion":1,"steps":[
            {"stepId":"pivot","type":"PIVOT","config":{"groupColumnIds":[],"pivotColumnId":"src:department","values":[],"aggregates":[{"aggregateId":"amount","op":"SUM","columnId":"src:amount","label":"합계"}]}},
            {"stepId":"output","type":"OUTPUT","config":{}},
        ]}
        connection=duckdb.connect()
        resolved,resolved_hash=_resolve_automatic_pivot_values(
            connection,
            "(VALUES ('외래', 10, 'p1'), ('입원', 20, 'p2'), ('외래', 30, 'p3')) AS t(department, amount, patient_id)",
            SOURCE,
            pipeline,
        )
        values=resolved["steps"][0]["config"]["values"]
        self.assertEqual(["외래","입원"],sorted(item["value"] for item in values))
        self.assertEqual(2,len({item["valueId"] for item in values}))
        self.assertEqual(resolved_hash,canonical_hash(resolved))
        self.assertNotEqual(resolved_hash,canonical_hash(pipeline))

    def test_automatic_pivot_uses_code_names_as_column_labels(self):
        source_schema=[
            ColumnSchema("src:item", "항목코드", "항목코드", "STRING", False, ("src:item",)),
            ColumnSchema("src:name:item", "항목코드__code_name", "항목코드(코드명)", "STRING", True, ("src:name:item",)),
            ColumnSchema("src:value", "측정값", "측정값", "DECIMAL(12,2)", True, ("src:value",)),
        ]
        pipeline={"pipelineVersion":1,"steps":[
            {"stepId":"pivot","type":"PIVOT","config":{"groupColumnIds":[],"pivotColumnId":"src:item","values":[],"aggregates":[{"aggregateId":"value","op":"MAX","columnId":"src:value","label":"최댓값"}]}},
            {"stepId":"output","type":"OUTPUT","config":{}},
        ]}
        resolved,_=_resolve_automatic_pivot_values(
            duckdb.connect(),
            "(VALUES ('11', '신장', 170), ('12', '체중', 60)) AS t(항목코드, 항목코드__code_name, 측정값)",
            source_schema,
            pipeline,
            [{"name":"item_cd","alias":"항목코드"},{"name":"항목코드__code_name"}],
            [{"sourceColumn":"item_cd","outputColumn":"항목코드__code_name"}],
        )
        self.assertEqual({"11":"신장","12":"체중"},{item["value"]:item["label"] for item in resolved["steps"][0]["config"]["values"]})
        compiled=compile_pipeline(
            "SELECT * FROM (VALUES ('11', '신장', 170), ('12', '체중', 60)) AS t(항목코드, 항목코드__code_name, 측정값)",
            source_schema,
            resolved,
        )
        self.assertEqual(["신장","체중"],[column.label for column in compiled.output_schema])

    def test_automatic_pivot_normalizes_non_json_duckdb_scalars(self):
        cases = [
            ("DECIMAL(10,2)", "CAST(1.20 AS DECIMAL(10,2))", Decimal("1.20"), "1.20"),
            ("DOUBLE", "CAST(0.1 AS DOUBLE)", 0.1, "0.1"),
            ("DATE", "DATE '2024-01-02'", date(2024, 1, 2), "2024-01-02"),
            (
                "TIMESTAMP",
                "TIMESTAMP '2024-01-02 03:04:05'",
                datetime(2024, 1, 2, 3, 4, 5),
                "2024-01-02T03:04:05",
            ),
        ]
        for data_type, expression, duckdb_value, expected_wire_value in cases:
            with self.subTest(data_type=data_type):
                schema = [
                    ColumnSchema(
                        "src:pivot_value",
                        "pivot_value",
                        "Pivot value",
                        data_type,
                        False,
                        ("src:pivot_value",),
                    )
                ]
                pipeline = {"pipelineVersion": 1, "steps": [
                    {
                        "stepId": "pivot",
                        "type": "PIVOT",
                        "config": {
                            "groupColumnIds": [],
                            "pivotColumnId": "src:pivot_value",
                            "values": [],
                            "aggregates": [{"aggregateId": "rows", "op": "COUNT_ROWS"}],
                        },
                    },
                    {"stepId": "output", "type": "OUTPUT", "config": {}},
                ]}
                source = f"(SELECT {expression} AS pivot_value) AS source"
                connection = duckdb.connect()
                try:
                    self.assertEqual(
                        duckdb_value,
                        connection.execute(f"SELECT pivot_value FROM {source}").fetchone()[0],
                    )
                    resolved, resolved_hash = _resolve_automatic_pivot_values(
                        connection, source, schema, pipeline
                    )
                    self.assertEqual(
                        expected_wire_value,
                        resolved["steps"][0]["config"]["values"][0]["value"],
                    )
                    json.dumps(resolved, ensure_ascii=False)
                    self.assertEqual(canonical_hash(resolved), resolved_hash)
                    compiled = compile_pipeline(f"SELECT * FROM {source}", schema, resolved)
                    self.assertEqual(
                        [(1,)],
                        connection.execute(compiled.sql, compiled.parameters).fetchall(),
                    )
                finally:
                    connection.close()

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
        self.assertEqual("분류 A", row[0])
        self.assertEqual(3, len(row))
        self.assertEqual("out:replace:replaced", compiled.output_schema[0].column_id)

    def test_in_place_transforms_replace_a_middle_column(self):
        source_schema = [
            ColumnSchema("src:left", "left_value", "왼쪽", "STRING", False, ("src:left",)),
            ColumnSchema("src:middle", "middle_value", "가운데", "STRING", True, ("src:middle",)),
            ColumnSchema("src:right", "right_value", "오른쪽", "STRING", False, ("src:right",)),
        ]
        cases = [
            (
                "FILL_NULL",
                "SELECT 'L' AS left_value, NULL::VARCHAR AS middle_value, 'R' AS right_value",
                {"columnId": "src:middle", "value": "채움", "outputId": "value"},
                "채움",
            ),
            (
                "TRIM",
                "SELECT 'L' AS left_value, '  값  ' AS middle_value, 'R' AS right_value",
                {"columnId": "src:middle", "mode": "BOTH", "outputId": "value"},
                "값",
            ),
            (
                "CHANGE_CASE",
                "SELECT 'L' AS left_value, 'abc' AS middle_value, 'R' AS right_value",
                {"columnId": "src:middle", "mode": "UPPER", "outputId": "value"},
                "ABC",
            ),
            (
                "REPLACE_VALUE",
                "SELECT 'L' AS left_value, '전남' AS middle_value, 'R' AS right_value",
                {
                    "columnId": "src:middle",
                    "mappings": [{"from": "전남", "to": "그럴"}],
                    "outputId": "value",
                },
                "그럴",
            ),
            (
                "CODE_LOOKUP",
                "SELECT 'L' AS left_value, '01' AS middle_value, 'R' AS right_value",
                {
                    "columnId": "src:middle",
                    "values": [{"code": "01", "name": "전남"}],
                    "outputId": "value",
                },
                "전남",
            ),
        ]

        for step_type, source_sql, config, expected in cases:
            with self.subTest(step_type=step_type):
                compiled = compile_pipeline(
                    source_sql,
                    source_schema,
                    {"steps": [
                        {"stepId": "work", "type": step_type, "config": config},
                        {"stepId": "output", "type": "OUTPUT", "config": {}},
                    ]},
                )
                self.assertEqual(
                    ["src:left", "out:work:value", "src:right"],
                    [column.column_id for column in compiled.output_schema],
                )
                self.assertEqual("가운데", compiled.output_schema[1].label)
                self.assertEqual(
                    ("L", expected, "R"),
                    duckdb.connect().execute(
                        compiled.sql, compiled.parameters
                    ).fetchone(),
                )

    def test_in_place_transform_outputs_can_be_referenced_by_later_steps(self):
        source_schema = [
            ColumnSchema("src:left", "left_value", "왼쪽", "STRING", False, ("src:left",)),
            ColumnSchema("src:middle", "middle_value", "가운데", "STRING", False, ("src:middle",)),
            ColumnSchema("src:right", "right_value", "오른쪽", "STRING", False, ("src:right",)),
        ]
        compiled = compile_pipeline(
            "SELECT 'L' AS left_value, '  jeonnam  ' AS middle_value, 'R' AS right_value",
            source_schema,
            {"steps": [
                {"stepId": "trim", "type": "TRIM", "config": {
                    "columnId": "src:middle", "mode": "BOTH", "outputId": "value",
                }},
                {"stepId": "upper", "type": "CHANGE_CASE", "config": {
                    "columnId": "out:trim:value", "mode": "UPPER", "outputId": "value",
                }},
                {"stepId": "replace", "type": "REPLACE_VALUE", "config": {
                    "columnId": "out:upper:value",
                    "mappings": [{"from": "JEONNAM", "to": "01"}],
                    "outputId": "value",
                }},
                {"stepId": "lookup", "type": "CODE_LOOKUP", "config": {
                    "columnId": "out:replace:value",
                    "values": [{"code": "01", "name": "전남"}],
                    "outputId": "value",
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )

        self.assertEqual(
            ["src:left", "out:lookup:value", "src:right"],
            [column.column_id for column in compiled.output_schema],
        )
        self.assertEqual(
            ("TRIM", "CHANGE_CASE", "REPLACE_VALUE", "CODE_LOOKUP"),
            compiled.output_schema[1].operations,
        )
        self.assertEqual(
            ("L", "전남", "R"),
            duckdb.connect().execute(compiled.sql, compiled.parameters).fetchone(),
        )

    def test_cast_then_replace_value_chains_on_the_same_column(self):
        compiled = compile_pipeline(
            "SELECT '42' AS value",
            [
                ColumnSchema(
                    "src:value",
                    "value",
                    "값",
                    "STRING",
                    False,
                    ("src:value",),
                )
            ],
            {"steps": [
                {"stepId": "cast", "type": "CAST", "config": {
                    "columnId": "src:value",
                    "targetType": "INT64",
                    "outputId": "value",
                }},
                {"stepId": "replace", "type": "REPLACE_VALUE", "config": {
                    "columnId": "out:cast:value",
                    # 화면의 일반 입력 필드가 보내는 문자열 값도 INT64 문맥에서 변환된다.
                    "mappings": [{"from": "42", "to": "100"}],
                    "outputId": "value",
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )

        self.assertEqual(["out:replace:value"], [
            column.column_id for column in compiled.output_schema
        ])
        self.assertEqual("INT64", compiled.output_schema[0].data_type)
        self.assertEqual(
            ("CAST", "REPLACE_VALUE"),
            compiled.output_schema[0].operations,
        )
        self.assertEqual(
            (100,),
            duckdb.connect().execute(
                compiled.sql, compiled.parameters
            ).fetchone(),
        )

    def test_replace_value_uses_declared_order_against_the_original_value(self):
        compiled = compile_pipeline(
            "SELECT * FROM (VALUES ('전남'), ('조대'), ('전남조대')) t(department)",
            [
                ColumnSchema(
                    "src:department",
                    "department",
                    "기관",
                    "STRING",
                    False,
                    ("src:department",),
                )
            ],
            {"steps": [
                {"stepId": "replace", "type": "REPLACE_VALUE", "config": {
                    "columnId": "src:department",
                    "matchMode": "CONTAINS",
                    "mappings": [
                        {"from": "전남", "to": "그럴"},
                        {"from": "조대", "to": "럴지도"},
                    ],
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )

        rows = duckdb.connect().execute(
            compiled.sql, compiled.parameters
        ).fetchall()
        self.assertEqual([("그럴",), ("럴지도",), ("그럴",)], rows)
        self.assertEqual(["%전남%", "그럴", "%조대%", "럴지도"], compiled.parameters)

    def test_replace_value_does_not_rematch_a_previous_mapping_result(self):
        compiled = compile_pipeline(
            "SELECT * FROM (VALUES ('전남'), ('조대')) t(department)",
            [
                ColumnSchema(
                    "src:department",
                    "department",
                    "기관",
                    "STRING",
                    False,
                    ("src:department",),
                )
            ],
            {"steps": [
                {"stepId": "replace", "type": "REPLACE_VALUE", "config": {
                    "columnId": "src:department",
                    "mappings": [
                        {"from": "전남", "to": "조대"},
                        {"from": "조대", "to": "럴지도"},
                    ],
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )

        rows = duckdb.connect().execute(
            compiled.sql, compiled.parameters
        ).fetchall()
        self.assertEqual([("조대",), ("럴지도",)], rows)

    def test_replace_value_contains_rejects_blank_or_non_string_source_values(self):
        source_schema = [
            ColumnSchema(
                "src:department",
                "department",
                "기관",
                "STRING",
                False,
                ("src:department",),
            )
        ]
        for invalid_value in (None, "", " ", 0):
            with self.subTest(invalid_value=invalid_value):
                with self.assertRaisesRegex(
                    ValueError,
                    r"REPLACE_VALUE_CONTAINS_VALUE_REQUIRED: mappings\[0\]\.from",
                ):
                    compile_pipeline(
                        "SELECT '전남' AS department",
                        source_schema,
                        {"steps": [
                            {"stepId": "replace", "type": "REPLACE_VALUE", "config": {
                                "columnId": "src:department",
                                "matchMode": "CONTAINS",
                                "mappings": [{"from": invalid_value, "to": "변경"}],
                            }},
                            {"stepId": "output", "type": "OUTPUT", "config": {}},
                        ]},
                    )

    def test_replace_value_distinguishes_string_zero_and_empty_exact_value(self):
        source_schema = [
            ColumnSchema(
                "src:department",
                "department",
                "기관",
                "STRING",
                False,
                ("src:department",),
            )
        ]
        cases = [
            (
                "SELECT 'A0B' AS department",
                "CONTAINS",
                "0",
                "문자열 영",
            ),
            (
                "SELECT '' AS department",
                "EXACT",
                "",
                "빈 문자열",
            ),
        ]
        for source_sql, match_mode, source_value, expected in cases:
            with self.subTest(match_mode=match_mode):
                compiled = compile_pipeline(
                    source_sql,
                    source_schema,
                    {"steps": [
                        {"stepId": "replace", "type": "REPLACE_VALUE", "config": {
                            "columnId": "src:department",
                            "matchMode": match_mode,
                            "mappings": [{"from": source_value, "to": expected}],
                        }},
                        {"stepId": "output", "type": "OUTPUT", "config": {}},
                    ]},
                )
                self.assertEqual(
                    (expected,),
                    duckdb.connect().execute(
                        compiled.sql, compiled.parameters
                    ).fetchone(),
                )

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
        self.assertEqual(pipeline,compiled.resolved_pipeline)
        self.assertEqual(canonical_hash(pipeline),compiled.pipeline_hash)
        self.assertEqual(["내과"],compiled.parameters)

    def test_pivot_rejects_sum_for_a_text_column_before_execution(self):
        pipeline={"pipelineVersion":1,"steps":[
            {"stepId":"pivot","type":"PIVOT","config":{"groupColumnIds":[],"pivotColumnId":"src:department","values":[{"valueId":"a","value":"A","label":"A"}],"aggregates":[{"aggregateId":"patients","op":"SUM","columnId":"src:patient","label":"합계"}]}},
            {"stepId":"output","type":"OUTPUT","config":{}},
        ]}
        with self.assertRaisesRegex(ValueError,"PIVOT_NUMERIC_AGGREGATE_REQUIRED"):
            compile_pipeline("SELECT * FROM source",SOURCE,pipeline)

    def test_pivot_places_values_in_columns_per_row_group(self):
        pipeline={"pipelineVersion":1,"steps":[
            {"stepId":"pivot","type":"PIVOT","config":{"groupColumnIds":["src:patient"],"pivotColumnId":"src:department","values":[{"valueId":"out","value":"외래","label":"외래"},{"valueId":"in","value":"입원","label":"입원"}],"aggregates":[{"aggregateId":"amount","op":"FIRST","columnId":"src:amount","label":"값"}]}},
            {"stepId":"output","type":"OUTPUT","config":{}},
        ]}
        source="SELECT * FROM (VALUES ('외래', 10, 'p1'), ('입원', 20, 'p1'), ('외래', 30, 'p2')) t(department, amount, patient_id)"
        compiled=compile_pipeline(source,SOURCE,pipeline)
        rows=duckdb.connect().execute(compiled.sql,compiled.parameters).fetchall()
        self.assertEqual([('p1',10,20),('p2',30,None)],sorted(rows))

    def test_output_is_required(self):
        with self.assertRaisesRegex(ValueError,"final active step"):
            compile_pipeline("SELECT * FROM source",SOURCE,{"steps":[]})

    def test_cast_replaces_a_middle_column_without_changing_its_position(self):
        source_schema = [
            ColumnSchema("src:left", "left_value", "왼쪽", "STRING", False, ("src:left",)),
            ColumnSchema("src:middle", "middle_value", "가운데", "STRING", False, ("src:middle",)),
            ColumnSchema("src:right", "right_value", "오른쪽", "STRING", False, ("src:right",)),
        ]
        compiled = compile_pipeline(
            "SELECT 'L' AS left_value, '42' AS middle_value, 'R' AS right_value",
            source_schema,
            {"steps": [
                {"stepId": "cast-middle", "type": "CAST", "config": {
                    "columnId": "src:middle",
                    "targetType": "INT64",
                    "outputId": "integer",
                    "keepInput": False,
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )

        self.assertEqual(
            ["src:left", "out:cast-middle:integer", "src:right"],
            [column.column_id for column in compiled.output_schema],
        )
        self.assertEqual(
            ["STRING", "INT64", "STRING"],
            [column.data_type for column in compiled.output_schema],
        )
        self.assertEqual("가운데", compiled.output_schema[1].label)

        result = duckdb.connect().execute(compiled.sql, compiled.parameters)
        self.assertEqual(
            [column.physical_name for column in compiled.output_schema],
            [description[0] for description in result.description],
        )
        self.assertEqual(["VARCHAR", "BIGINT", "VARCHAR"], [str(description[1]) for description in result.description])
        self.assertEqual(("L", 42, "R"), result.fetchone())

    def test_cast_keep_input_is_accepted_but_still_replaces_the_source(self):
        source_schema = [
            ColumnSchema("src:left", "left_value", "왼쪽", "STRING", False, ("src:left",)),
            ColumnSchema("src:middle", "middle_value", "가운데", "STRING", False, ("src:middle",)),
            ColumnSchema("src:right", "right_value", "오른쪽", "STRING", False, ("src:right",)),
        ]
        compiled = compile_pipeline(
            "SELECT 'L' AS left_value, '42' AS middle_value, 'R' AS right_value",
            source_schema,
            {"steps": [
                {"stepId": "cast-middle", "type": "CAST", "config": {
                    "columnId": "src:middle",
                    "targetType": "INT64",
                    "outputId": "integer",
                    "keepInput": True,
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )

        self.assertEqual(
            ["src:left", "out:cast-middle:integer", "src:right"],
            [column.column_id for column in compiled.output_schema],
        )
        result = duckdb.connect().execute(compiled.sql, compiled.parameters)
        self.assertEqual(
            [column.physical_name for column in compiled.output_schema],
            [description[0] for description in result.description],
        )
        self.assertEqual(
            ["VARCHAR", "BIGINT", "VARCHAR"],
            [str(description[1]) for description in result.description],
        )
        self.assertEqual(("L", 42, "R"), result.fetchone())

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
                cast_index = next(
                    index
                    for index, column in enumerate(compiled.output_schema)
                    if column.column_id == "out:cast:cast"
                )
                value = duckdb.connect().execute(
                    compiled.sql, compiled.parameters
                ).fetchone()[cast_index]
                self.assertEqual(expected, str(value))

    def test_cast_parses_fractional_timezone_timestamp_string(self):
        raw_value = "2026-02-11 15:18:49.120833+00:00"
        cases = [
            ("DATE", "", "2026-02-11"),
            ("TIMESTAMP", "", "2026-02-11 15:18:49.120833"),
            (
                "TIMESTAMP_TZ",
                "",
                "2026-02-11 15:18:49.120833+00:00",
            ),
            (
                "TIMESTAMP_TZ",
                "ISO8601_TZ",
                "2026-02-11 15:18:49.120833+00:00",
            ),
        ]
        for target_type, input_format, expected in cases:
            with self.subTest(target_type=target_type, input_format=input_format):
                compiled = compile_pipeline(
                    f"SELECT '{raw_value}' AS department, 1 AS amount, 'p1' AS patient_id",
                    SOURCE,
                    {"steps": [
                        {"stepId": "cast", "type": "CAST", "config": {
                            "columnId": "src:department",
                            "targetType": target_type,
                            "inputFormat": input_format,
                            "onError": "NULL",
                        }},
                        {"stepId": "output", "type": "OUTPUT", "config": {}},
                    ]},
                )
                connection = duckdb.connect()
                try:
                    connection.execute("SET TimeZone='UTC'")
                    cast_index = next(
                        index
                        for index, column in enumerate(compiled.output_schema)
                        if column.column_id == "out:cast:cast"
                    )
                    value = connection.execute(
                        compiled.sql, compiled.parameters
                    ).fetchone()[cast_index]
                finally:
                    connection.close()
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

    def test_declared_creation_transforms_keep_inputs_and_add_results(self):
        cases = [
            (
                "SPLIT_COLUMN",
                {
                    "inputColumnId": "src:department",
                    "delimiter": "-",
                    "outputs": [
                        {"outputId": "first", "label": "첫 값"},
                        {"outputId": "second", "label": "둘째 값"},
                    ],
                },
                ["out:work:first", "out:work:second"],
            ),
            (
                "MERGE_COLUMNS",
                {
                    "inputColumnIds": ["src:department", "src:patient"],
                    "delimiter": "-",
                    "output": {"outputId": "merged", "label": "합친 값"},
                },
                ["out:work:merged"],
            ),
            (
                "CALCULATE",
                {
                    "expression": {
                        "op": "ADD",
                        "args": [
                            {"op": "COLUMN", "columnId": "src:amount"},
                            {"op": "LITERAL", "value": 1, "dataType": "INT64"},
                        ],
                    },
                    "outputId": "calculated",
                    "label": "계산 결과",
                },
                ["out:work:calculated"],
            ),
        ]

        for step_type, config, generated_ids in cases:
            with self.subTest(step_type=step_type):
                compiled = compile_pipeline(
                    "SELECT 'A-B' AS department, 10 AS amount, 'p1' AS patient_id",
                    SOURCE,
                    {"steps": [
                        {"stepId": "work", "type": step_type, "config": config},
                        {"stepId": "output", "type": "OUTPUT", "config": {}},
                    ]},
                )
                self.assertEqual(
                    [
                        "src:department",
                        "src:amount",
                        "src:patient",
                        *generated_ids,
                    ],
                    [column.column_id for column in compiled.output_schema],
                )

    def test_row_number_is_created_before_every_existing_column(self):
        compiled = compile_pipeline(
            "SELECT 'A' AS department, 10 AS amount, 'p1' AS patient_id",
            SOURCE,
            {"steps": [
                {"stepId": "number", "type": "ROW_NUMBER", "config": {
                    "orderBy": [{"columnId": "src:amount", "direction": "ASC"}],
                    "output": {"outputId": "rn", "label": "행 번호"},
                }},
                {"stepId": "output", "type": "OUTPUT", "config": {}},
            ]},
        )

        self.assertEqual(
            ["out:number:rn", "src:department", "src:amount", "src:patient"],
            [column.column_id for column in compiled.output_schema],
        )
        result = duckdb.connect().execute(compiled.sql, compiled.parameters)
        self.assertEqual(
            [column.physical_name for column in compiled.output_schema],
            [description[0] for description in result.description],
        )
        self.assertEqual((1, "A", 10, "p1"), result.fetchone())

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
