from __future__ import annotations

import unittest

from pydantic import ValidationError

from cdw_extract.analytics_models import CalculatedExpression


LITERAL = {"op": "LITERAL", "value": 1}
COLUMN = {"op": "COLUMN", "column": "event_date"}


class CalculatedExpressionBoundaryTest(unittest.TestCase):
    def test_operator_validation_preserves_error_order_and_messages(self) -> None:
        cases = [
            (
                {"op": "ADD", "column": "amount", "args": [LITERAL, LITERAL]},
                "column is valid only for COLUMN",
            ),
            ({"op": "LITERAL"}, "LITERAL requires only value"),
            ({"op": "ADD", "args": [LITERAL]}, "ADD requires exactly two args"),
            (
                {"op": "DATE_DIFF", "args": [LITERAL]},
                "DATE_DIFF requires exactly two args",
            ),
            (
                {"op": "DATE_DIFF", "args": [LITERAL, LITERAL]},
                "DATE_DIFF requires unit",
            ),
            (
                {
                    "op": "PARSE_DATE",
                    "args": [],
                    "format": "BAD",
                    "onError": "NULL",
                },
                "PARSE_DATE requires exactly one arg",
            ),
            (
                {
                    "op": "PARSE_DATE",
                    "args": [COLUMN],
                    "format": "BAD",
                    "onError": "NULL",
                },
                "PARSE_DATE requires a supported format",
            ),
            (
                {
                    "op": "PARSE_NUMBER",
                    "args": [COLUMN],
                    "format": "PLAIN",
                },
                "parse expressions currently require onError=NULL",
            ),
            (
                {"op": "CASE", "else": LITERAL},
                "CASE requires branches and else",
            ),
            (
                {"op": "NOT", "args": [LITERAL], "else": LITERAL},
                "branches and else are valid only for CASE",
            ),
        ]

        for payload, expected_message in cases:
            with self.subTest(payload=payload):
                with self.assertRaises(ValidationError) as raised:
                    CalculatedExpression.model_validate(payload)
                self.assertIn(expected_message, str(raised.exception))

    def test_each_operator_group_accepts_its_existing_wire_shape(self) -> None:
        cases = [
            COLUMN,
            {"op": "LITERAL", "value": None},
            {"op": "ADD", "args": [LITERAL, LITERAL]},
            {"op": "NOT", "args": [LITERAL]},
            {"op": "COALESCE", "args": [LITERAL, LITERAL]},
            {"op": "CONCAT", "args": [LITERAL], "separator": ","},
            {
                "op": "DATE_DIFF",
                "args": [COLUMN, COLUMN],
                "unit": "DAY",
            },
            {"op": "DATE_PART", "args": [COLUMN], "unit": "MONTH"},
            {
                "op": "PARSE_DATE",
                "args": [COLUMN],
                "format": "YYYY-MM-DD",
                "onError": "NULL",
            },
            {
                "op": "PARSE_NUMBER",
                "args": [COLUMN],
                "format": "THOUSANDS_COMMA",
                "onError": "NULL",
            },
            {
                "op": "CASE",
                "branches": [{"when": LITERAL, "then": LITERAL}],
                "else": LITERAL,
            },
        ]

        for payload in cases:
            with self.subTest(operator=payload["op"]):
                expression = CalculatedExpression.model_validate(payload)
                self.assertEqual(payload["op"], expression.op.value)

        parse_wire = CalculatedExpression.model_validate(cases[8]).model_dump(
            by_alias=True
        )
        case_wire = CalculatedExpression.model_validate(cases[10]).model_dump(
            by_alias=True
        )
        self.assertEqual("NULL", parse_wire["onError"])
        self.assertNotIn("on_error", parse_wire)
        self.assertIn("else", case_wire)
        self.assertNotIn("else_expression", case_wire)


if __name__ == "__main__":
    unittest.main()
