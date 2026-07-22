"""검증된 분석 DSL을 DuckDB SQL과 바인딩 파라미터로 변환한다.

이 모듈은 외부 SQL 표현식을 허용하지 않는 보안 경계다. 새 연산자를 추가할 때에는 지원 타입,
식별자 인용, 파라미터 바인딩과 표현식 복잡도 제한을 함께 검토한다.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from cdw_extract.analytics_models import (
    Aggregation,
    AnalyticsCalculatedField,
    AnalyticsColumn,
    AnalyticsDetailRequest,
    AnalyticsField,
    AnalyticsQueryRequest,
    AnalyticsSort,
    CalculatedDataType,
    CalculatedExpression,
    ChartType,
    ComparisonMode,
    ExpressionOp,
    FilterOperator,
    NullPolicy,
    SortDirection,
    ValueTransform,
)
from cdw_extract.duck import quote_ident


@dataclass(frozen=True)
class ColumnInfo:
    """원본 또는 계산 열의 이름과 DuckDB 자료형을 보관한다."""

    name: str
    duckdb_type: str

    @property
    def kind(self) -> str:
        upper = self.duckdb_type.upper()
        if upper.endswith("[]") or upper.startswith(
            ("LIST(", "STRUCT(", "MAP(", "UNION(")
        ):
            return "other"
        if upper.startswith(
            (
                "TINYINT",
                "SMALLINT",
                "INTEGER",
                "BIGINT",
                "HUGEINT",
                "UTINYINT",
                "USMALLINT",
                "UINTEGER",
                "UBIGINT",
                "FLOAT",
                "DOUBLE",
                "DECIMAL",
                "REAL",
            )
        ):
            return "numeric"
        if upper.startswith(("DATE", "TIME", "TIMESTAMP")):
            return "temporal"
        if upper.startswith(("VARCHAR", "CHAR", "TEXT", "UUID", "ENUM")):
            return "text"
        if upper.startswith("BOOLEAN"):
            return "boolean"
        return "other"

    @property
    def supports_time_grain(self) -> bool:
        upper = self.duckdb_type.upper()
        return upper.startswith("DATE") or upper.startswith("TIMESTAMP")


@dataclass(frozen=True)
class CompiledAnalyticsQuery:
    """차트 분석 SQL과 바인딩 값, 출력 스키마, 실행 힌트이다."""

    sql: str
    parameters: list[object]
    columns: list[AnalyticsColumn]
    row_limit: int
    detect_truncation: bool = True
    warnings: tuple[str, ...] = ()
    hidden_keys: tuple[str, ...] = ()
    others_label: str | None = None


@dataclass(frozen=True)
class CompiledDetailQuery:
    """상세 행 조회 SQL과 바인딩 값, 출력 스키마이다."""

    sql: str
    parameters: list[object]
    columns: list[AnalyticsColumn]
    row_limit: int


@dataclass(frozen=True)
class _CompiledCalculatedExpression:
    """계산식 한 노드의 SQL, 바인딩 값과 추론된 논리 자료형이다."""

    sql: str
    parameters: tuple[object, ...]
    kind: str


@dataclass(frozen=True)
class _CompiledCalculatedArguments:
    """연산자 전략에 전달할 자식 계산식 결과를 순서대로 보관한다."""

    values: tuple[_CompiledCalculatedExpression, ...]

    @property
    def sql(self) -> tuple[str, ...]:
        return tuple(value.sql for value in self.values)

    @property
    def parameters(self) -> tuple[object, ...]:
        return tuple(
            parameter for value in self.values for parameter in value.parameters
        )

    @property
    def kinds(self) -> tuple[str, ...]:
        return tuple(value.kind for value in self.values)


@dataclass(frozen=True)
class _CategoryChartContext:
    """범주형 차트 SQL 전략들이 공유하는 검증 완료 컴파일 문맥이다."""

    category: AnalyticsField
    chart_type: ChartType
    category_expression: str
    series: AnalyticsField | None
    series_expression: str | None
    value_expression: str
    aggregation: Aggregation
    where: str
    filter_parameters: tuple[object, ...]
    order_by: str
    columns: tuple[AnalyticsColumn, ...]


@dataclass(frozen=True)
class _CategoryProjection:
    """범주형 집계 후 비교 단계가 선택할 관계와 출력 열을 나타낸다."""

    relation: str
    select_columns: str
    columns: tuple[AnalyticsColumn, ...]
    warnings: tuple[str, ...] = ()


_CALCULATED_TYPE_KIND = {
    CalculatedDataType.NUMBER: "numeric",
    CalculatedDataType.TEXT: "text",
    CalculatedDataType.DATE: "temporal",
    CalculatedDataType.BOOLEAN: "boolean",
}

_ARITHMETIC_OPERATORS = {
    ExpressionOp.ADD: "+",
    ExpressionOp.SUBTRACT: "-",
    ExpressionOp.MULTIPLY: "*",
    ExpressionOp.DIVIDE: "/",
}
_COMPARISON_OPERATORS = {
    ExpressionOp.EQ: "=",
    ExpressionOp.NE: "<>",
    ExpressionOp.GT: ">",
    ExpressionOp.GTE: ">=",
    ExpressionOp.LT: "<",
    ExpressionOp.LTE: "<=",
}
_PARSE_DATE_FORMATS = {
    "YYMMDD": "%y%m%d",
    "YYYYMMDD": "%Y%m%d",
    "YYYY-MM-DD": "%Y-%m-%d",
    "YYYY/MM/DD": "%Y/%m/%d",
    "YYYYMMDDHH24MISS": "%Y%m%d%H%M%S",
    "YYYY-MM-DD HH24:MI:SS": "%Y-%m-%d %H:%M:%S",
}


class AnalyticsCompiler:
    """폐쇄형 분석 DSL을 컴파일한다. 호출자로부터 SQL 표현식을 직접 받지 않는다."""

    def __init__(
        self,
        request: AnalyticsQueryRequest,
        parquet_path: str | Path,
        schema: list[tuple[str, str]],
        source_row_limit: int | None = None,
    ):
        self.request = request
        self.parquet_path = Path(parquet_path).as_posix()
        self.schema = {
            name: ColumnInfo(name, duckdb_type) for name, duckdb_type in schema
        }
        self.source_row_limit = source_row_limit
        if not self.schema:
            raise ValueError("source Parquet schema is empty")
        self.calculated_columns: dict[str, ColumnInfo] = {}
        self.calculated_labels: dict[str, str] = {}
        self._calculated_selects: list[str] = []
        self._calculated_parameters: list[object] = []
        self._build_calculated_fields(request.calculated_fields)

    def _build_calculated_fields(self, fields: list[AnalyticsCalculatedField]) -> None:
        for index, field in enumerate(fields):
            alias = f"__calculated_{index}"
            node_count = [0]
            compiled = self._compile_calculated_expression(
                field.formula, depth=0, node_count=node_count
            )
            kind = compiled.kind
            if field.data_type is not None:
                expected = _CALCULATED_TYPE_KIND[field.data_type]
                if kind not in {expected, "unknown"}:
                    raise ValueError(
                        f"calculated field {field.id} declares {field.data_type.value} but formula is {kind}"
                    )
                kind = expected
            duckdb_type = {
                "numeric": "DOUBLE",
                "text": "VARCHAR",
                "temporal": "TIMESTAMP",
                "boolean": "BOOLEAN",
                "unknown": "VARCHAR",
            }[kind]
            self.calculated_columns[field.id] = ColumnInfo(alias, duckdb_type)
            self.calculated_labels[field.id] = field.name
            self._calculated_selects.append(f"{compiled.sql} AS {quote_ident(alias)}")
            self._calculated_parameters.extend(compiled.parameters)

    def _compile_calculated_expression(
        self,
        expression: CalculatedExpression,
        *,
        depth: int,
        node_count: list[int],
    ) -> _CompiledCalculatedExpression:
        if depth > 8:
            raise ValueError("calculated formula depth must not exceed 8")
        node_count[0] += 1
        if node_count[0] > 100:
            raise ValueError("calculated formula must not exceed 100 nodes")

        op = expression.op
        if op == ExpressionOp.COLUMN:
            return self._compile_calculated_column(expression)
        if op == ExpressionOp.LITERAL:
            return self._compile_calculated_literal(expression)

        arguments = _CompiledCalculatedArguments(
            tuple(
                self._compile_calculated_expression(
                    argument,
                    depth=depth + 1,
                    node_count=node_count,
                )
                for argument in expression.args
            )
        )
        strategies = {
            **dict.fromkeys(_ARITHMETIC_OPERATORS, self._compile_calculated_arithmetic),
            **dict.fromkeys(_COMPARISON_OPERATORS, self._compile_calculated_comparison),
            **dict.fromkeys(
                {ExpressionOp.AND, ExpressionOp.OR, ExpressionOp.NOT},
                self._compile_calculated_boolean,
            ),
            ExpressionOp.COALESCE: self._compile_calculated_coalesce,
            ExpressionOp.CONCAT: self._compile_calculated_concat,
            ExpressionOp.DATE_DIFF: self._compile_calculated_date,
            ExpressionOp.DATE_PART: self._compile_calculated_date,
            ExpressionOp.PARSE_DATE: self._compile_calculated_parse,
            ExpressionOp.PARSE_NUMBER: self._compile_calculated_parse,
            ExpressionOp.CASE: self._compile_calculated_case,
        }
        strategy = strategies.get(op)
        if strategy is None:
            raise ValueError(f"unsupported calculated expression op: {op.value}")
        return strategy(expression, arguments, depth, node_count)

    def _compile_calculated_column(
        self,
        expression: CalculatedExpression,
    ) -> _CompiledCalculatedExpression:
        column = self.schema.get(expression.column or "")
        if column is None:
            raise ValueError(
                f"unknown physical column in calculated formula: {expression.column}"
            )
        if column.kind == "other":
            raise ValueError(
                f"calculated formulas do not support column type {column.duckdb_type}"
            )
        return _CompiledCalculatedExpression(quote_ident(column.name), (), column.kind)

    @staticmethod
    def _compile_calculated_literal(
        expression: CalculatedExpression,
    ) -> _CompiledCalculatedExpression:
        value = expression.value
        if value is None:
            kind = "unknown"
        elif isinstance(value, bool):
            kind = "boolean"
        elif isinstance(value, (int, float, Decimal)):
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("calculated literal numbers must be finite")
            kind = "numeric"
        elif isinstance(value, (date, datetime)):
            kind = "temporal"
        else:
            kind = "text"
        return _CompiledCalculatedExpression("?", (value,), kind)

    def _compile_calculated_arithmetic(
        self,
        expression: CalculatedExpression,
        arguments: _CompiledCalculatedArguments,
        _depth: int,
        _node_count: list[int],
    ) -> _CompiledCalculatedExpression:
        if any(kind not in {"numeric", "unknown"} for kind in arguments.kinds):
            raise ValueError(f"{expression.op.value} accepts only numeric operands")
        left, right = arguments.sql
        if expression.op == ExpressionOp.DIVIDE:
            sql = f"({left} / NULLIF({right}, 0))"
        else:
            sql = f"({left} {_ARITHMETIC_OPERATORS[expression.op]} {right})"
        return _CompiledCalculatedExpression(sql, arguments.parameters, "numeric")

    def _compile_calculated_comparison(
        self,
        expression: CalculatedExpression,
        arguments: _CompiledCalculatedArguments,
        _depth: int,
        _node_count: list[int],
    ) -> _CompiledCalculatedExpression:
        non_unknown = {kind for kind in arguments.kinds if kind != "unknown"}
        if len(non_unknown) > 1:
            raise ValueError(
                f"{expression.op.value} operands must have compatible types"
            )
        left, right = arguments.sql
        sql = f"({left} {_COMPARISON_OPERATORS[expression.op]} {right})"
        return _CompiledCalculatedExpression(sql, arguments.parameters, "boolean")

    def _compile_calculated_boolean(
        self,
        expression: CalculatedExpression,
        arguments: _CompiledCalculatedArguments,
        _depth: int,
        _node_count: list[int],
    ) -> _CompiledCalculatedExpression:
        if expression.op == ExpressionOp.NOT:
            if arguments.kinds[0] not in {"boolean", "unknown"}:
                raise ValueError("NOT accepts only a boolean operand")
            sql = f"(NOT {arguments.sql[0]})"
        else:
            if any(kind not in {"boolean", "unknown"} for kind in arguments.kinds):
                raise ValueError(f"{expression.op.value} accepts only boolean operands")
            sql = f"({arguments.sql[0]} {expression.op.value} {arguments.sql[1]})"
        return _CompiledCalculatedExpression(sql, arguments.parameters, "boolean")

    def _compile_calculated_coalesce(
        self,
        _expression: CalculatedExpression,
        arguments: _CompiledCalculatedArguments,
        _depth: int,
        _node_count: list[int],
    ) -> _CompiledCalculatedExpression:
        non_unknown = {kind for kind in arguments.kinds if kind != "unknown"}
        if len(non_unknown) > 1:
            raise ValueError("COALESCE operands must have compatible types")
        sql = f"coalesce({', '.join(arguments.sql)})"
        return _CompiledCalculatedExpression(
            sql,
            arguments.parameters,
            next(iter(non_unknown), "unknown"),
        )

    def _compile_calculated_concat(
        self,
        expression: CalculatedExpression,
        arguments: _CompiledCalculatedArguments,
        _depth: int,
        _node_count: list[int],
    ) -> _CompiledCalculatedExpression:
        cast_arguments = ", ".join(
            f"CAST({argument} AS VARCHAR)" for argument in arguments.sql
        )
        sql = f"concat_ws(?, {cast_arguments})"
        return _CompiledCalculatedExpression(
            sql,
            (expression.separator or "", *arguments.parameters),
            "text",
        )

    def _compile_calculated_date(
        self,
        expression: CalculatedExpression,
        arguments: _CompiledCalculatedArguments,
        _depth: int,
        _node_count: list[int],
    ) -> _CompiledCalculatedExpression:
        unit = expression.unit.value.lower()
        if expression.op == ExpressionOp.DATE_DIFF:
            if any(kind not in {"temporal", "unknown"} for kind in arguments.kinds):
                raise ValueError("DATE_DIFF accepts only temporal operands")
            sql = f"date_diff('{unit}', {arguments.sql[0]}, {arguments.sql[1]})"
        else:
            if arguments.kinds[0] not in {"temporal", "unknown"}:
                raise ValueError("DATE_PART accepts only a temporal operand")
            sql = f"date_part('{unit}', {arguments.sql[0]})"
        return _CompiledCalculatedExpression(sql, arguments.parameters, "numeric")

    def _compile_calculated_parse(
        self,
        expression: CalculatedExpression,
        arguments: _CompiledCalculatedArguments,
        _depth: int,
        _node_count: list[int],
    ) -> _CompiledCalculatedExpression:
        source = f"CAST({arguments.sql[0]} AS VARCHAR)"
        if expression.op == ExpressionOp.PARSE_DATE:
            date_format = _PARSE_DATE_FORMATS[expression.format or ""]
            sql = f"try_strptime({source}, '{date_format}')"
            kind = "temporal"
        else:
            if expression.format == "THOUSANDS_COMMA":
                source = f"replace({source}, ',', '')"
            sql = f"TRY_CAST({source} AS DOUBLE)"
            kind = "numeric"
        return _CompiledCalculatedExpression(sql, arguments.parameters, kind)

    def _compile_calculated_case(
        self,
        expression: CalculatedExpression,
        _arguments: _CompiledCalculatedArguments,
        depth: int,
        node_count: list[int],
    ) -> _CompiledCalculatedExpression:
        pieces: list[str] = []
        parameters: list[object] = []
        result_kinds: set[str] = set()
        for branch in expression.branches:
            when = self._compile_calculated_expression(
                branch.when,
                depth=depth + 1,
                node_count=node_count,
            )
            then = self._compile_calculated_expression(
                branch.then,
                depth=depth + 1,
                node_count=node_count,
            )
            if when.kind not in {"boolean", "unknown"}:
                raise ValueError("CASE when expressions must be boolean")
            if then.kind != "unknown":
                result_kinds.add(then.kind)
            pieces.append(f"WHEN {when.sql} THEN {then.sql}")
            parameters.extend(when.parameters)
            parameters.extend(then.parameters)

        else_expression = expression.else_expression
        if else_expression is None:  # pragma: no cover - Pydantic shape invariant
            raise RuntimeError("CASE expression is missing else")
        otherwise = self._compile_calculated_expression(
            else_expression,
            depth=depth + 1,
            node_count=node_count,
        )
        if otherwise.kind != "unknown":
            result_kinds.add(otherwise.kind)
        if len(result_kinds) > 1:
            raise ValueError("CASE result expressions must have compatible types")
        parameters.extend(otherwise.parameters)
        sql = f"(CASE {' '.join(pieces)} ELSE {otherwise.sql} END)"
        return _CompiledCalculatedExpression(
            sql,
            tuple(parameters),
            next(iter(result_kinds), "unknown"),
        )

    def _source_sql(self) -> str:
        relation = "read_parquet(?)"
        if self.source_row_limit is not None:
            relation = f"(SELECT * FROM read_parquet(?) LIMIT {self.source_row_limit})"
        base = f"{relation} AS {quote_ident('__raw')}"
        if not self._calculated_selects:
            return f"{relation} AS {quote_ident('__src')}"
        return (
            f"(SELECT {quote_ident('__raw')}.*, {', '.join(self._calculated_selects)} "
            f"FROM {base}) AS {quote_ident('__src')}"
        )

    def _query_parameters(self, parameters: list[object]) -> list[object]:
        # Calculated expressions occur in the SELECT list of the derived source,
        # therefore their placeholders precede read_parquet(?) in SQL order.
        return [*self._calculated_parameters, self.parquet_path, *parameters]

    def compile(self) -> CompiledAnalyticsQuery:
        chart_type = self.request.chart_type
        if (
            self.request.top_n
            and self.request.top_n.enabled
            and chart_type
            not in {ChartType.BAR, ChartType.PIE, ChartType.LINE, ChartType.FUNNEL}
        ):
            raise ValueError("topN is supported only for category charts")
        if chart_type in {ChartType.BAR, ChartType.PIE, ChartType.LINE}:
            self._validate_roles({"category", "value", "series"})
            category = self._category_field()
            return self._compile_category_chart(category, chart_type)
        if chart_type == ChartType.FUNNEL:
            self._validate_roles({"category", "value", "series", "stage"})
            if self.request.encoding.stage and self.request.encoding.category:
                raise ValueError(
                    "FUNNEL accepts either encoding.stage or encoding.category, not both"
                )
            category = self.request.encoding.stage or self.request.encoding.category
            return self._compile_category_chart(
                self._required(category, "encoding.stage or encoding.category"),
                chart_type,
            )
        if chart_type == ChartType.SCATTER:
            self._validate_roles({"x", "y", "size", "series"})
            return self._compile_scatter()
        if chart_type == ChartType.BOXPLOT:
            self._validate_roles({"value", "group"})
            return self._compile_boxplot()
        if chart_type == ChartType.SANKEY:
            self._validate_roles({"source", "target", "value"})
            return self._compile_sankey()
        if chart_type == ChartType.TREEMAP:
            self._validate_roles({"hierarchy", "value"})
            return self._compile_treemap()
        raise ValueError(f"unsupported chartType: {chart_type}")

    def _category_field(self) -> AnalyticsField:
        if self.request.drilldown is not None:
            return self.request.drilldown.fields[self.request.drilldown.level]
        return self._required(self.request.encoding.category, "encoding.category")

    def _series_field(self) -> AnalyticsField | None:
        comparison = self.request.comparison
        if (
            comparison
            and comparison.enabled
            and comparison.mode == ComparisonMode.SERIES
        ):
            if self.request.encoding.series is not None:
                raise ValueError(
                    "SERIES comparison.field and encoding.series cannot both be set"
                )
            return comparison.field
        return self.request.encoding.series

    def _row_limit(self) -> int:
        if self.request.top_n and self.request.top_n.enabled:
            return self.request.top_n.count + (
                1 if self.request.top_n.include_others else 0
            )
        return self.request.limit

    def _include_others(self) -> bool:
        if self.request.top_n and self.request.top_n.enabled:
            return self.request.top_n.include_others
        return self.request.options.include_others

    def _top_count(self) -> int:
        if self.request.top_n and self.request.top_n.enabled:
            return self.request.top_n.count
        return max(1, self.request.limit - 1)

    def _validate_roles(self, allowed: set[str]) -> None:
        encoding = self.request.encoding
        scalar_roles = {
            role
            for role in (
                "category",
                "value",
                "series",
                "x",
                "y",
                "size",
                "group",
                "stage",
                "source",
                "target",
            )
            if getattr(encoding, role) is not None
        }
        if encoding.hierarchy:
            scalar_roles.add("hierarchy")
        unexpected = scalar_roles - allowed
        if unexpected:
            raise ValueError(
                f"{self.request.chart_type.value} does not support encoding role(s): {', '.join(sorted(unexpected))}"
            )

    @staticmethod
    def _required(field: AnalyticsField | None, role: str) -> AnalyticsField:
        if field is None:
            raise ValueError(f"{role} is required")
        return field

    def _column(self, field: AnalyticsField, role: str) -> ColumnInfo:
        reference = field.derived_field_id or field.column
        if not reference:
            raise ValueError(f"{role} field reference is required")
        column = (
            self.calculated_columns.get(reference)
            if field.derived_field_id
            else self.schema.get(reference)
        )
        if column is None:
            kind = "calculated field" if field.derived_field_id else "source column"
            raise ValueError(f"unknown {kind} for {role}: {reference}")
        return column

    def _dimension(self, field: AnalyticsField, role: str) -> tuple[str, ColumnInfo]:
        if field.aggregation is not None:
            raise ValueError(f"{role} must not define aggregation")
        column = self._column(field, role)
        if column.kind == "other":
            raise ValueError(
                f"{role} requires a scalar chart-compatible column, got {column.duckdb_type}"
            )
        expression = quote_ident(column.name)
        if field.time_grain:
            if not column.supports_time_grain:
                raise ValueError(
                    f"{role}.timeGrain requires a DATE or TIMESTAMP column"
                )
            expression = f"date_trunc('{field.time_grain.value.lower()}', {expression})"
        if field.bin:
            if column.kind != "numeric":
                raise ValueError(f"{role}.bin requires a numeric column")
            size = format(field.bin.size, ".17g")
            offset = format(field.bin.offset, ".17g")
            expression = (
                f"(floor(({expression} - {offset}) / {size}) * {size} + {offset})"
            )
        return expression, column

    def _raw_numeric(self, field: AnalyticsField, role: str) -> tuple[str, ColumnInfo]:
        if (
            field.aggregation is not None
            or field.time_grain is not None
            or field.bin is not None
        ):
            raise ValueError(f"{role} must be a raw numeric column")
        column = self._column(field, role)
        if column.kind != "numeric":
            raise ValueError(
                f"{role} requires a numeric column, got {column.duckdb_type}"
            )
        return quote_ident(column.name), column

    def _measure(
        self, field: AnalyticsField | None, role: str = "encoding.value"
    ) -> tuple[str, Aggregation]:
        if field is None:
            return "count(*)", Aggregation.COUNT
        if field.time_grain is not None:
            raise ValueError(f"{role} must not define timeGrain")
        aggregation = field.aggregation or Aggregation.SUM
        if (
            aggregation == Aggregation.COUNT
            and not field.column
            and not field.derived_field_id
        ):
            return "count(*)", aggregation
        column = self._column(field, role)
        quoted = quote_ident(column.name)
        if aggregation == Aggregation.COUNT:
            return f"count({quoted})", aggregation
        if aggregation == Aggregation.COUNT_DISTINCT:
            return f"count(DISTINCT {quoted})", aggregation
        if column.kind != "numeric":
            raise ValueError(
                f"{role} aggregation {aggregation.value} requires a numeric column, got {column.duckdb_type}"
            )
        functions = {
            Aggregation.SUM: "sum",
            Aggregation.AVG: "avg",
            Aggregation.MIN: "min",
            Aggregation.MAX: "max",
            Aggregation.MEDIAN: "median",
        }
        return f"{functions[aggregation]}({quoted})", aggregation

    def _compile_filters(self) -> tuple[list[str], list[object]]:
        clauses: list[str] = []
        parameters: list[object] = []
        symbols = {
            FilterOperator.EQ: "=",
            FilterOperator.NE: "<>",
            FilterOperator.GT: ">",
            FilterOperator.GTE: ">=",
            FilterOperator.LT: "<",
            FilterOperator.LTE: "<=",
        }
        for item in self.request.all_filters:
            reference = item.derived_field_id or item.column or ""
            column = (
                self.calculated_columns.get(reference)
                if item.derived_field_id
                else self.schema.get(reference)
            )
            if column is None:
                raise ValueError(f"unknown filter field: {reference}")
            quoted = quote_ident(column.name)
            if item.time_grain:
                if not column.supports_time_grain:
                    raise ValueError(
                        "filter timeGrain requires a DATE or TIMESTAMP field"
                    )
                quoted = f"date_trunc('{item.time_grain.value.lower()}', {quoted})"
            if item.bin:
                if column.kind != "numeric":
                    raise ValueError("filter bin requires a numeric field")
                size = format(item.bin.size, ".17g")
                offset = format(item.bin.offset, ".17g")
                quoted = f"(floor(({quoted} - {offset}) / {size}) * {size} + {offset})"
            operator = item.operator
            if operator in {FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL}:
                suffix = (
                    "IS NULL" if operator == FilterOperator.IS_NULL else "IS NOT NULL"
                )
                clauses.append(f"{quoted} {suffix}")
                continue
            if operator == FilterOperator.CONTAINS:
                if column.kind != "text" or not isinstance(item.value, str):
                    raise ValueError("CONTAINS requires a text column and string value")
                clauses.append(f"contains(CAST({quoted} AS VARCHAR), ?)")
                parameters.append(item.value)
                continue
            values = (
                item.values
                if operator in {FilterOperator.IN, FilterOperator.BETWEEN}
                else [item.value]
            )
            for value in values:
                self._validate_filter_value(column, value, operator)
            if operator == FilterOperator.IN:
                placeholders = ", ".join(
                    self._parameter_expression(column) for _ in values
                )
                clauses.append(f"{quoted} IN ({placeholders})")
                parameters.extend(values)
            elif operator == FilterOperator.BETWEEN:
                parameter = self._parameter_expression(column)
                clauses.append(f"{quoted} BETWEEN {parameter} AND {parameter}")
                parameters.extend(values)
            else:
                if column.kind == "boolean" and operator not in {
                    FilterOperator.EQ,
                    FilterOperator.NE,
                }:
                    raise ValueError(
                        f"{operator.value} is not valid for BOOLEAN column {column.name}"
                    )
                clauses.append(
                    f"{quoted} {symbols[operator]} {self._parameter_expression(column)}"
                )
                parameters.append(item.value)
        return clauses, parameters

    @staticmethod
    def _parameter_expression(column: ColumnInfo) -> str:
        if column.kind != "temporal":
            return "?"
        upper = column.duckdb_type.upper()
        if upper.startswith("DATE"):
            target = "DATE"
        elif upper.startswith("TIMESTAMP"):
            target = (
                "TIMESTAMPTZ"
                if "TIME ZONE" in upper or upper.startswith("TIMESTAMPTZ")
                else "TIMESTAMP"
            )
        else:
            target = "TIME"
        return f"CAST(? AS {target})"

    @staticmethod
    def _validate_filter_value(
        column: ColumnInfo, value: object, operator: FilterOperator
    ) -> None:
        if value is None:
            raise ValueError(
                f"{operator.value} does not accept null; use IS_NULL or IS_NOT_NULL"
            )
        if column.kind == "numeric":
            if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
                raise ValueError(
                    f"filter for numeric column {column.name} requires a number"
                )
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("filter numbers must be finite")
        elif column.kind == "temporal":
            if not isinstance(value, (str, date, datetime)):
                raise ValueError(
                    f"filter for temporal column {column.name} requires an ISO string or date"
                )
        elif column.kind == "text":
            if not isinstance(value, str):
                raise ValueError(
                    f"filter for text column {column.name} requires a string"
                )
        elif column.kind == "boolean":
            if not isinstance(value, bool):
                raise ValueError(
                    f"filter for boolean column {column.name} requires a boolean"
                )
        else:
            raise ValueError(
                f"filters are not supported for column type {column.duckdb_type}"
            )

    def _where(self, extra: list[str]) -> tuple[str, list[object]]:
        clauses, parameters = self._compile_filters()
        clauses.extend(extra)
        return (f" WHERE {' AND '.join(clauses)}" if clauses else ""), parameters

    def _nonnull_condition(self, field: AnalyticsField) -> str | None:
        if self.request.options.null_policy != NullPolicy.EXCLUDE:
            return None
        column = self._column(field, "null policy")
        return f"{quote_ident(column.name)} IS NOT NULL"

    def _order_by(
        self,
        allowed_fields: set[str],
        default: list[tuple[str, SortDirection]],
    ) -> str:
        sorts = self.request.sorts
        if self.request.top_n and self.request.top_n.enabled:
            sorts = [
                AnalyticsSort(field="value", direction=self.request.top_n.direction)
            ]
        parts: list[str] = []
        if sorts:
            for sort in sorts:
                if sort.field not in allowed_fields:
                    raise ValueError(
                        f"sort field {sort.field} is not available; expected one of {sorted(allowed_fields)}"
                    )
                parts.append(
                    f"{quote_ident(sort.field)} {sort.direction.value} NULLS LAST"
                )
        else:
            parts = [
                f"{quote_ident(field)} {direction.value} NULLS LAST"
                for field, direction in default
            ]
        return f" ORDER BY {', '.join(parts)}" if parts else ""

    def _field_label(self, field: AnalyticsField | None, fallback: str) -> str:
        if field is None:
            return fallback
        if field.label:
            return field.label
        if field.derived_field_id:
            return self.calculated_labels.get(
                field.derived_field_id, field.derived_field_id
            )
        return field.column or fallback

    @staticmethod
    def _dimension_type(column: ColumnInfo, field: AnalyticsField) -> str:
        if field.time_grain or column.kind == "temporal":
            return "DATETIME"
        if column.kind == "numeric":
            return "NUMBER"
        if column.kind == "boolean":
            return "BOOLEAN"
        return "STRING"

    def _compile_category_chart(
        self,
        category: AnalyticsField,
        chart_type: ChartType,
    ) -> CompiledAnalyticsQuery:
        context = self._category_chart_context(category, chart_type)
        if self._include_others():
            return self._compile_category_with_others(context)
        return self._compile_standard_category(context)

    def _category_chart_context(
        self,
        category: AnalyticsField,
        chart_type: ChartType,
    ) -> _CategoryChartContext:
        category_expression, category_column = self._dimension(
            category, "encoding.category"
        )
        series = self._series_field()
        series_expression: str | None = None
        series_column: ColumnInfo | None = None
        if series:
            series_expression, series_column = self._dimension(
                series, "encoding.series"
            )
        value_expression, aggregation = self._measure(self.request.encoding.value)

        extra = [
            condition for condition in [self._nonnull_condition(category)] if condition
        ]
        if series:
            condition = self._nonnull_condition(series)
            if condition:
                extra.append(condition)
        where, filter_parameters = self._where(extra)

        allowed = {"category", "value"} | ({"series"} if series_expression else set())
        default_sort = (
            [("category", SortDirection.ASC)]
            if chart_type == ChartType.LINE
            else [("value", SortDirection.DESC), ("category", SortDirection.ASC)]
        )
        if series_expression:
            default_sort.append(("series", SortDirection.ASC))
        order_by = self._order_by(allowed, default_sort)

        columns = [
            AnalyticsColumn(
                key="category",
                label=self._field_label(category, "Category"),
                type=self._dimension_type(category_column, category),
            ),
            AnalyticsColumn(
                key="value",
                label=self._field_label(self.request.encoding.value, "Count"),
                type="NUMBER",
            ),
        ]
        if series and series_column:
            columns.append(
                AnalyticsColumn(
                    key="series",
                    label=self._field_label(series, "Series"),
                    type=self._dimension_type(series_column, series),
                )
            )

        return _CategoryChartContext(
            category=category,
            chart_type=chart_type,
            category_expression=category_expression,
            series=series,
            series_expression=series_expression,
            value_expression=value_expression,
            aggregation=aggregation,
            where=where,
            filter_parameters=tuple(filter_parameters),
            order_by=order_by,
            columns=tuple(columns),
        )

    def _compile_category_with_others(
        self,
        context: _CategoryChartContext,
    ) -> CompiledAnalyticsQuery:
        if context.chart_type not in {ChartType.BAR, ChartType.PIE, ChartType.FUNNEL}:
            raise ValueError("includeOthers is supported only for BAR, PIE, and FUNNEL")
        if context.series:
            raise ValueError("includeOthers does not support encoding.series")
        if context.aggregation not in {Aggregation.COUNT, Aggregation.SUM}:
            raise ValueError("includeOthers requires COUNT or SUM aggregation")
        if self._row_limit() < 2:
            raise ValueError(
                "includeOthers requires at least one Top N item plus Others"
            )
        if self.request.options.value_transform != ValueTransform.NONE:
            raise ValueError("includeOthers cannot be combined with valueTransform")

        category_alias = quote_ident("category")
        value_alias = quote_ident("value")
        top_count = self._top_count()
        sql = (
            "WITH aggregated AS ("
            f"SELECT {context.category_expression} AS {category_alias}, "
            f"{context.value_expression} AS {value_alias} "
            f"FROM {self._source_sql()}{context.where} GROUP BY 1"
            "), ranked AS ("
            f"SELECT {category_alias}, {value_alias}, CAST({category_alias} AS VARCHAR) = ? "
            f"AS {quote_ident('__label_collision')}, "
            f"row_number() OVER ({context.order_by.strip()}) AS {quote_ident('__rank')} "
            "FROM aggregated"
            "), folded AS ("
            f"SELECT CASE WHEN {quote_ident('__rank')} <= {top_count} "
            f"THEN CAST({category_alias} AS VARCHAR) ELSE ? END "
            f"AS {category_alias}, {quote_ident('__rank')} > {top_count} "
            f"AS {quote_ident('__is_others')}, "
            f"min({quote_ident('__rank')}) AS {quote_ident('__order')}, "
            f"bool_or({quote_ident('__label_collision')}) AS {quote_ident('__label_collision')}, "
            f"sum({value_alias}) AS {value_alias} FROM ranked GROUP BY 1, 2"
            ") "
            f"SELECT {category_alias}, {value_alias}, {quote_ident('__label_collision')} "
            "FROM folded "
            f"ORDER BY {quote_ident('__is_others')} ASC, {quote_ident('__order')} ASC"
        )
        columns = list(context.columns)
        columns[0] = columns[0].model_copy(update={"type": "STRING"})
        return CompiledAnalyticsQuery(
            sql=sql,
            parameters=self._query_parameters(list(context.filter_parameters))
            + [
                self.request.options.others_label,
                self.request.options.others_label,
            ],
            columns=columns,
            row_limit=self._row_limit(),
            detect_truncation=False,
            warnings=(
                "includeOthers is enabled; any categories outside the top-N are combined into one bucket.",
            ),
            hidden_keys=("__label_collision",),
            others_label=self.request.options.others_label,
        )

    def _compile_standard_category(
        self,
        context: _CategoryChartContext,
    ) -> CompiledAnalyticsQuery:
        category_alias = quote_ident("category")
        value_alias = quote_ident("value")
        series_select = (
            f", {context.series_expression} AS {quote_ident('series')}"
            if context.series_expression
            else ""
        )
        group_positions = "1, 3" if context.series_expression else "1"
        base = (
            f"SELECT {context.category_expression} AS {category_alias}, "
            f"{context.value_expression} AS {value_alias}{series_select} "
            f"FROM {self._source_sql()}{context.where} GROUP BY {group_positions}"
        )
        ctes = [f"aggregated AS ({base})"]
        current_relation = self._append_category_value_transform(context, ctes)
        projection = self._category_comparison_projection(
            context,
            ctes,
            current_relation,
        )
        sql = (
            f"WITH {', '.join(ctes)} SELECT {projection.select_columns} "
            f"FROM {projection.relation}{context.order_by} LIMIT {self._row_limit() + 1}"
        )
        return CompiledAnalyticsQuery(
            sql=sql,
            parameters=self._query_parameters(list(context.filter_parameters)),
            columns=list(projection.columns),
            row_limit=self._row_limit(),
            warnings=projection.warnings,
        )

    def _append_category_value_transform(
        self,
        context: _CategoryChartContext,
        ctes: list[str],
    ) -> str:
        transform = self.request.options.value_transform
        category_alias = quote_ident("category")
        value_alias = quote_ident("value")
        series_projection = (
            f", {quote_ident('series')}" if context.series_expression else ""
        )
        if transform == ValueTransform.PERCENT_OF_TOTAL:
            partition = (
                f"PARTITION BY {quote_ident('series')}"
                if context.series_expression
                else ""
            )
            transformed_value = f"100.0 * {value_alias} / NULLIF(sum({value_alias}) OVER ({partition}), 0)"
        elif transform == ValueTransform.RUNNING_TOTAL:
            partition = (
                f"PARTITION BY {quote_ident('series')} "
                if context.series_expression
                else ""
            )
            transformed_value = (
                f"sum({value_alias}) OVER ({partition}ORDER BY {category_alias} ASC "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
            )
        else:
            return "aggregated"
        ctes.append(
            f"transformed AS (SELECT {category_alias}, {transformed_value} AS {value_alias}"
            f"{series_projection} FROM aggregated)"
        )
        return "transformed"

    def _category_comparison_projection(
        self,
        context: _CategoryChartContext,
        ctes: list[str],
        current_relation: str,
    ) -> _CategoryProjection:
        category_alias = quote_ident("category")
        value_alias = quote_ident("value")
        select_columns = f"{category_alias}, {value_alias}"
        if context.series_expression:
            select_columns += f", {quote_ident('series')}"

        comparison = self.request.comparison
        if not (
            comparison
            and comparison.enabled
            and comparison.mode == ComparisonMode.PREVIOUS_PERIOD
        ):
            return _CategoryProjection(
                relation=current_relation,
                select_columns=select_columns,
                columns=context.columns,
            )
        if context.category.time_grain != comparison.period_unit:
            raise ValueError(
                "PREVIOUS_PERIOD periodUnit must match the active category timeGrain"
            )

        lag_count = abs(comparison.offset)
        partition = (
            f"PARTITION BY {quote_ident('series')} "
            if context.series_expression
            else ""
        )
        window = f"{partition}ORDER BY {category_alias} ASC"
        lag = f"lag({value_alias}, {lag_count}) OVER ({window})"
        ctes.append(
            f"compared AS (SELECT {select_columns}, {lag} AS {quote_ident('previousValue')} "
            f"FROM {current_relation})"
        )
        select_columns += (
            f", {quote_ident('previousValue')}, "
            f"({value_alias} - {quote_ident('previousValue')}) AS {quote_ident('change')}, "
            f"100.0 * ({value_alias} - {quote_ident('previousValue')}) / "
            f"NULLIF(abs({quote_ident('previousValue')}), 0) AS {quote_ident('changeRate')}"
        )
        comparison_columns = (
            AnalyticsColumn(key="previousValue", label="Previous value", type="NUMBER"),
            AnalyticsColumn(key="change", label="Change", type="NUMBER"),
            AnalyticsColumn(key="changeRate", label="Change rate (%)", type="NUMBER"),
        )
        return _CategoryProjection(
            relation="compared",
            select_columns=select_columns,
            columns=(*context.columns, *comparison_columns),
            warnings=(
                "PREVIOUS_PERIOD uses the previous available bucket; a missing calendar bucket is not synthesized.",
            ),
        )

    def _compile_scatter(self) -> CompiledAnalyticsQuery:
        x = self._required(self.request.encoding.x, "encoding.x")
        y = self._required(self.request.encoding.y, "encoding.y")
        x_expression, _ = self._raw_numeric(x, "encoding.x")
        y_expression, _ = self._raw_numeric(y, "encoding.y")
        select_parts = [
            f"{x_expression} AS {quote_ident('x')}",
            f"{y_expression} AS {quote_ident('y')}",
        ]
        columns = [
            AnalyticsColumn(key="x", label=self._field_label(x, "X"), type="NUMBER"),
            AnalyticsColumn(key="y", label=self._field_label(y, "Y"), type="NUMBER"),
        ]
        extra = [f"{x_expression} IS NOT NULL", f"{y_expression} IS NOT NULL"]
        allowed = {"x", "y"}

        size = self.request.encoding.size
        if size:
            size_expression, _ = self._raw_numeric(size, "encoding.size")
            select_parts.append(f"{size_expression} AS {quote_ident('size')}")
            columns.append(
                AnalyticsColumn(
                    key="size", label=self._field_label(size, "Size"), type="NUMBER"
                )
            )
            allowed.add("size")
            condition = self._nonnull_condition(size)
            if condition:
                extra.append(condition)

        series = self.request.encoding.series
        if series:
            series_expression, series_column = self._dimension(
                series, "encoding.series"
            )
            select_parts.append(f"{series_expression} AS {quote_ident('series')}")
            columns.append(
                AnalyticsColumn(
                    key="series",
                    label=self._field_label(series, "Series"),
                    type=self._dimension_type(series_column, series),
                )
            )
            allowed.add("series")
            condition = self._nonnull_condition(series)
            if condition:
                extra.append(condition)

        if self._include_others():
            raise ValueError("includeOthers is not valid for SCATTER")
        where, filter_parameters = self._where(extra)
        cap = min(self._row_limit(), self.request.options.scatter_sample_size)
        sample_size = cap + 1
        seed = self.request.options.random_seed % 2_147_483_647
        order_by = self._order_by(allowed, [])
        sql = (
            "WITH filtered AS ("
            f"SELECT {', '.join(select_parts)} FROM {self._source_sql()}{where}"
            "), sampled AS ("
            f"SELECT * FROM filtered USING SAMPLE reservoir ({sample_size} ROWS) REPEATABLE ({seed})"
            ") SELECT * FROM sampled"
            f"{order_by}"
        )
        return CompiledAnalyticsQuery(
            sql=sql,
            parameters=self._query_parameters(filter_parameters),
            columns=columns,
            row_limit=cap,
            warnings=(
                f"SCATTER uses deterministic reservoir sampling capped at {cap} points.",
            ),
        )

    def _compile_boxplot(self) -> CompiledAnalyticsQuery:
        value = self._required(self.request.encoding.value, "encoding.value")
        value_expression, _ = self._raw_numeric(value, "encoding.value")
        group = self.request.encoding.group
        extra = [f"{value_expression} IS NOT NULL"]
        if group:
            group_expression, group_column = self._dimension(group, "encoding.group")
            category_expression = group_expression
            condition = self._nonnull_condition(group)
            if condition:
                extra.append(condition)
            category_type = self._dimension_type(group_column, group)
            category_label = self._field_label(group, "Group")
        else:
            category_expression = "CAST('All' AS VARCHAR)"
            category_type = "STRING"
            category_label = "Group"
        if self._include_others():
            raise ValueError("includeOthers is not valid for BOXPLOT")
        where, filter_parameters = self._where(extra)
        allowed = {
            "category",
            "count",
            "min",
            "q1",
            "median",
            "q3",
            "max",
            "lowerFence",
            "upperFence",
            "outlierCount",
        }
        order_by = self._order_by(allowed, [("category", SortDirection.ASC)])
        q = quote_ident
        sql = (
            "WITH base AS ("
            f"SELECT {category_expression} AS {q('category')}, {value_expression} AS {q('__value')} "
            f"FROM {self._source_sql()}{where}"
            "), stats AS ("
            f"SELECT {q('category')}, count(*)::BIGINT AS {q('count')}, "
            f"quantile_cont({q('__value')}, 0.25) AS {q('q1')}, median({q('__value')}) AS {q('median')}, "
            f"quantile_cont({q('__value')}, 0.75) AS {q('q3')} FROM base GROUP BY 1"
            "), boxed AS ("
            f"SELECT s.{q('category')}, s.{q('count')}, "
            f"min(b.{q('__value')}) FILTER (WHERE b.{q('__value')} >= s.{q('q1')} - 1.5 * (s.{q('q3')} - s.{q('q1')})) AS {q('min')}, "
            f"s.{q('q1')}, s.{q('median')}, s.{q('q3')}, "
            f"max(b.{q('__value')}) FILTER (WHERE b.{q('__value')} <= s.{q('q3')} + 1.5 * (s.{q('q3')} - s.{q('q1')})) AS {q('max')}, "
            f"s.{q('q1')} - 1.5 * (s.{q('q3')} - s.{q('q1')}) AS {q('lowerFence')}, "
            f"s.{q('q3')} + 1.5 * (s.{q('q3')} - s.{q('q1')}) AS {q('upperFence')}, "
            f"sum(CASE WHEN b.{q('__value')} < s.{q('q1')} - 1.5 * (s.{q('q3')} - s.{q('q1')}) "
            f"OR b.{q('__value')} > s.{q('q3')} + 1.5 * (s.{q('q3')} - s.{q('q1')}) THEN 1 ELSE 0 END)::BIGINT AS {q('outlierCount')} "
            f"FROM stats s JOIN base b ON b.{q('category')} IS NOT DISTINCT FROM s.{q('category')} "
            f"GROUP BY s.{q('category')}, s.{q('count')}, s.{q('q1')}, s.{q('median')}, s.{q('q3')}"
            ") SELECT * FROM boxed"
            f"{order_by} LIMIT {self._row_limit() + 1}"
        )
        columns = [
            AnalyticsColumn(key="category", label=category_label, type=category_type)
        ]
        columns.extend(
            AnalyticsColumn(
                key=key,
                label=key,
                type="INTEGER" if key in {"count", "outlierCount"} else "NUMBER",
            )
            for key in [
                "count",
                "min",
                "q1",
                "median",
                "q3",
                "max",
                "lowerFence",
                "upperFence",
                "outlierCount",
            ]
        )
        return CompiledAnalyticsQuery(
            sql=sql,
            parameters=self._query_parameters(filter_parameters),
            columns=columns,
            row_limit=self._row_limit(),
        )

    def _compile_sankey(self) -> CompiledAnalyticsQuery:
        source = self._required(self.request.encoding.source, "encoding.source")
        target = self._required(self.request.encoding.target, "encoding.target")
        source_expression, _ = self._dimension(source, "encoding.source")
        target_expression, _ = self._dimension(target, "encoding.target")
        # Graph node identifiers share one namespace. Normalizing both sides also
        # makes self-link comparison deterministic when source columns differ in type.
        source_expression = f"CAST({source_expression} AS VARCHAR)"
        target_expression = f"CAST({target_expression} AS VARCHAR)"
        value_expression, _ = self._measure(self.request.encoding.value)
        extra: list[str] = []
        if self.request.options.null_policy == NullPolicy.EXCLUDE:
            extra.extend(
                [
                    f"{quote_ident(self._column(source, 'encoding.source').name)} IS NOT NULL",
                    f"{quote_ident(self._column(target, 'encoding.target').name)} IS NOT NULL",
                ]
            )
        if self.request.options.exclude_self_links:
            extra.append(f"{source_expression} IS DISTINCT FROM {target_expression}")
        if self._include_others():
            raise ValueError("includeOthers is not valid for SANKEY")
        where, filter_parameters = self._where(extra)
        order_by = self._order_by(
            {"source", "target", "value"},
            [
                ("value", SortDirection.DESC),
                ("source", SortDirection.ASC),
                ("target", SortDirection.ASC),
            ],
        )
        sql = (
            f"SELECT {source_expression} AS {quote_ident('source')}, "
            f"{target_expression} AS {quote_ident('target')}, {value_expression} AS {quote_ident('value')} "
            f"FROM {self._source_sql()}{where} GROUP BY 1, 2"
            f"{order_by} LIMIT {self._row_limit() + 1}"
        )
        return CompiledAnalyticsQuery(
            sql=sql,
            parameters=self._query_parameters(filter_parameters),
            columns=[
                AnalyticsColumn(
                    key="source",
                    label=self._field_label(source, "Source"),
                    type="STRING",
                ),
                AnalyticsColumn(
                    key="target",
                    label=self._field_label(target, "Target"),
                    type="STRING",
                ),
                AnalyticsColumn(
                    key="value",
                    label=self._field_label(self.request.encoding.value, "Count"),
                    type="NUMBER",
                ),
            ],
            row_limit=self._row_limit(),
        )

    def _compile_treemap(self) -> CompiledAnalyticsQuery:
        hierarchy = self.request.encoding.hierarchy
        if not hierarchy:
            raise ValueError("encoding.hierarchy requires between 1 and 3 fields")
        if self._include_others():
            raise ValueError(
                "includeOthers is not valid for TREEMAP because hierarchical folding is ambiguous"
            )
        select_parts: list[str] = []
        columns: list[AnalyticsColumn] = []
        extra: list[str] = []
        for index, field in enumerate(hierarchy):
            expression, column = self._dimension(field, f"encoding.hierarchy[{index}]")
            key = f"level{index}"
            select_parts.append(f"{expression} AS {quote_ident(key)}")
            columns.append(
                AnalyticsColumn(
                    key=key,
                    label=self._field_label(field, key),
                    type=self._dimension_type(column, field),
                )
            )
            condition = self._nonnull_condition(field)
            if condition:
                extra.append(condition)
        value_expression, _ = self._measure(self.request.encoding.value)
        select_parts.append(f"{value_expression} AS {quote_ident('value')}")
        columns.append(
            AnalyticsColumn(
                key="value",
                label=self._field_label(self.request.encoding.value, "Count"),
                type="NUMBER",
            )
        )
        where, filter_parameters = self._where(extra)
        allowed = {column.key for column in columns}
        default = [("value", SortDirection.DESC)] + [
            (f"level{index}", SortDirection.ASC) for index in range(len(hierarchy))
        ]
        order_by = self._order_by(allowed, default)
        group_positions = ", ".join(str(index + 1) for index in range(len(hierarchy)))
        sql = (
            f"SELECT {', '.join(select_parts)} FROM {self._source_sql()}{where} "
            f"GROUP BY {group_positions}{order_by} LIMIT {self._row_limit() + 1}"
        )
        return CompiledAnalyticsQuery(
            sql=sql,
            parameters=self._query_parameters(filter_parameters),
            columns=columns,
            row_limit=self._row_limit(),
        )


class AnalyticsDetailCompiler(AnalyticsCompiler):
    """동일한 안전 소스·필터 DSL로 페이지형 원본 상세 조회를 컴파일한다."""

    def __init__(
        self,
        request: AnalyticsDetailRequest,
        parquet_path: str | Path,
        schema: list[tuple[str, str]],
        source_row_limit: int | None = None,
    ):
        super().__init__(request, parquet_path, schema, source_row_limit)  # type: ignore[arg-type]
        self.request: AnalyticsDetailRequest = request

    def compile(self) -> CompiledDetailQuery:
        select_parts: list[str] = []
        columns: list[AnalyticsColumn] = []
        keys: set[str] = set()
        for index, field in enumerate(self.request.detail_columns):
            expression, column = self._dimension(field, f"detailColumns[{index}]")
            key = field.derived_field_id or field.column or f"column{index}"
            if key in keys:
                raise ValueError(f"duplicate detail column key: {key}")
            keys.add(key)
            select_parts.append(f"{expression} AS {quote_ident(key)}")
            columns.append(
                AnalyticsColumn(
                    key=key,
                    label=field.label
                    or self.calculated_labels.get(field.derived_field_id or "")
                    or key,
                    type=self._dimension_type(column, field),
                )
            )
        where, filter_parameters = self._where([])
        order_parts: list[str] = []
        for sort in self.request.sorts:
            if sort.field not in keys:
                raise ValueError(
                    f"detail sort field {sort.field} must be selected in detailColumns"
                )
            order_parts.append(
                f"{quote_ident(sort.field)} {sort.direction.value} NULLS LAST"
            )
        order_by = f" ORDER BY {', '.join(order_parts)}" if order_parts else ""
        sql = (
            f"SELECT {', '.join(select_parts)} FROM {self._source_sql()}{where}{order_by} "
            f"LIMIT {self.request.limit + 1} OFFSET {self.request.offset}"
        )
        return CompiledDetailQuery(
            sql=sql,
            parameters=self._query_parameters(filter_parameters),
            columns=columns,
            row_limit=self.request.limit,
        )
