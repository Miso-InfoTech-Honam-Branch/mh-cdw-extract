"""분석 질의, 차트, 산출물 API의 엄격한 데이터 계약을 정의한다."""

from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Annotated, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    """정의하지 않은 필드를 거부하고 별칭 입력을 허용하는 기본 모델이다."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ChartType(str, Enum):
    """지원하는 분석 차트 유형이다."""

    BAR = "BAR"
    PIE = "PIE"
    LINE = "LINE"
    SCATTER = "SCATTER"
    BOXPLOT = "BOXPLOT"
    FUNNEL = "FUNNEL"
    SANKEY = "SANKEY"
    TREEMAP = "TREEMAP"


class Aggregation(str, Enum):
    """분석 측정값에 적용할 집계 연산이다."""

    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    MEDIAN = "MEDIAN"


class TimeGrain(str, Enum):
    """날짜·시간 값을 묶을 시간 단위이다."""

    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    QUARTER = "QUARTER"
    YEAR = "YEAR"


class FilterOperator(str, Enum):
    """분석 필터에서 허용하는 비교 연산이다."""

    EQ = "EQ"
    NE = "NE"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"
    IN = "IN"
    CONTAINS = "CONTAINS"
    BETWEEN = "BETWEEN"
    IS_NULL = "IS_NULL"
    IS_NOT_NULL = "IS_NOT_NULL"


class SortDirection(str, Enum):
    """결과 정렬 방향이다."""

    ASC = "ASC"
    DESC = "DESC"


class NullPolicy(str, Enum):
    """차트 결과에서 NULL 범주를 다루는 정책이다."""

    EXCLUDE = "EXCLUDE"
    INCLUDE = "INCLUDE"


class ValueTransform(str, Enum):
    """집계 후 측정값에 적용할 후처리 방식이다."""

    NONE = "NONE"
    PERCENT_OF_TOTAL = "PERCENT_OF_TOTAL"
    RUNNING_TOTAL = "RUNNING_TOTAL"


class ExpressionOp(str, Enum):
    """계산 필드 표현식에서 지원하는 연산자이다."""

    COLUMN = "COLUMN"
    LITERAL = "LITERAL"
    ADD = "ADD"
    SUBTRACT = "SUBTRACT"
    MULTIPLY = "MULTIPLY"
    DIVIDE = "DIVIDE"
    EQ = "EQ"
    NE = "NE"
    GT = "GT"
    GTE = "GTE"
    LT = "LT"
    LTE = "LTE"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    COALESCE = "COALESCE"
    CONCAT = "CONCAT"
    DATE_DIFF = "DATE_DIFF"
    DATE_PART = "DATE_PART"
    PARSE_DATE = "PARSE_DATE"
    PARSE_NUMBER = "PARSE_NUMBER"
    CASE = "CASE"


class CalculatedDataType(str, Enum):
    """계산 필드가 선언할 수 있는 논리 자료형이다."""

    NUMBER = "NUMBER"
    TEXT = "TEXT"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"


JsonScalar = str | int | float | bool | date | datetime | None


class AnalyticsSource(StrictModel):
    """분석 대상 메타데이터 테이블 또는 사용자 데이터셋을 식별한다."""

    source_kind: Literal["USER_DATST", "MTDT_TBL"] = Field(alias="sourceKind")
    user_id: Annotated[str, Field(min_length=1, max_length=200)] | None = Field(default=None, alias="userId")
    user_dataset_id: Annotated[str, Field(min_length=1, max_length=200)] | None = Field(default=None, alias="userDatasetId")
    user_dataset_file_id: Annotated[str, Field(min_length=1, max_length=200)] | None = Field(default=None, alias="userDatasetFileId")
    metadata_id: Annotated[str, Field(min_length=1, max_length=200)] | None = Field(default=None, alias="metadataId")
    metadata_table_id: Annotated[str, Field(min_length=1, max_length=200)] | None = Field(default=None, alias="metadataTableId")

    @field_validator("user_id", "user_dataset_id", "user_dataset_file_id", "metadata_id", "metadata_table_id")
    @classmethod
    def reject_path_patterns(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if any(character in value for character in "*?[]{}"):
            raise ValueError("USER_DATST identifiers must not contain glob pattern characters")
        if value in {".", ".."} or "/" in value or "\\" in value or "\x00" in value:
            raise ValueError("USER_DATST identifiers must be safe path segments")
        return value

    @model_validator(mode="after")
    def require_source_identity(self) -> "AnalyticsSource":
        if self.source_kind == "USER_DATST" and not all((self.user_id, self.user_dataset_id, self.user_dataset_file_id)):
            raise ValueError("USER_DATST source identifiers are required")
        if self.source_kind == "MTDT_TBL" and not all((self.metadata_id, self.metadata_table_id)):
            raise ValueError("MTDT_TBL source identifiers are required")
        return self


class AnalyticsBin(StrictModel):
    """연속형 값을 일정 간격으로 구간화하는 규칙이다."""

    size: Annotated[float, Field(gt=0)]
    offset: float = 0

    @field_validator("offset", mode="before")
    @classmethod
    def default_null_offset(cls, value: object) -> object:
        return 0 if value is None else value

    @field_validator("size", "offset")
    @classmethod
    def finite_numbers(cls, value: float) -> float:
        import math

        if not math.isfinite(value):
            raise ValueError("bin values must be finite")
        return value


class AnalyticsField(StrictModel):
    """차트 인코딩에 사용할 원본·계산 필드와 변환을 정의한다."""

    column: Annotated[str, Field(min_length=1, max_length=255)] | None = None
    derived_field_id: Annotated[str, Field(min_length=1, max_length=100)] | None = Field(
        default=None,
        alias="derivedFieldId",
        validation_alias=AliasChoices("derivedFieldId", "calculatedFieldId"),
    )
    label: Annotated[str, Field(min_length=1, max_length=255)] | None = None
    aggregation: Aggregation | None = None
    time_grain: TimeGrain | None = Field(default=None, alias="timeGrain")
    bin: AnalyticsBin | None = None

    @model_validator(mode="after")
    def validate_expression(self) -> "AnalyticsField":
        references = int(bool(self.column)) + int(bool(self.derived_field_id))
        if references == 0 and self.aggregation != Aggregation.COUNT:
            raise ValueError("column or derivedFieldId is required unless aggregation is COUNT")
        if references > 1:
            raise ValueError("column and derivedFieldId are mutually exclusive")
        if self.time_grain and references == 0:
            raise ValueError("timeGrain requires a field reference")
        if self.time_grain and self.aggregation:
            raise ValueError("timeGrain and aggregation cannot be combined in one field")
        if self.bin and (self.time_grain or self.aggregation):
            raise ValueError("bin cannot be combined with timeGrain or aggregation")
        return self


class AnalyticsEncoding(StrictModel):
    """차트 유형별 시각 채널에 분석 필드를 배치한다."""

    category: AnalyticsField | None = None
    value: AnalyticsField | None = None
    series: AnalyticsField | None = None
    x: AnalyticsField | None = None
    y: AnalyticsField | None = None
    size: AnalyticsField | None = None
    group: AnalyticsField | None = None
    stage: AnalyticsField | None = None
    source: AnalyticsField | None = None
    target: AnalyticsField | None = None
    hierarchy: Annotated[list[AnalyticsField], Field(max_length=3)] = Field(default_factory=list)


class AnalyticsFilter(StrictModel):
    """필드와 연산자, 피연산자로 구성된 분석 필터이다."""

    column: Annotated[str, Field(min_length=1, max_length=255)] | None = None
    derived_field_id: Annotated[str, Field(min_length=1, max_length=100)] | None = Field(
        default=None,
        alias="derivedFieldId",
        validation_alias=AliasChoices("derivedFieldId", "calculatedFieldId"),
    )
    operator: FilterOperator
    time_grain: TimeGrain | None = Field(default=None, alias="timeGrain")
    bin: AnalyticsBin | None = None
    value: JsonScalar = None
    values: Annotated[list[JsonScalar], Field(max_length=1000)] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_operands(self) -> "AnalyticsFilter":
        if int(bool(self.column)) + int(bool(self.derived_field_id)) != 1:
            raise ValueError("exactly one of column or derivedFieldId is required")
        if self.time_grain and self.bin:
            raise ValueError("filter timeGrain and bin are mutually exclusive")
        no_operand = {FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL}
        if self.operator in no_operand:
            if self.value is not None or self.values:
                raise ValueError(f"{self.operator.value} does not accept value or values")
            return self
        if self.operator == FilterOperator.IN:
            if not self.values:
                raise ValueError("IN requires at least one values item")
            if self.value is not None:
                raise ValueError("IN accepts values, not value")
            return self
        if self.operator == FilterOperator.BETWEEN:
            if len(self.values) != 2:
                raise ValueError("BETWEEN requires exactly two values items")
            if self.value is not None:
                raise ValueError("BETWEEN accepts values, not value")
            return self
        if self.value is None:
            raise ValueError(f"{self.operator.value} requires value")
        if self.values:
            raise ValueError(f"{self.operator.value} accepts value, not values")
        return self


class AnalyticsSort(StrictModel):
    """출력 필드에 적용할 정렬 조건이다."""

    field: Annotated[str, Field(min_length=1, max_length=255)]
    direction: SortDirection = SortDirection.ASC


class CalculatedExpression(StrictModel):
    """검증 가능한 재귀형 계산 필드 표현식이다."""

    op: ExpressionOp
    column: Annotated[str, Field(min_length=1, max_length=255)] | None = None
    value: JsonScalar = None
    args: Annotated[list["CalculatedExpression"], Field(max_length=20)] = Field(default_factory=list)
    unit: TimeGrain | None = None
    separator: Annotated[str, Field(max_length=20)] | None = None
    format: Annotated[str, Field(min_length=1, max_length=40)] | None = None
    on_error: Literal["NULL"] | None = Field(default=None, alias="onError")
    branches: Annotated[list["CalculatedCaseBranch"], Field(max_length=20)] = Field(default_factory=list)
    else_expression: "CalculatedExpression | None" = Field(default=None, alias="else")

    @model_validator(mode="after")
    def validate_shape(self) -> "CalculatedExpression":
        self._validate_field_ownership()
        self._validate_operator_shape()
        self._validate_branch_ownership()
        return self

    def _validate_field_ownership(self) -> None:
        if self.op != ExpressionOp.COLUMN and self.column is not None:
            raise ValueError("column is valid only for COLUMN")
        if self.op != ExpressionOp.LITERAL and self.value is not None:
            raise ValueError("value is valid only for LITERAL")
        if (
            self.op not in {ExpressionOp.DATE_DIFF, ExpressionOp.DATE_PART}
            and self.unit is not None
        ):
            raise ValueError("unit is valid only for DATE_DIFF or DATE_PART")
        if self.op != ExpressionOp.CONCAT and self.separator:
            raise ValueError("separator is valid only for CONCAT")
        if self.op not in {ExpressionOp.PARSE_DATE, ExpressionOp.PARSE_NUMBER} and (
            self.format or self.on_error
        ):
            raise ValueError("format and onError are valid only for parse expressions")

    def _validate_operator_shape(self) -> None:
        validators = {
            ExpressionOp.COLUMN: self._validate_column_shape,
            ExpressionOp.LITERAL: self._validate_literal_shape,
            ExpressionOp.ADD: self._validate_binary_shape,
            ExpressionOp.SUBTRACT: self._validate_binary_shape,
            ExpressionOp.MULTIPLY: self._validate_binary_shape,
            ExpressionOp.DIVIDE: self._validate_binary_shape,
            ExpressionOp.EQ: self._validate_binary_shape,
            ExpressionOp.NE: self._validate_binary_shape,
            ExpressionOp.GT: self._validate_binary_shape,
            ExpressionOp.GTE: self._validate_binary_shape,
            ExpressionOp.LT: self._validate_binary_shape,
            ExpressionOp.LTE: self._validate_binary_shape,
            ExpressionOp.AND: self._validate_binary_shape,
            ExpressionOp.OR: self._validate_binary_shape,
            ExpressionOp.NOT: self._validate_unary_shape,
            ExpressionOp.COALESCE: self._validate_coalesce_shape,
            ExpressionOp.CONCAT: self._validate_concat_shape,
            ExpressionOp.DATE_DIFF: self._validate_date_diff_shape,
            ExpressionOp.DATE_PART: self._validate_date_part_shape,
            ExpressionOp.PARSE_DATE: self._validate_parse_date_shape,
            ExpressionOp.PARSE_NUMBER: self._validate_parse_number_shape,
            ExpressionOp.CASE: self._validate_case_shape,
        }
        validators[self.op]()

    def _validate_column_shape(self) -> None:
        if (
            not self.column
            or self.args
            or self.branches
            or self.else_expression is not None
        ):
            raise ValueError("COLUMN requires only column")

    def _validate_literal_shape(self) -> None:
        if (
            "value" not in self.model_fields_set
            or self.column
            or self.args
            or self.branches
        ):
            raise ValueError("LITERAL requires only value")

    def _validate_binary_shape(self) -> None:
        if len(self.args) != 2:
            raise ValueError(f"{self.op.value} requires exactly two args")

    def _validate_unary_shape(self) -> None:
        if len(self.args) != 1:
            raise ValueError(f"{self.op.value} requires exactly one arg")

    def _validate_coalesce_shape(self) -> None:
        if not 2 <= len(self.args) <= 20:
            raise ValueError("COALESCE requires between two and twenty args")

    def _validate_concat_shape(self) -> None:
        if not 1 <= len(self.args) <= 20:
            raise ValueError("CONCAT requires between one and twenty args")

    def _validate_date_diff_shape(self) -> None:
        self._validate_binary_shape()
        if self.unit is None:
            raise ValueError("DATE_DIFF requires unit")

    def _validate_date_part_shape(self) -> None:
        self._validate_unary_shape()
        if self.unit is None:
            raise ValueError("DATE_PART requires unit")

    def _validate_parse_date_shape(self) -> None:
        self._validate_parse_shape(
            {
                "YYMMDD",
                "YYYYMMDD",
                "YYYY-MM-DD",
                "YYYY/MM/DD",
                "YYYYMMDDHH24MISS",
                "YYYY-MM-DD HH24:MI:SS",
            },
            "PARSE_DATE requires a supported format",
        )

    def _validate_parse_number_shape(self) -> None:
        self._validate_parse_shape(
            {"PLAIN", "THOUSANDS_COMMA"},
            "PARSE_NUMBER requires a supported format",
        )

    def _validate_parse_shape(self, supported_formats: set[str], message: str) -> None:
        self._validate_unary_shape()
        if self.format not in supported_formats:
            raise ValueError(message)
        if self.on_error != "NULL":
            raise ValueError("parse expressions currently require onError=NULL")

    def _validate_case_shape(self) -> None:
        if not self.branches or self.else_expression is None or self.args:
            raise ValueError("CASE requires branches and else")

    def _validate_branch_ownership(self) -> None:
        if self.op != ExpressionOp.CASE and (self.branches or self.else_expression is not None):
            raise ValueError("branches and else are valid only for CASE")


class CalculatedCaseBranch(StrictModel):
    """CASE 계산식의 조건과 결과 한 쌍이다."""

    when: CalculatedExpression
    then: CalculatedExpression


class AnalyticsCalculatedField(StrictModel):
    """요청 안에서 재사용할 계산 필드를 정의한다."""

    id: Annotated[str, Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,99}$")]
    name: Annotated[str, Field(min_length=1, max_length=255)]
    data_type: CalculatedDataType | None = Field(default=None, alias="dataType")
    formula: CalculatedExpression


class AnalyticsTopN(StrictModel):
    """상위 N개 범주와 나머지 묶음 처리 옵션이다."""

    enabled: bool = False
    count: Annotated[int, Field(ge=1, le=2000)] = 10
    by: Literal["value"] = "value"
    direction: SortDirection = SortDirection.DESC
    include_others: bool = Field(default=False, alias="includeOthers")


class AnalyticsDrilldown(StrictModel):
    """계층형 탐색 필드와 현재 드릴다운 수준을 정의한다."""

    fields: Annotated[list[AnalyticsField], Field(min_length=1, max_length=8)]
    level: Annotated[int, Field(ge=0, le=7)] = 0

    @model_validator(mode="after")
    def validate_level(self) -> "AnalyticsDrilldown":
        if self.level >= len(self.fields):
            raise ValueError("drilldown.level must point to an item in drilldown.fields")
        return self


class ComparisonMode(str, Enum):
    """계열 또는 이전 기간 비교 방식이다."""

    SERIES = "SERIES"
    PREVIOUS_PERIOD = "PREVIOUS_PERIOD"


class AnalyticsComparison(StrictModel):
    """차트 측정값의 비교 기준과 기간 오프셋을 정의한다."""

    enabled: bool = False
    mode: ComparisonMode = ComparisonMode.SERIES
    field: AnalyticsField | None = None
    period_unit: TimeGrain | None = Field(default=None, alias="periodUnit")
    offset: Annotated[int, Field(ge=-100, le=-1)] = -1

    @model_validator(mode="after")
    def validate_mode(self) -> "AnalyticsComparison":
        if self.enabled and self.mode == ComparisonMode.SERIES and self.field is None:
            raise ValueError("SERIES comparison requires field")
        if self.enabled and self.mode == ComparisonMode.PREVIOUS_PERIOD and self.period_unit is None:
            raise ValueError("PREVIOUS_PERIOD comparison requires periodUnit")
        return self


class ReferenceLineType(str, Enum):
    """기준선 값을 계산하는 방식이다."""

    CONSTANT = "CONSTANT"
    AVERAGE = "AVERAGE"
    TARGET = "TARGET"


class AnalyticsReferenceLine(StrictModel):
    """차트에 표시할 고정값 또는 통계 기준선 요청이다."""

    id: Annotated[str, Field(min_length=1, max_length=100)]
    type: ReferenceLineType
    value: float | None = None
    label: Annotated[str, Field(min_length=1, max_length=255)] | None = None
    color: Annotated[str, Field(min_length=1, max_length=50)] | None = None

    @model_validator(mode="after")
    def validate_value(self) -> "AnalyticsReferenceLine":
        if self.type in {ReferenceLineType.CONSTANT, ReferenceLineType.TARGET} and self.value is None:
            raise ValueError(f"{self.type.value} reference line requires value")
        if self.value is not None and not (-1e308 < self.value < 1e308):
            raise ValueError("reference line value must be finite")
        return self


class AnalyticsOptions(StrictModel):
    """분석 실행의 제한, 자원, NULL 처리 옵션이다."""

    null_policy: NullPolicy = Field(default=NullPolicy.EXCLUDE, alias="nullPolicy")
    include_others: bool = Field(default=False, alias="includeOthers")
    others_label: Annotated[str, Field(min_length=1, max_length=100)] = Field(default="Others", alias="othersLabel")
    exclude_self_links: bool = Field(default=True, alias="excludeSelfLinks")
    scatter_sample_size: Annotated[int, Field(ge=1, le=5000)] = Field(default=1000, alias="scatterSampleSize")
    random_seed: Annotated[int, Field(ge=0, le=2_147_483_646)] = Field(default=42, alias="randomSeed")
    memory_limit_mb: Annotated[int, Field(ge=64, le=2048)] = Field(default=256, alias="memoryLimitMb")
    threads: Annotated[int, Field(ge=1, le=8)] = 2
    timeout_ms: Annotated[int, Field(ge=100, le=30000)] = Field(default=5000, alias="timeoutMs")
    value_transform: ValueTransform = Field(default=ValueTransform.NONE, alias="valueTransform")


class AnalyticsQueryRequest(StrictModel):
    """차트 분석 질의의 전체 입력 계약이다."""

    schema_version: Literal[1] = Field(alias="schemaVersion")
    request_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="requestId")
    source: AnalyticsSource
    chart_type: ChartType = Field(alias="chartType")
    encoding: AnalyticsEncoding
    calculated_fields: Annotated[list[AnalyticsCalculatedField], Field(max_length=50)] = Field(
        default_factory=list, alias="calculatedFields"
    )
    filters: Annotated[list[AnalyticsFilter], Field(max_length=50)] = Field(default_factory=list)
    global_filters: Annotated[list[AnalyticsFilter], Field(max_length=50)] = Field(default_factory=list, alias="globalFilters")
    chart_filters: Annotated[list[AnalyticsFilter], Field(max_length=50)] = Field(default_factory=list, alias="chartFilters")
    interaction_filters: Annotated[list[AnalyticsFilter], Field(max_length=50)] = Field(
        default_factory=list, alias="interactionFilters"
    )
    sorts: Annotated[list[AnalyticsSort], Field(max_length=5)] = Field(default_factory=list)
    limit: Annotated[int, Field(ge=1, le=2000)] = 100
    top_n: AnalyticsTopN | None = Field(default=None, alias="topN")
    drilldown: AnalyticsDrilldown | None = None
    comparison: AnalyticsComparison | None = None
    reference_lines: Annotated[list[AnalyticsReferenceLine], Field(max_length=20)] = Field(
        default_factory=list, alias="referenceLines"
    )
    detail_columns: Annotated[list[AnalyticsField], Field(max_length=100)] = Field(default_factory=list, alias="detailColumns")
    options: AnalyticsOptions = Field(default_factory=AnalyticsOptions)

    @property
    def all_filters(self) -> list[AnalyticsFilter]:
        return [*self.filters, *self.global_filters, *self.chart_filters, *self.interaction_filters]

    @model_validator(mode="after")
    def validate_unique_calculated_fields(self) -> "AnalyticsQueryRequest":
        identifiers = [item.id for item in self.calculated_fields]
        if len(identifiers) != len(set(identifiers)):
            raise ValueError("calculatedFields ids must be unique")
        if len(self.all_filters) > 100:
            raise ValueError("combined filters must not exceed 100")
        return self


class AnalyticsColumn(StrictModel):
    """분석 결과 열의 이름과 DuckDB 자료형 메타데이터이다."""

    key: str
    label: str
    type: str


class ResolvedReferenceLine(StrictModel):
    """실제 값까지 계산된 기준선 결과이다."""

    id: str
    type: ReferenceLineType
    value: float
    label: str | None = None
    color: str | None = None


class AnalyticsQueryResponse(StrictModel):
    """분석 행, 열, 기준선, 경고와 실행 메타데이터를 반환한다."""

    request_id: str = Field(alias="requestId")
    chart_type: ChartType = Field(alias="chartType")
    source_version: str = Field(alias="sourceVersion")
    elapsed_ms: int = Field(alias="elapsedMs")
    row_count: int = Field(alias="rowCount")
    truncated: bool
    warnings: list[str]
    columns: list[AnalyticsColumn]
    rows: list[dict[str, object]]
    reference_lines: list[ResolvedReferenceLine] = Field(default_factory=list, alias="referenceLines")
    metadata: dict[str, object] = Field(default_factory=dict)


class AnalyticsDetailRequest(StrictModel):
    """차트 선택 지점의 원본 상세 행 조회 요청이다."""

    schema_version: Literal[1] = Field(alias="schemaVersion")
    request_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="requestId")
    source: AnalyticsSource
    calculated_fields: Annotated[list[AnalyticsCalculatedField], Field(max_length=50)] = Field(
        default_factory=list, alias="calculatedFields"
    )
    filters: Annotated[list[AnalyticsFilter], Field(max_length=50)] = Field(default_factory=list)
    global_filters: Annotated[list[AnalyticsFilter], Field(max_length=50)] = Field(default_factory=list, alias="globalFilters")
    chart_filters: Annotated[list[AnalyticsFilter], Field(max_length=50)] = Field(default_factory=list, alias="chartFilters")
    interaction_filters: Annotated[list[AnalyticsFilter], Field(max_length=50)] = Field(
        default_factory=list, alias="interactionFilters"
    )
    detail_columns: Annotated[list[AnalyticsField], Field(min_length=1, max_length=100)] = Field(alias="detailColumns")
    sorts: Annotated[list[AnalyticsSort], Field(max_length=5)] = Field(default_factory=list)
    offset: Annotated[int, Field(ge=0, le=1_000_000)] = 0
    limit: Annotated[int, Field(ge=1, le=500)] = 100
    options: AnalyticsOptions = Field(default_factory=AnalyticsOptions)

    @property
    def all_filters(self) -> list[AnalyticsFilter]:
        return [*self.filters, *self.global_filters, *self.chart_filters, *self.interaction_filters]

    @model_validator(mode="after")
    def validate_request(self) -> "AnalyticsDetailRequest":
        ids = [item.id for item in self.calculated_fields]
        if len(ids) != len(set(ids)):
            raise ValueError("calculatedFields ids must be unique")
        if len(self.all_filters) > 100:
            raise ValueError("combined filters must not exceed 100")
        for field in self.detail_columns:
            if field.aggregation is not None:
                raise ValueError("detailColumns must not use aggregation")
        return self


class AnalyticsDetailResponse(StrictModel):
    """상세 행과 페이지 탐색 정보를 담는 응답이다."""

    request_id: str = Field(alias="requestId")
    source_version: str = Field(alias="sourceVersion")
    elapsed_ms: int = Field(alias="elapsedMs")
    offset: int
    limit: int
    row_count: int = Field(alias="rowCount")
    has_more: bool = Field(alias="hasMore")
    columns: list[AnalyticsColumn]
    rows: list[dict[str, object]]
    warnings: list[str] = Field(default_factory=list)


class ArtifactFormat(str, Enum):
    """분석 산출물로 내보낼 수 있는 파일 형식이다."""

    PNG = "PNG"
    PDF = "PDF"
    XLSX = "XLSX"


class ArtifactCallback(StrictModel):
    """분석 산출물 완료 콜백의 URL과 인증 헤더이다."""

    url: Annotated[str, Field(min_length=1, max_length=2048)]
    headers: dict[str, Annotated[str, Field(max_length=4096)]] = Field(default_factory=dict)
    timeout_seconds: Annotated[float, Field(gt=0, le=60)] = Field(default=10, alias="timeoutSeconds")


class AnalyticsArtifactRequest(StrictModel):
    """분석 결과 파일 생성 작업의 입력 계약이다."""

    schema_version: Literal[1] = Field(alias="schemaVersion")
    job_id: Annotated[str, Field(min_length=1, max_length=100)] = Field(alias="jobId")
    request_id: Annotated[str, Field(min_length=1, max_length=200)] | None = Field(default=None, alias="requestId")
    analysis_artifact_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="analysisArtifactId")
    analysis_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="analysisId")
    user_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="userId")
    name: Annotated[str, Field(min_length=1, max_length=255)]
    format: ArtifactFormat
    spec: dict[str, object]
    callback: ArtifactCallback | None = None


class AnalyticsArtifactAccepted(StrictModel):
    """비동기 분석 산출물 작업 수락 응답이다."""

    job_id: str = Field(alias="jobId")
    job_type: Literal["ANALYSIS_ARTIFACT"] = Field(default="ANALYSIS_ARTIFACT", alias="jobType")
    request_id: str = Field(alias="requestId")
    analysis_artifact_id: str = Field(alias="analysisArtifactId")
    analysis_id: str = Field(alias="analysisId")
    user_id: str = Field(alias="userId")
    state: Literal["ACCEPTED"] = "ACCEPTED"


CalculatedCaseBranch.model_rebuild()
CalculatedExpression.model_rebuild()
