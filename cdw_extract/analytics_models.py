from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Annotated, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class ChartType(str, Enum):
    BAR = "BAR"
    PIE = "PIE"
    LINE = "LINE"
    SCATTER = "SCATTER"
    BOXPLOT = "BOXPLOT"
    FUNNEL = "FUNNEL"
    SANKEY = "SANKEY"
    TREEMAP = "TREEMAP"


class Aggregation(str, Enum):
    COUNT = "COUNT"
    COUNT_DISTINCT = "COUNT_DISTINCT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"
    MEDIAN = "MEDIAN"


class TimeGrain(str, Enum):
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    QUARTER = "QUARTER"
    YEAR = "YEAR"


class FilterOperator(str, Enum):
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
    ASC = "ASC"
    DESC = "DESC"


class NullPolicy(str, Enum):
    EXCLUDE = "EXCLUDE"
    INCLUDE = "INCLUDE"


class ValueTransform(str, Enum):
    NONE = "NONE"
    PERCENT_OF_TOTAL = "PERCENT_OF_TOTAL"
    RUNNING_TOTAL = "RUNNING_TOTAL"


class ExpressionOp(str, Enum):
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
    CASE = "CASE"


class CalculatedDataType(str, Enum):
    NUMBER = "NUMBER"
    TEXT = "TEXT"
    DATE = "DATE"
    BOOLEAN = "BOOLEAN"


JsonScalar = str | int | float | bool | date | datetime | None


class AnalyticsSource(StrictModel):
    source_kind: Literal["USER_DATST"] = Field(alias="sourceKind")
    user_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="userId")
    user_dataset_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="userDatasetId")
    user_dataset_file_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="userDatasetFileId")

    @field_validator("user_id", "user_dataset_id", "user_dataset_file_id")
    @classmethod
    def reject_path_patterns(cls, value: str) -> str:
        if any(character in value for character in "*?[]{}"):
            raise ValueError("USER_DATST identifiers must not contain glob pattern characters")
        if value in {".", ".."} or "/" in value or "\\" in value or "\x00" in value:
            raise ValueError("USER_DATST identifiers must be safe path segments")
        return value


class AnalyticsBin(StrictModel):
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
    field: Annotated[str, Field(min_length=1, max_length=255)]
    direction: SortDirection = SortDirection.ASC


class CalculatedExpression(StrictModel):
    op: ExpressionOp
    column: Annotated[str, Field(min_length=1, max_length=255)] | None = None
    value: JsonScalar = None
    args: Annotated[list["CalculatedExpression"], Field(max_length=20)] = Field(default_factory=list)
    unit: TimeGrain | None = None
    separator: Annotated[str, Field(max_length=20)] | None = None
    branches: Annotated[list["CalculatedCaseBranch"], Field(max_length=20)] = Field(default_factory=list)
    else_expression: "CalculatedExpression | None" = Field(default=None, alias="else")

    @model_validator(mode="after")
    def validate_shape(self) -> "CalculatedExpression":
        if self.op != ExpressionOp.COLUMN and self.column is not None:
            raise ValueError("column is valid only for COLUMN")
        if self.op != ExpressionOp.LITERAL and self.value is not None:
            raise ValueError("value is valid only for LITERAL")
        if self.op not in {ExpressionOp.DATE_DIFF, ExpressionOp.DATE_PART} and self.unit is not None:
            raise ValueError("unit is valid only for DATE_DIFF or DATE_PART")
        if self.op != ExpressionOp.CONCAT and self.separator:
            raise ValueError("separator is valid only for CONCAT")
        binary = {
            ExpressionOp.ADD, ExpressionOp.SUBTRACT, ExpressionOp.MULTIPLY, ExpressionOp.DIVIDE,
            ExpressionOp.EQ, ExpressionOp.NE, ExpressionOp.GT, ExpressionOp.GTE, ExpressionOp.LT,
            ExpressionOp.LTE, ExpressionOp.AND, ExpressionOp.OR, ExpressionOp.DATE_DIFF,
        }
        if self.op == ExpressionOp.COLUMN:
            if not self.column or self.args or self.branches or self.else_expression is not None:
                raise ValueError("COLUMN requires only column")
        elif self.op == ExpressionOp.LITERAL:
            if "value" not in self.model_fields_set or self.column or self.args or self.branches:
                raise ValueError("LITERAL requires only value")
        elif self.op in binary and len(self.args) != 2:
            raise ValueError(f"{self.op.value} requires exactly two args")
        elif self.op in {ExpressionOp.NOT, ExpressionOp.DATE_PART} and len(self.args) != 1:
            raise ValueError(f"{self.op.value} requires exactly one arg")
        elif self.op == ExpressionOp.COALESCE and not (2 <= len(self.args) <= 20):
            raise ValueError("COALESCE requires between two and twenty args")
        elif self.op == ExpressionOp.CONCAT and not (1 <= len(self.args) <= 20):
            raise ValueError("CONCAT requires between one and twenty args")
        elif self.op == ExpressionOp.DATE_DIFF and self.unit is None:
            raise ValueError("DATE_DIFF requires unit")
        elif self.op == ExpressionOp.DATE_PART and self.unit is None:
            raise ValueError("DATE_PART requires unit")
        elif self.op == ExpressionOp.CASE:
            if not self.branches or self.else_expression is None or self.args:
                raise ValueError("CASE requires branches and else")
        if self.op != ExpressionOp.CASE and (self.branches or self.else_expression is not None):
            raise ValueError("branches and else are valid only for CASE")
        return self


class CalculatedCaseBranch(StrictModel):
    when: CalculatedExpression
    then: CalculatedExpression


class AnalyticsCalculatedField(StrictModel):
    id: Annotated[str, Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,99}$")]
    name: Annotated[str, Field(min_length=1, max_length=255)]
    data_type: CalculatedDataType | None = Field(default=None, alias="dataType")
    formula: CalculatedExpression


class AnalyticsTopN(StrictModel):
    enabled: bool = False
    count: Annotated[int, Field(ge=1, le=2000)] = 10
    by: Literal["value"] = "value"
    direction: SortDirection = SortDirection.DESC
    include_others: bool = Field(default=False, alias="includeOthers")


class AnalyticsDrilldown(StrictModel):
    fields: Annotated[list[AnalyticsField], Field(min_length=1, max_length=8)]
    level: Annotated[int, Field(ge=0, le=7)] = 0

    @model_validator(mode="after")
    def validate_level(self) -> "AnalyticsDrilldown":
        if self.level >= len(self.fields):
            raise ValueError("drilldown.level must point to an item in drilldown.fields")
        return self


class ComparisonMode(str, Enum):
    SERIES = "SERIES"
    PREVIOUS_PERIOD = "PREVIOUS_PERIOD"


class AnalyticsComparison(StrictModel):
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
    CONSTANT = "CONSTANT"
    AVERAGE = "AVERAGE"
    TARGET = "TARGET"


class AnalyticsReferenceLine(StrictModel):
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
    key: str
    label: str
    type: str


class ResolvedReferenceLine(StrictModel):
    id: str
    type: ReferenceLineType
    value: float
    label: str | None = None
    color: str | None = None


class AnalyticsQueryResponse(StrictModel):
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
    PNG = "PNG"
    PDF = "PDF"
    XLSX = "XLSX"


class ArtifactCallback(StrictModel):
    url: Annotated[str, Field(min_length=1, max_length=2048)]
    headers: dict[str, Annotated[str, Field(max_length=4096)]] = Field(default_factory=dict)
    timeout_seconds: Annotated[float, Field(gt=0, le=60)] = Field(default=10, alias="timeoutSeconds")


class AnalyticsArtifactRequest(StrictModel):
    schema_version: Literal[1] = Field(alias="schemaVersion")
    job_id: Annotated[str, Field(min_length=1, max_length=100)] = Field(alias="jobId")
    request_id: Annotated[str, Field(min_length=1, max_length=200)] | None = Field(default=None, alias="requestId")
    artifact_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="artifactId")
    analysis_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="analysisId")
    user_id: Annotated[str, Field(min_length=1, max_length=200)] = Field(alias="userId")
    name: Annotated[str, Field(min_length=1, max_length=255)]
    format: ArtifactFormat
    spec: dict[str, object]
    callback: ArtifactCallback | None = None


class AnalyticsArtifactAccepted(StrictModel):
    job_id: str = Field(alias="jobId")
    job_type: Literal["ANALYSIS_ARTIFACT"] = Field(default="ANALYSIS_ARTIFACT", alias="jobType")
    request_id: str = Field(alias="requestId")
    artifact_id: str = Field(alias="artifactId")
    analysis_id: str = Field(alias="analysisId")
    user_id: str = Field(alias="userId")
    state: Literal["ACCEPTED"] = "ACCEPTED"


CalculatedCaseBranch.model_rebuild()
CalculatedExpression.model_rebuild()
