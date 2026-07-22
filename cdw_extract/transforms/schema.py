"""변환 열의 논리 타입, 물리 이름, 계보와 스키마 해시를 관리한다."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace


_DECIMAL = re.compile(r"^DECIMAL\((\d+),(\d+)\)$", re.IGNORECASE)


@dataclass(frozen=True)
class ColumnSchema:
    """변환 전후 열의 안정적인 식별자, 물리 이름, 타입과 계보이다."""

    column_id: str
    physical_name: str
    label: str
    data_type: str
    nullable: bool = True
    source_column_ids: tuple[str, ...] = ()
    created_by_step_id: str | None = None
    operations: tuple[str, ...] = ()

    def json(self) -> dict:
        return {
            "columnId": self.column_id,
            "physicalName": self.physical_name,
            "label": self.label,
            "dataType": self.data_type,
            "nullable": self.nullable,
            "lineage": {
                "sourceColumnIds": list(self.source_column_ids),
                "createdByStepId": self.created_by_step_id,
                "operations": list(self.operations),
            },
        }


def safe_physical_name(column_id: str, pivot: bool = False) -> str:
    """사용자 라벨과 분리된 충돌 저항성 DuckDB 물리 열 이름을 만든다."""

    prefix = "p" if pivot else "c"
    return f"{prefix}_{hashlib.sha256(column_id.encode('utf-8')).hexdigest()[:12]}"


def normalize_type(value: str) -> str:
    """DuckDB·레거시 타입 이름을 파이프라인 논리 타입으로 정규화한다."""

    raw = str(value or "").upper().strip()
    match = _DECIMAL.match(raw)
    if match:
        precision, scale = int(match.group(1)), int(match.group(2))
        if precision < 1 or precision > 38 or scale < 0 or scale > precision:
            raise ValueError(f"invalid DECIMAL type: {value}")
        return f"DECIMAL({precision},{scale})"
    if raw in {"STRING", "BOOLEAN", "INT64", "DATE", "TIMESTAMP", "TIMESTAMP_TZ", "BINARY", "NULL"}:
        return raw
    aliases = {
        "VARCHAR": "STRING", "TEXT": "STRING", "CHAR": "STRING",
        "BOOL": "BOOLEAN", "BIGINT": "INT64", "INTEGER": "INT64", "INT": "INT64",
        "DOUBLE": "DECIMAL(38,6)", "FLOAT": "DECIMAL(38,6)", "REAL": "DECIMAL(38,6)",
        "TIMESTAMP WITH TIME ZONE": "TIMESTAMP_TZ", "TIMESTAMPTZ": "TIMESTAMP_TZ",
        "BLOB": "BINARY", "BYTEA": "BINARY",
    }
    if raw.startswith("VARCHAR") or raw.startswith("CHAR"):
        return "STRING"
    if raw.startswith("TIMESTAMP WITH"):
        return "TIMESTAMP_TZ"
    if raw.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if raw.startswith("DECIMAL") or raw.startswith("NUMERIC"):
        numbers = re.findall(r"\d+", raw)
        return normalize_type(f"DECIMAL({numbers[0] if numbers else 38},{numbers[1] if len(numbers) > 1 else 6})")
    return aliases.get(raw, "STRING")


def decimal_parts(value: str) -> tuple[int, int] | None:
    """DECIMAL 타입의 정밀도와 소수 자릿수를 반환한다."""

    match = _DECIMAL.match(normalize_type(value))
    return (int(match.group(1)), int(match.group(2))) if match else None


def common_type(values: list[str], explicit: str | None = None) -> str:
    """여러 열을 손실 없이 결합할 수 있는 공통 논리 타입을 결정한다."""

    if explicit:
        return normalize_type(explicit)
    types = {normalize_type(value) for value in values}
    types.discard("NULL")
    if not types:
        raise ValueError("NULL literals need an explicit target type")
    if len(types) == 1:
        return next(iter(types))
    if types <= {"INT64", *[value for value in types if decimal_parts(value)]}:
        decimals = [decimal_parts(value) or (19, 0) for value in types]
        scale = max(item[1] for item in decimals)
        integer = max(item[0] - item[1] for item in decimals)
        return f"DECIMAL({min(38, integer + scale)},{min(scale, max(0, 38 - integer))})"
    if types == {"DATE", "TIMESTAMP"}:
        return "TIMESTAMP"
    raise ValueError(f"columns do not have a safe common type: {', '.join(sorted(types))}")


def numeric_result_type(left: str, right: str, operator: str) -> tuple[str, str | None]:
    """산술 연산 결과 타입과 정밀도 조정 경고를 계산한다."""

    left_type, right_type = normalize_type(left), normalize_type(right)
    if left_type == right_type == "INT64" and operator in {"ADD", "SUBTRACT", "MULTIPLY"}:
        return "INT64", None
    if left_type != "INT64" and not decimal_parts(left_type):
        raise ValueError(f"PIPELINE_TYPE_MISMATCH: {left_type} is not numeric")
    if right_type != "INT64" and not decimal_parts(right_type):
        raise ValueError(f"PIPELINE_TYPE_MISMATCH: {right_type} is not numeric")
    p1, s1 = decimal_parts(left_type) or (19, 0)
    p2, s2 = decimal_parts(right_type) or (19, 0)
    if operator in {"ADD", "SUBTRACT"}:
        scale = max(s1, s2)
        precision = max(p1 - s1, p2 - s2) + scale + 1
    elif operator == "MULTIPLY":
        scale, precision = s1 + s2, p1 + p2 + 1
    elif operator == "DIVIDE":
        scale = max(6, s1 + p2 + 1)
        precision = p1 - s1 + s2 + scale
    else:
        raise ValueError(f"unsupported numeric operator: {operator}")
    if precision <= 38:
        return f"DECIMAL({precision},{scale})", None
    integer_digits = precision - scale
    if integer_digits > 38:
        raise ValueError("PIPELINE_DECIMAL_OVERFLOW: integer digits exceed precision 38")
    adjusted_scale = 38 - integer_digits
    return f"DECIMAL(38,{adjusted_scale})", "DECIMAL_SCALE_REDUCED"


def derived_column(step_id: str, output_id: str, label: str, data_type: str, sources: list[ColumnSchema], operation: str, *, pivot: bool = False, nullable: bool = True) -> ColumnSchema:
    """원본 계보를 합쳐 한 변환 단계의 파생 열 스키마를 만든다."""

    column_id = f"{'pivot' if pivot else 'out'}:{step_id}:{output_id}"
    source_ids: list[str] = []
    for source in sources:
        for item in source.source_column_ids or (source.column_id,):
            if item not in source_ids:
                source_ids.append(item)
    operations = tuple(dict.fromkeys(op for source in sources for op in source.operations)) + (operation,)
    return ColumnSchema(column_id, safe_physical_name(column_id, pivot), label, normalize_type(data_type), nullable, tuple(source_ids), step_id, operations)


def schema_hash(columns: list[ColumnSchema]) -> str:
    """열 순서와 무관한 정규 스키마 SHA-256 식별자를 만든다."""

    payload = [column.json() for column in sorted(columns, key=lambda item: item.column_id)]
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def relabel(column: ColumnSchema, label: str | None) -> ColumnSchema:
    """열 식별자와 계보를 유지한 채 표시 라벨만 바꾼 복사본을 반환한다."""

    return replace(column, label=str(label).strip() if label else column.label)
