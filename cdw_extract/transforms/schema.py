from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace


_DECIMAL = re.compile(r"^DECIMAL\((\d+),(\d+)\)$", re.IGNORECASE)


@dataclass(frozen=True)
class ColumnSchema:
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
    prefix = "p" if pivot else "c"
    return f"{prefix}_{hashlib.sha256(column_id.encode('utf-8')).hexdigest()[:12]}"


def normalize_type(value: str) -> str:
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
    match = _DECIMAL.match(normalize_type(value))
    return (int(match.group(1)), int(match.group(2))) if match else None


def common_type(values: list[str], explicit: str | None = None) -> str:
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


def derived_column(step_id: str, output_id: str, label: str, data_type: str, sources: list[ColumnSchema], operation: str, *, pivot: bool = False, nullable: bool = True) -> ColumnSchema:
    column_id = f"{'pivot' if pivot else 'out'}:{step_id}:{output_id}"
    source_ids: list[str] = []
    for source in sources:
        for item in source.source_column_ids or (source.column_id,):
            if item not in source_ids:
                source_ids.append(item)
    operations = tuple(dict.fromkeys(op for source in sources for op in source.operations)) + (operation,)
    return ColumnSchema(column_id, safe_physical_name(column_id, pivot), label, normalize_type(data_type), nullable, tuple(source_ids), step_id, operations)


def schema_hash(columns: list[ColumnSchema]) -> str:
    payload = [column.json() for column in sorted(columns, key=lambda item: item.column_id)]
    canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def relabel(column: ColumnSchema, label: str | None) -> ColumnSchema:
    return replace(column, label=str(label).strip() if label else column.label)
