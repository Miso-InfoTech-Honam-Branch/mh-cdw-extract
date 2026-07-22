from __future__ import annotations

import copy
import hashlib
import math
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

from ..duck import connect, json_safe_rows, quote_ident
from ..errors import (
    PipelineCompilerVersionMismatch,
    PipelineSnapshotMismatch,
    PipelineSourceSchemaChanged,
)
from ..query import SourceResolver, projection_with_mappings_sql, source_from_sql, source_with_code_mappings_sql, single_table_alias
from .compiler import MAX_PIVOT_VALUES, canonical_hash, compile_pipeline, inspect_source_schema


COMPILER_VERSION = "1"


def _source_sql(connection_id: str, data_root: str | Path, request: dict) -> str:
    resolver=SourceResolver(connection_id,data_root)
    default_alias=single_table_alias(request) if str(request.get("sourceType") or "").lower()=="table" else None
    source=source_from_sql(resolver,request)
    joined=source_with_code_mappings_sql(resolver,request,source,default_alias)
    projection=projection_with_mappings_sql(request.get("columns") or [],request.get("codeMappings") or [],default_alias)
    return f"(SELECT {projection} FROM {joined}) AS __pipeline_source"


def _pivot_value_id(value: object) -> str:
    digest=hashlib.sha256(repr(value).encode("utf-8")).hexdigest()[:16]
    return f"auto-{digest}"


def _pivot_wire_value(value: object) -> str | int | bool:
    """Normalize a DuckDB scalar into a lossless JSON contract value."""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(
                "PIVOT_VALUE_NOT_JSON_SERIALIZABLE: automatic PIVOT values must be finite"
            )
        # Keep binary floating-point values out of the cross-language JSON hash.
        # DuckDB will cast this round-trip representation back to the typed pivot column.
        return repr(value)
    if isinstance(value, (str, int)):
        return value
    raise ValueError(
        "PIVOT_VALUE_NOT_JSON_SERIALIZABLE: automatic PIVOT values must be JSON scalars"
    )


def _resolve_automatic_pivot_values(
    connection,
    source_sql: str,
    source_schema: list,
    pipeline: dict,
    columns: list[dict] | None = None,
    code_mappings: list[dict] | None = None,
) -> tuple[dict, str]:
    """Return an isolated, fully resolved pipeline and its canonical hash."""
    resolved=copy.deepcopy(pipeline)
    steps=resolved.get("steps") or []
    for index, step in enumerate(steps):
        if not step.get("enabled",True) or str(step.get("type") or "").upper()!="PIVOT":
            continue
        config=step.get("config") or {}
        if config.get("values"):
            continue
        pivot_column_id=config.get("pivotColumnId")
        prefix={**resolved,"steps":[*steps[:index],{"stepId":"__pivot_discovery_output","type":"OUTPUT","enabled":True,"config":{}}]}
        compiled_prefix=compile_pipeline(f"SELECT * FROM {source_sql}",source_schema,prefix)
        pivot_column=next((item for item in compiled_prefix.output_schema if item.column_id==pivot_column_id),None)
        if pivot_column is None:
            raise ValueError("PIVOT_COLUMN_NOT_FOUND")
        code_name_physical=None
        for mapping in code_mappings or []:
            source_matches=any(
                (column.get("alias") or column.get("name"))==pivot_column.physical_name
                and column.get("name")==mapping.get("sourceColumn")
                and (column.get("table") or None)==(mapping.get("sourceTable") or None)
                for column in columns or []
            )
            if source_matches:
                code_name_physical=mapping.get("outputColumn")
                break
        if code_name_physical:
            discovery_sql=(
                f"SELECT {quote_ident(pivot_column.physical_name)} AS __pivot_value, "
                f"max({quote_ident(code_name_physical)}) AS __pivot_label FROM ({compiled_prefix.sql}) AS __pivot_source "
                f"WHERE {quote_ident(pivot_column.physical_name)} IS NOT NULL GROUP BY 1 ORDER BY 1 LIMIT {MAX_PIVOT_VALUES + 1}"
            )
        else:
            discovery_sql=(
                f"SELECT DISTINCT {quote_ident(pivot_column.physical_name)} AS __pivot_value FROM ({compiled_prefix.sql}) AS __pivot_source "
                f"WHERE {quote_ident(pivot_column.physical_name)} IS NOT NULL ORDER BY 1 LIMIT {MAX_PIVOT_VALUES + 1}"
            )
        result=connection.execute(
            discovery_sql,
            compiled_prefix.parameters,
        ).fetchall()
        if len(result)>MAX_PIVOT_VALUES:
            raise ValueError(f"PIVOT_TOO_MANY_VALUES: 고유값이 {MAX_PIVOT_VALUES}개를 초과하여 가로로 펼칠 수 없습니다.")
        if not result:
            raise ValueError("PIVOT_NO_VALUES: 가로로 펼칠 값이 없습니다.")
        resolved_values=[]
        for order,row in enumerate(result,1):
            wire_value=_pivot_wire_value(row[0])
            resolved_values.append({
                "valueId":_pivot_value_id(wire_value),
                "value":wire_value,
                "label":str(row[1]) if len(row)>1 and row[1] is not None and str(row[1]).strip() else str(wire_value),
                "sort":order,
            })
        config["values"]=resolved_values
        step["config"]=config
    return resolved,canonical_hash(resolved)


def compile_pipeline_request(connection_id: str, request: dict, data_root: str | Path, connection):
    expected_compiler = request.get("expectedCompilerVersion")
    if expected_compiler is not None and str(expected_compiler) != COMPILER_VERSION:
        raise PipelineCompilerVersionMismatch(
            "PIPELINE_COMPILER_VERSION_MISMATCH: the saved pipeline requires compiler "
            f"{expected_compiler}, but this worker provides {COMPILER_VERSION}."
        )
    source_sql=_source_sql(connection_id,data_root,request)
    described=connection.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
    source_schema=inspect_source_schema(described,request.get("sourceColumns"))
    pipeline,resolved_pipeline_hash=_resolve_automatic_pivot_values(
        connection,
        source_sql,
        source_schema,
        request.get("pipeline") or {},
        request.get("columns") or [],
        request.get("codeMappings") or [],
    )
    compiled=compile_pipeline(f"SELECT * FROM {source_sql}",source_schema,pipeline)
    if compiled.pipeline_hash != resolved_pipeline_hash:
        raise RuntimeError("resolved pipeline hash changed during compilation")
    expected_schema=request.get("expectedSourceSchemaHash")
    actual_schema=compiled.json(False)["sourceSchemaHash"]
    if expected_schema and expected_schema != actual_schema:
        raise PipelineSourceSchemaChanged(
            "PIPELINE_SOURCE_SCHEMA_CHANGED: 원본 항목의 형식이 바뀌었습니다. "
            "양식을 다시 확인해 주세요."
        )
    expected_pipeline=request.get("expectedPipelineHash")
    if expected_pipeline and expected_pipeline != compiled.pipeline_hash:
        raise PipelineSnapshotMismatch("PIPELINE_SNAPSHOT_MISMATCH: 저장된 변환 단계가 검증본과 다릅니다.")
    return compiled


def validate_pipeline_request(connection_id: str, request: dict, data_root: str | Path) -> dict:
    conn=connect(data_root,"pipeline-validate",request.get("requestId"))
    try:
        compiled=compile_pipeline_request(connection_id,request,data_root,conn)
        return {
            **compiled.json(False, include_resolved_pipeline=True),
            "compilerVersion":COMPILER_VERSION,
        }
    finally:
        conn.close()


def preview_pipeline(connection_id: str, request: dict, data_root: str | Path) -> dict:
    limit=int(request.get("limit") or 100)
    if limit<1 or limit>500: raise ValueError("preview limit must be between 1 and 500")
    conn=connect(data_root,"pipeline-preview",request.get("requestId"))
    try:
        compiled=compile_pipeline_request(connection_id,request,data_root,conn)
        result=conn.execute(f"SELECT * FROM ({compiled.sql}) AS __preview LIMIT {limit}",compiled.parameters)
        names=[item[0] for item in result.description or []]
        rows=[dict(zip(names,row)) for row in result.fetchall()]
        return {**compiled.json(False),"compilerVersion":COMPILER_VERSION,"limit":limit,"rowCount":len(rows),"columns":[item.json() for item in compiled.output_schema],"rows":json_safe_rows(rows)}
    finally:
        conn.close()
