from __future__ import annotations

import copy
import hashlib
from pathlib import Path

from ..duck import connect, json_safe_rows, quote_ident
from ..query import SourceResolver, projection_with_mappings_sql, source_from_sql, source_with_code_mappings_sql, single_table_alias
from .compiler import MAX_PIVOT_VALUES, CompiledPipeline, canonical_hash, compile_pipeline, inspect_source_schema


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


def _resolve_automatic_pivot_values(connection, source_sql: str, source_schema: list, pipeline: dict) -> tuple[dict, str]:
    """Resolve an empty PIVOT values list from all rows at that pipeline step."""
    declarative_hash=canonical_hash(pipeline)
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
        result=connection.execute(
            f"SELECT DISTINCT {quote_ident(pivot_column.physical_name)} AS __pivot_value FROM ({compiled_prefix.sql}) AS __pivot_source "
            f"WHERE {quote_ident(pivot_column.physical_name)} IS NOT NULL ORDER BY 1 LIMIT {MAX_PIVOT_VALUES + 1}",
            compiled_prefix.parameters,
        ).fetchall()
        if len(result)>MAX_PIVOT_VALUES:
            raise ValueError(f"PIVOT_TOO_MANY_VALUES: 고유값이 {MAX_PIVOT_VALUES}개를 초과하여 가로로 펼칠 수 없습니다.")
        if not result:
            raise ValueError("PIVOT_NO_VALUES: 가로로 펼칠 값이 없습니다.")
        config["values"]=[{"valueId":_pivot_value_id(row[0]),"value":row[0],"label":str(row[0]),"sort":order} for order,row in enumerate(result,1)]
        step["config"]=config
    return resolved,declarative_hash


def compile_pipeline_request(connection_id: str, request: dict, data_root: str | Path, connection):
    source_sql=_source_sql(connection_id,data_root,request)
    described=connection.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
    source_schema=inspect_source_schema(described,request.get("sourceColumns"))
    pipeline,declarative_hash=_resolve_automatic_pivot_values(connection,source_sql,source_schema,request.get("pipeline") or {})
    compiled=compile_pipeline(f"SELECT * FROM {source_sql}",source_schema,pipeline)
    if compiled.pipeline_hash!=declarative_hash:
        compiled=CompiledPipeline(compiled.sql,compiled.parameters,compiled.output_schema,compiled.step_schemas,compiled.warnings,declarative_hash)
    expected_schema=request.get("expectedSourceSchemaHash")
    actual_schema=compiled.json(False)["sourceSchemaHash"]
    if expected_schema and expected_schema != actual_schema:
        raise ValueError("PIPELINE_SOURCE_SCHEMA_CHANGED: 원본 항목의 형식이 바뀌었습니다. 양식을 다시 확인해 주세요.")
    expected_pipeline=request.get("expectedPipelineHash")
    if expected_pipeline and expected_pipeline != compiled.pipeline_hash:
        raise ValueError("PIPELINE_SNAPSHOT_MISMATCH: 저장된 변환 단계가 검증본과 다릅니다.")
    return compiled


def validate_pipeline_request(connection_id: str, request: dict, data_root: str | Path) -> dict:
    conn=connect(data_root,"pipeline-validate",request.get("requestId"))
    try:
        compiled=compile_pipeline_request(connection_id,request,data_root,conn)
        return {**compiled.json(False),"compilerVersion":"1"}
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
        return {**compiled.json(False),"compilerVersion":"1","limit":limit,"rowCount":len(rows),"columns":[item.json() for item in compiled.output_schema],"rows":json_safe_rows(rows)}
    finally:
        conn.close()
