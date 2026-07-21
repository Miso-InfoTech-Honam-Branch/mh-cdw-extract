from __future__ import annotations

from pathlib import Path

from ..duck import connect, json_safe_rows
from ..query import SourceResolver, source_from_sql, source_with_code_mappings_sql, single_table_alias
from .compiler import compile_pipeline, inspect_source_schema


def _source_sql(connection_id: str, data_root: str | Path, request: dict) -> str:
    resolver=SourceResolver(connection_id,data_root)
    default_alias=single_table_alias(request) if str(request.get("sourceType") or "").lower()=="table" else None
    source=source_from_sql(resolver,request)
    return source_with_code_mappings_sql(resolver,request,source,default_alias)


def compile_pipeline_request(connection_id: str, request: dict, data_root: str | Path, connection):
    source_sql=_source_sql(connection_id,data_root,request)
    described=connection.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
    source_schema=inspect_source_schema(described,request.get("sourceColumns"))
    compiled=compile_pipeline(f"SELECT * FROM {source_sql}",source_schema,request.get("pipeline") or {})
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
