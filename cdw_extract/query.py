"""테이블·조인·코드 매핑 요청을 안전한 DuckDB SQL로 조립한다."""

from __future__ import annotations

from pathlib import Path

from .duck import parquet_scan, quote_ident, sql_literal
from .manifest import load_connection_manifest
from .paths import connection_root
from .user_dataset import (
    dataset_file_parquet_path,
    load_dataset_file_manifest,
)

MTDT_TBL = "MTDT_TBL"
USER_DATST = "USER_DATST"


def table_alias(table: dict) -> str:
    """테이블 참조에 사용할 우선순위 기반 별칭을 반환한다."""

    return table.get("alias") or table.get("tableId") or table.get("tableName") or table.get("userDatasetFileId")


def normalized_source_kind(value: object) -> str:
    """호환 입력 이름을 메타데이터 테이블 또는 사용자 데이터셋 종류로 정규화한다."""

    raw = str(value or MTDT_TBL).strip().upper().replace("-", "_")
    if raw in {"", MTDT_TBL, "DB_TABLE"}:
        return MTDT_TBL
    if raw in {USER_DATST, "USER_DATASET"}:
        return USER_DATST
    raise ValueError(f"unsupported sourceKind: {value}")


def table_source_kind(table: dict) -> str:
    """테이블 요청의 레거시·현재 필드에서 소스 종류를 해석한다."""

    value = table.get("sourceKind")
    if value is None and str(table.get("sourceType") or "").strip().upper() in {MTDT_TBL, USER_DATST, "USER_DATASET", "DB_TABLE"}:
        value = table.get("sourceType")
    return normalized_source_kind(value)


def table_user_dataset_file_ref(table: dict) -> tuple[str, str, str] | None:
    """사용자 데이터셋 파일 식별자 세 쌍을 검증해 반환한다."""

    user_id = table.get("userId")
    user_dataset_id = table.get("userDatasetId") or table.get("userDatstId") or table.get("sourceId")
    user_dataset_file_id = table.get("userDatasetFileId") or table.get("userDatstFileId") or table.get("fileId")
    if user_id or user_dataset_id or user_dataset_file_id:
        if not user_id:
            raise ValueError("userId is required when sourceKind is USER_DATST")
        if not user_dataset_id:
            raise ValueError("userDatasetId is required when sourceKind is USER_DATST")
        if not user_dataset_file_id:
            raise ValueError("userDatasetFileId is required when sourceKind is USER_DATST")
        return str(user_id), str(user_dataset_id), str(user_dataset_file_id)
    return None


def table_ref_from_manifest(manifest: dict, table: dict) -> dict:
    """연결 매니페스트에서 식별자 또는 스키마·이름이 일치하는 테이블을 찾는다."""

    table_id = table.get("tableId")
    schema = table.get("schemaName")
    name = table.get("tableName")
    for item in manifest.get("tables", []):
        if table_id and item.get("tableId") == table_id:
            return item
        if name and item.get("tableName") == name and (not schema or item.get("schemaName") == schema):
            return item
    raise ValueError(f"table not found in manifest: {table}")


def connection_table_path(data_root: str | Path, connection_id: str, manifest: dict, table: dict) -> Path:
    """매니페스트 테이블 경로가 연결의 tables 디렉터리 안인지 검증한다."""

    item = table_ref_from_manifest(manifest, table)
    raw_path = item.get("path")
    if not isinstance(raw_path, str) or not raw_path.strip():
        raise ValueError("connection manifest table path is required")
    relative = Path(raw_path.strip())
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("connection manifest table path must be relative and must not traverse parents")
    if any(character in raw_path for character in "*?[]{}"):
        raise ValueError("connection manifest table path must not contain glob patterns")

    root = connection_root(data_root, connection_id)
    tables_root = (root / "tables").resolve()
    candidate = (root / relative).resolve()
    if not tables_root.is_relative_to(root) or not candidate.is_relative_to(tables_root):
        raise ValueError("connection manifest table path must remain beneath its tables directory")
    return candidate


class SourceResolver:
    """연결·사용자 데이터셋 매니페스트를 캐시하며 정규 Parquet 경로를 해석한다."""

    def __init__(self, connection_id: str, data_root: str | Path):
        self.connection_id = connection_id
        self.data_root = Path(data_root)
        self._connection_manifest: dict | None = None
        self._user_dataset_file_manifests: dict[tuple[str, str, str], dict] = {}

    def connection_manifest(self) -> dict:
        if self._connection_manifest is None:
            self._connection_manifest = load_connection_manifest(self.connection_id, self.data_root)
            manifest_connection_id = self._connection_manifest.get("connectionId")
            if (
                manifest_connection_id is not None
                and str(manifest_connection_id) != self.connection_id
            ):
                raise ValueError("connection manifest identity does not match connectionId")
        return self._connection_manifest

    def user_dataset_file_manifest(self, user_id: str, user_dataset_id: str, user_dataset_file_id: str) -> dict:
        key = (user_id, user_dataset_id, user_dataset_file_id)
        if key not in self._user_dataset_file_manifests:
            self._user_dataset_file_manifests[key] = load_dataset_file_manifest(
                self.data_root,
                user_id,
                user_dataset_id,
                user_dataset_file_id,
            )
        return self._user_dataset_file_manifests[key]

    def table_path(self, table: dict) -> Path:
        source_kind = table_source_kind(table)
        if source_kind == MTDT_TBL:
            return connection_table_path(self.data_root, self.connection_id, self.connection_manifest(), table)

        dataset_file_ref = table_user_dataset_file_ref(table)
        if dataset_file_ref:
            user_id, user_dataset_id, user_dataset_file_id = dataset_file_ref
            manifest = self.user_dataset_file_manifest(user_id, user_dataset_id, user_dataset_file_id)
            expected_identity = {
                "userId": user_id,
                "userDatasetId": user_dataset_id,
                "userDatasetFileId": user_dataset_file_id,
            }
            for field_name, expected_value in expected_identity.items():
                if str(manifest.get(field_name) or "") != expected_value:
                    raise ValueError(f"user dataset manifest {field_name} does not match the requested source")
            if str(manifest.get("status") or "").upper() != "SUCCESS":
                raise ValueError("user dataset manifest is not published successfully")
            path = Path(manifest["path"]) if manifest.get("path") else None
            if path:
                if path.is_absolute():
                    raise ValueError("user dataset manifest path must be relative to DATA_ROOT")
                resolved = (self.data_root / path).resolve()
            else:
                resolved = dataset_file_parquet_path(
                    self.data_root,
                    user_id,
                    user_dataset_id,
                    user_dataset_file_id,
                ).resolve()
            expected = dataset_file_parquet_path(
                self.data_root,
                user_id,
                user_dataset_id,
                user_dataset_file_id,
            ).resolve()
            if resolved != expected:
                raise ValueError("user dataset manifest path does not match its canonical file location")
            if not resolved.exists():
                raise FileNotFoundError(f"user dataset Parquet file not found: {resolved}")
            return resolved

        raise ValueError("userId, userDatasetId, and userDatasetFileId are required when sourceKind is USER_DATST")


def output_column_name(column: dict) -> str:
    """투영 열의 출력 별칭 또는 원래 이름을 반환한다."""

    return column.get("alias") or column["name"]


def column_expr(column: dict, default_alias: str | None = None) -> str:
    """선택적 테이블 별칭을 포함한 인용 열 표현식을 만든다."""

    table = column.get("table") or default_alias
    name = column["name"]
    if table:
        return f"{quote_ident(table)}.{quote_ident(name)}"
    return quote_ident(name)


def projection_sql(columns: list[dict], default_alias: str | None = None) -> str:
    """요청 열 목록을 명시적 출력 별칭이 있는 SELECT 투영으로 변환한다."""

    if not columns:
        return "*"
    parts = []
    for column in columns:
        expr = column_expr(column, default_alias)
        out = output_column_name(column)
        parts.append(f"{expr} AS {quote_ident(out)}")
    return ", ".join(parts)


def mapping_output_expr(mapping: dict, index: int) -> str:
    """코드 매핑 테이블의 이름 열을 요청 출력 열로 투영한다."""

    output = mapping["outputColumn"]
    return f"{quote_ident(f'__code_map_{index}')}.{quote_ident(mapping['codeNameColumn'])} AS {quote_ident(output)}"


def projection_with_mappings_sql(columns: list[dict], mappings: list[dict], default_alias: str | None = None) -> str:
    """일반 열과 코드명 치환 열을 결합한 SELECT 투영을 만든다."""

    outputs = {mapping["outputColumn"]: i for i, mapping in enumerate(mappings or [])}
    if not columns:
        parts = ["*"]
        parts.extend(mapping_output_expr(mapping, i) for i, mapping in enumerate(mappings or []))
        return ", ".join(parts)
    parts = []
    for column in columns:
        name = column["name"]
        if name in outputs and not column.get("table"):
            parts.append(mapping_output_expr(mappings[outputs[name]], outputs[name]))
            continue
        expr = column_expr(column, default_alias)
        out = output_column_name(column)
        parts.append(f"{expr} AS {quote_ident(out)}")
    return ", ".join(parts)


def filter_sql(filters: list[dict]) -> str:
    """허용 목록 기반 필터 요청을 AND로 연결된 SQL 조건으로 만든다."""

    clauses = []
    for item in filters or []:
        col = quote_ident(item["column"])
        op = (item.get("op") or "").lower().strip()
        value = item.get("value")
        values = item.get("values") or []
        if op in {"eq", "=", "=="}:
            clauses.append(f"{col} = {sql_literal(value)}")
        elif op in {"ne", "!=", "<>"}:
            clauses.append(f"{col} <> {sql_literal(value)}")
        elif op in {"gt", ">"}:
            clauses.append(f"{col} > {sql_literal(value)}")
        elif op in {"gte", ">="}:
            clauses.append(f"{col} >= {sql_literal(value)}")
        elif op in {"lt", "<"}:
            clauses.append(f"{col} < {sql_literal(value)}")
        elif op in {"lte", "<="}:
            clauses.append(f"{col} <= {sql_literal(value)}")
        elif op == "in":
            clauses.append(f"{col} IN ({', '.join(sql_literal(v) for v in values)})")
        elif op in {"contains", "like"}:
            clauses.append(f"contains(CAST({col} AS VARCHAR), {sql_literal(value)})")
        elif op == "is_null":
            clauses.append(f"{col} IS NULL")
        elif op == "is_not_null":
            clauses.append(f"{col} IS NOT NULL")
        else:
            raise ValueError(f"unsupported filter op: {op}")
    return " AND ".join(clauses)


def sort_sql(sorts: list[dict]) -> str:
    """출력 열 정렬 요청을 인용된 ORDER BY 항목으로 만든다."""

    parts = []
    for item in sorts or []:
        direction = "DESC" if (item.get("direction") or "asc").lower() == "desc" else "ASC"
        parts.append(f"{quote_ident(item['column'])} {direction}")
    return ", ".join(parts)


def single_table_request(request: dict) -> dict:
    """단일 테이블 추출 요청을 공통 테이블 참조 형태로 변환한다."""

    return {
        "sourceKind": request.get("sourceKind"),
        "userId": request.get("userId"),
        "userDatasetId": request.get("userDatasetId") or request.get("userDatstId"),
        "userDatasetFileId": request.get("userDatasetFileId") or request.get("userDatstFileId") or request.get("fileId"),
        "alias": request.get("alias"),
        "tableId": request.get("tableId"),
        "schemaName": request.get("schemaName"),
        "tableName": request.get("tableName"),
    }


def single_table_alias(request: dict) -> str:
    """단일 소스에 사용할 안정적인 DuckDB 별칭을 반환한다."""

    return request.get("alias") or request.get("tableName") or request.get("tableId") or "__src"


def join_type_sql(value: object) -> str:
    """요청 조인 유형을 지원되는 DuckDB JOIN 키워드로 정규화한다."""

    raw = str(value or "inner").strip().upper().replace("_", " ")
    if raw in {"LEFT", "LEFT JOIN"}:
        return "LEFT JOIN"
    if raw in {"RIGHT", "RIGHT JOIN"}:
        return "RIGHT JOIN"
    if raw in {"FULL", "OUTER", "FULL OUTER", "FULL OUTER JOIN"}:
        return "FULL OUTER JOIN"
    return "INNER JOIN"


def source_from_sql(resolver: SourceResolver, request: dict) -> str:
    """단일 Parquet 소스 또는 검증된 다중 테이블 JOIN 절을 조립한다."""

    source_type = (request.get("sourceType") or "").lower()
    if source_type == "table":
        table = single_table_request(request)
        alias = single_table_alias(request)
        path = resolver.table_path(table)
        return parquet_scan(path, alias)
    if source_type == "join":
        tables = request.get("tables") or []
        if not tables:
            raise ValueError("tables is required when sourceType is join")
        table_map = {}
        for table in tables:
            alias = table_alias(table)
            if not alias:
                raise ValueError("tables[].alias/tableId/tableName is required when sourceType is join")
            table_map[alias] = table
        base_alias = request.get("baseTable")
        if not base_alias or base_alias not in table_map:
            raise ValueError("baseTable must match one of tables[].alias/tableId/tableName")
        base_path = resolver.table_path(table_map[base_alias])
        sql = parquet_scan(base_path, base_alias)
        # 조인은 요청 순서대로 이어 붙인다. 각 단계의 키만 SQL로 만들고
        # 파일 경로와 식별자는 앞선 해석·인용 경계를 통과한 값만 사용한다.
        for join in request.get("joins") or []:
            right_alias = join["rightTable"]
            if right_alias not in table_map:
                raise ValueError("joins[].rightTable must match one of tables[].alias/tableId/tableName")
            right = table_map[right_alias]
            right_path = resolver.table_path(right)
            join_type = join_type_sql(join.get("joinType"))
            clauses = []
            for key in join.get("keys") or []:
                clauses.append(
                    f"{quote_ident(join['leftTable'])}.{quote_ident(key['leftColumn'])} = "
                    f"{quote_ident(right_alias)}.{quote_ident(key['rightColumn'])}"
                )
            if not clauses:
                raise ValueError("joins[].keys is required")
            sql += f" {join_type} {parquet_scan(right_path, right_alias)} ON {' AND '.join(clauses)}"
        return sql
    raise ValueError("sourceType must be one of: table, join")


def mapping_source_expr(request: dict, mapping: dict, default_alias: str | None) -> str:
    """코드 매핑의 원본 코드 열을 정확한 소스 별칭에 연결한다."""

    source_table = mapping.get("sourceTable")
    if source_table:
        return f"{quote_ident(source_table)}.{quote_ident(mapping['sourceColumn'])}"
    if (request.get("sourceType") or "").lower() == "join" and request.get("baseTable"):
        return f"{quote_ident(request['baseTable'])}.{quote_ident(mapping['sourceColumn'])}"
    if default_alias:
        return f"{quote_ident(default_alias)}.{quote_ident(mapping['sourceColumn'])}"
    return quote_ident(mapping["sourceColumn"])


def source_with_code_mappings_sql(
    resolver: SourceResolver,
    request: dict,
    source: str,
    default_alias: str | None,
) -> str:
    """코드 테이블을 LEFT JOIN해 매핑 실패 시 원본 행을 보존한다."""

    sql = source
    for index, mapping in enumerate(request.get("codeMappings") or []):
        table = {
            "sourceKind": mapping.get("codeSourceKind") or mapping.get("sourceKind"),
            "userId": mapping.get("codeUserId") or mapping.get("userId"),
            "userDatasetId": mapping.get("codeUserDatasetId") or mapping.get("codeUserDatstId") or mapping.get("userDatasetId") or mapping.get("userDatstId"),
            "userDatasetFileId": mapping.get("codeUserDatasetFileId") or mapping.get("codeUserDatstFileId") or mapping.get("userDatasetFileId") or mapping.get("userDatstFileId") or mapping.get("fileId"),
            "tableId": mapping.get("codeTableId"),
            "schemaName": mapping.get("schemaName"),
            "tableName": mapping.get("codeTableName"),
        }
        path = resolver.table_path(table)
        alias = f"__code_map_{index}"
        left = mapping_source_expr(request, mapping, default_alias)
        right = f"{quote_ident(alias)}.{quote_ident(mapping['codeColumn'])}"
        # 서로 다른 DB에서 수집된 코드 열의 물리 타입이 달라도 의미상
        # 동일한 코드가 매칭되도록 양쪽을 VARCHAR로 통일한다.
        sql += (
            f" LEFT JOIN {parquet_scan(path, alias)}"
            f" ON CAST({left} AS VARCHAR) = CAST({right} AS VARCHAR)"
        )
    return sql


def final_query(connection_id: str, data_root: str | Path, request: dict, limit: int | None = None) -> str:
    """소스, 투영, 매핑, 필터, 정렬, 제한을 최종 추출 SQL로 결합한다."""

    resolver = SourceResolver(connection_id, data_root)
    default_alias = None
    if (request.get("sourceType") or "").lower() == "table":
        default_alias = single_table_alias(request)
    source = source_with_code_mappings_sql(
        resolver,
        request,
        source_from_sql(resolver, request),
        default_alias,
    )
    inner = (
        f"SELECT {projection_with_mappings_sql(request.get('columns') or [], request.get('codeMappings') or [], default_alias)} "
        f"FROM {source}"
    )
    # 매핑으로 새로 만든 출력 열이나 투영 별칭도 필터·정렬할 수 있도록
    # 먼저 내부 SELECT를 확정한 다음 외부 질의에 조건을 적용한다.
    sql = f"SELECT * FROM ({inner}) AS __base"
    where = filter_sql(request.get("filters") or [])
    if where:
        sql += f" WHERE {where}"
    order_by = sort_sql(request.get("sorts") or [])
    if order_by:
        sql += f" ORDER BY {order_by}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql
