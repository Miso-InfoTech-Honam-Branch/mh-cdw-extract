"""변환 파이프라인이 없는 일반 추출 미리보기를 실행한다."""

from __future__ import annotations

from pathlib import Path

from .duck import connect, json_safe_rows
from .query import final_query

DEFAULT_LIMIT = 100
MAX_LIMIT = 100000


def normalized_limit(value: int | None) -> int:
    """미리보기 행 제한을 기본값과 최대 허용 범위로 정규화한다."""

    if not value or value <= 0:
        return DEFAULT_LIMIT
    if value > MAX_LIMIT:
        raise ValueError(f"limit must be less than or equal to {MAX_LIMIT}")
    return value


def preview(connection_id: str, request: dict, data_root: str | Path) -> dict:
    """추출 SQL을 제한된 행 수로 실행하고 JSON 안전한 미리보기를 반환한다."""

    limit = normalized_limit(request.get("limit"))
    sql = final_query(connection_id, data_root, request, limit=limit)
    conn = connect(data_root, "preview", request.get("requestId"))
    try:
        result = conn.execute(sql)
        names = [desc[0] for desc in result.description or []]
        rows = [dict(zip(names, row)) for row in result.fetchall()]
    finally:
        conn.close()
    return {
        "connectionId": connection_id,
        "requestId": request.get("requestId", ""),
        "sourceType": (request.get("sourceType") or "").lower(),
        "limit": limit,
        "rowCount": len(rows),
        "columns": [{"name": name} for name in names],
        "rows": json_safe_rows(rows),
    }
