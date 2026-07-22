"""ClickHouse HTTP 질의와 Parquet 스트리밍 저장을 지원한다."""

from __future__ import annotations

from pathlib import Path

import requests

from .duck import quote_ident
from .errors import ResourceLimitExceeded
from .execution_scope import current_execution_resources


def http_port(source: dict) -> int:
    """소스 설정에서 ClickHouse HTTP 포트를 구한다."""

    options = source.get("options") or {}
    return int(options.get("httpPort") or source.get("httpPort") or 8123)


def http_protocol(source: dict) -> str:
    """소스 설정에서 ClickHouse HTTP 프로토콜을 구한다."""

    options = source.get("options") or {}
    return options.get("httpProtocol") or source.get("httpProtocol") or "http"


def clickhouse_url(source: dict) -> str:
    """ClickHouse HTTP 엔드포인트 URL을 조립한다."""

    return f"{http_protocol(source)}://{source.get('host', 'localhost')}:{http_port(source)}"


def clickhouse_params(source: dict) -> dict:
    """ClickHouse HTTP API에 전달할 인증·데이터베이스 매개변수를 만든다."""

    params = {
        "database": source.get("database") or "default",
        "user": source.get("username") or "default",
    }
    password = source.get("password")
    if password:
        params["password"] = password
    return params


def positive_seconds(value: object, default: int) -> int:
    """값을 양의 초 단위 정수로 정규화하고 실패하면 기본값을 반환한다."""

    try:
        seconds = int(value)
    except (TypeError, ValueError):
        seconds = default
    return seconds if seconds > 0 else default


def request_timeout(source: dict) -> tuple[int, int]:
    """연결 및 읽기 제한 시간을 requests 형식으로 반환한다."""

    options = source.get("options") or {}
    connect_timeout = positive_seconds(
        source.get("connectTimeoutSeconds") or options.get("connectTimeoutSeconds"),
        5,
    )
    read_timeout = positive_seconds(
        source.get("readTimeoutSeconds") or options.get("readTimeoutSeconds"),
        60,
    )
    return connect_timeout, read_timeout


def clickhouse_table_name(source: dict, table: dict) -> str:
    """스키마와 테이블 식별자를 안전하게 인용해 전체 이름을 만든다."""

    schema = table.get("schemaName") or source.get("schemaName") or source.get("database")
    name = table.get("tableName")
    if not name:
        raise ValueError("tables[].tableName is required")
    if schema:
        return f"{quote_ident(schema)}.{quote_ident(name)}"
    return quote_ident(name)


def clickhouse_select_list(table: dict) -> str:
    """요청된 열 목록을 인용된 SELECT 절로 변환한다."""

    columns = table.get("columns") or []
    if not columns:
        return "*"
    return ", ".join(quote_ident(column["name"]) for column in columns)


def post_query(source: dict, query: str, stream: bool = False) -> requests.Response:
    """ClickHouse HTTP 질의를 실행하고 성공 응답을 반환한다."""

    response = requests.post(
        clickhouse_url(source),
        params=clickhouse_params(source),
        data=query.encode("utf-8"),
        stream=stream,
        timeout=request_timeout(source),
    )
    if response.status_code < 200 or response.status_code >= 300:
        message = response.text[:4096].strip()
        raise RuntimeError(f"clickhouse http query failed status={response.status_code} body={message}")
    return response


def write_clickhouse_table_parquet(
    source: dict,
    table: dict,
    output_path: str | Path,
    *,
    maximum_bytes: int | None = None,
) -> int:
    """ClickHouse 테이블을 Parquet으로 스트리밍하고 원본 행 수를 반환한다."""

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    resources = current_execution_resources()
    cancellation = resources.cancellation if resources is not None else None
    byte_limits = [maximum_bytes] if maximum_bytes is not None else []
    # ClickHouse streams directly into the durable metadata snapshot volume.
    # resourceBudget.tempBytes governs disposable worker/spill space, not the
    # size of this persisted snapshot. Only an explicit outputBytes ceiling is
    # allowed to stop a legitimate large table export.
    if resources is not None and resources.budget.output_bytes is not None:
        byte_limits.append(resources.budget.output_bytes)
    effective_maximum = min(byte_limits) if byte_limits else None
    if effective_maximum is not None and effective_maximum < 0:
        raise ResourceLimitExceeded("ClickHouse metadata refresh has no remaining byte budget.")

    def raise_if_cancelled() -> None:
        if cancellation is not None:
            cancellation.raise_if_cancelled()

    source_table = clickhouse_table_name(source, table)
    select_list = clickhouse_select_list(table)
    query = f"SELECT {select_list} FROM {source_table} FORMAT Parquet"
    raise_if_cancelled()
    try:
        with post_query(source, query, stream=True) as response:
            register_interrupt = getattr(cancellation, "register_interrupt", None)
            registration = register_interrupt(response.close) if register_interrupt else None
            try:
                written_bytes = 0
                with output.open("xb") as file:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        raise_if_cancelled()
                        if not chunk:
                            continue
                        next_size = written_bytes + len(chunk)
                        if effective_maximum is not None and next_size > effective_maximum:
                            raise ResourceLimitExceeded(
                                "ClickHouse metadata refresh exceeded its remaining "
                                f"output byte budget={effective_maximum}."
                            )
                        file.write(chunk)
                        written_bytes = next_size
            finally:
                if registration is not None:
                    registration.close()
    except Exception:
        output.unlink(missing_ok=True)
        # Closing the response through the interrupt hook may surface as a
        # requests transport error. Preserve the job-level cancellation
        # outcome when cancellation was the actual cause.
        raise_if_cancelled()
        raise

    raise_if_cancelled()
    count_query = f"SELECT count() FROM {source_table} FORMAT TabSeparated"
    with post_query(source, count_query) as count_response:
        row_count = int(count_response.text.strip() or "0")
    raise_if_cancelled()
    return row_count
