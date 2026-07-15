from __future__ import annotations

from pathlib import Path

import requests

from .duck import quote_ident


def http_port(source: dict) -> int:
    options = source.get("options") or {}
    return int(options.get("httpPort") or source.get("httpPort") or 8123)


def http_protocol(source: dict) -> str:
    options = source.get("options") or {}
    return options.get("httpProtocol") or source.get("httpProtocol") or "http"


def clickhouse_url(source: dict) -> str:
    return f"{http_protocol(source)}://{source.get('host', 'localhost')}:{http_port(source)}"


def clickhouse_params(source: dict) -> dict:
    params = {
        "database": source.get("database") or "default",
        "user": source.get("username") or "default",
    }
    password = source.get("password")
    if password:
        params["password"] = password
    return params


def positive_seconds(value: object, default: int) -> int:
    try:
        seconds = int(value)
    except (TypeError, ValueError):
        seconds = default
    return seconds if seconds > 0 else default


def request_timeout(source: dict) -> tuple[int, int]:
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
    schema = table.get("schemaName") or source.get("schemaName") or source.get("database")
    name = table.get("tableName")
    if not name:
        raise ValueError("tables[].tableName is required")
    if schema:
        return f"{quote_ident(schema)}.{quote_ident(name)}"
    return quote_ident(name)


def clickhouse_select_list(table: dict) -> str:
    columns = table.get("columns") or []
    if not columns:
        return "*"
    return ", ".join(quote_ident(column["name"]) for column in columns)


def post_query(source: dict, query: str, stream: bool = False) -> requests.Response:
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


def write_clickhouse_table_parquet(source: dict, table: dict, output_path: str | Path) -> int:
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    source_table = clickhouse_table_name(source, table)
    select_list = clickhouse_select_list(table)
    query = f"SELECT {select_list} FROM {source_table} FORMAT Parquet"
    with post_query(source, query, stream=True) as response:
        with output.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    count_query = f"SELECT count() FROM {source_table} FORMAT TabSeparated"
    count_response = post_query(source, count_query)
    return int(count_response.text.strip() or "0")
