from __future__ import annotations

from pathlib import Path


def connection_root(data_root: str | Path, connection_id: str) -> Path:
    return Path(data_root) / "connections" / connection_id


def tables_dir(data_root: str | Path, connection_id: str) -> Path:
    return connection_root(data_root, connection_id) / "tables"


def extracts_dir(data_root: str | Path, connection_id: str) -> Path:
    return connection_root(data_root, connection_id) / "extracts"


def table_file_name(table: dict) -> str:
    table_id = table.get("tableId") or ""
    schema = table.get("schemaName") or ""
    name = table.get("tableName") or ""
    if schema and name:
        return f"{schema}.{name}.parquet"
    if name:
        return f"{name}.parquet"
    if table_id:
        return f"{table_id}.parquet"
    raise ValueError("tableId or tableName is required")
