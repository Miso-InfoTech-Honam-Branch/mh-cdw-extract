from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import uuid
from pathlib import Path
from typing import Any

import duckdb


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def _positive_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


_operation_slots_guard = threading.Lock()
_operation_slots: threading.BoundedSemaphore | None = None


def _get_operation_slots() -> threading.BoundedSemaphore:
    global _operation_slots
    with _operation_slots_guard:
        if _operation_slots is None:
            maximum = _positive_int_env("DUCKDB_MAX_CONCURRENT_OPERATIONS", 4)
            _operation_slots = threading.BoundedSemaphore(maximum)
        return _operation_slots


def _safe_operation_segment(value: object) -> str:
    normalized = "".join(character if character.isalnum() or character in "-_" else "-" for character in str(value or "duckdb"))
    return normalized.strip("-")[:96] or "duckdb"


def _temp_root(data_root: str | Path | None) -> Path:
    if data_root is not None:
        return Path(data_root).expanduser().resolve() / "_tmp" / "duckdb"
    configured = os.environ.get("DUCKDB_TEMP_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    return Path(tempfile.gettempdir()).resolve() / "cdw-extract" / "duckdb"


class ManagedDuckDBConnection:
    """One bounded DuckDB operation with an isolated, disposable spill directory."""

    def __init__(
        self,
        data_root: str | Path | None,
        operation: str,
        operation_id: object | None,
    ) -> None:
        timeout = _positive_float_env("DUCKDB_OPERATION_QUEUE_TIMEOUT_SECONDS", 60.0)
        self._slots = _get_operation_slots()
        if not self._slots.acquire(timeout=timeout):
            raise TimeoutError(
                "DuckDB worker is busy; timed out waiting for an operation slot "
                f"after {timeout:g} seconds"
            )
        self._closed = False
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._lifecycle_lock = threading.Lock()
        try:
            label = _safe_operation_segment(operation_id or operation)
            self.temp_directory = _temp_root(data_root) / f"{_safe_operation_segment(operation)}-{label}-{uuid.uuid4().hex}"
            self.temp_directory.mkdir(parents=True, exist_ok=False)
            self._connection = duckdb.connect(database=":memory:")
            self._connection.execute(f"SET temp_directory={sql_literal(self.temp_directory.as_posix())}")
        except Exception:
            self._cleanup()
            raise

    def _cleanup(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            connection, self._connection = self._connection, None
        try:
            if connection is not None:
                connection.close()
        finally:
            temp_directory = getattr(self, "temp_directory", None)
            if temp_directory is not None:
                shutil.rmtree(temp_directory, ignore_errors=True)
            self._slots.release()

    def close(self) -> None:
        self._cleanup()

    def interrupt(self) -> None:
        with self._lifecycle_lock:
            connection = self._connection
        if connection is None:
            return
        try:
            connection.interrupt()
        except Exception:
            with self._lifecycle_lock:
                closed = self._closed
            if not closed:
                raise

    def execute(self, *args: Any, **kwargs: Any):
        if self._connection is None:
            raise RuntimeError("DuckDB connection is closed")
        return self._connection.execute(*args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        try:
            connection = object.__getattribute__(self, "_connection")
        except AttributeError as exc:
            raise AttributeError(name) from exc
        if connection is None:
            raise AttributeError(name)
        return getattr(connection, name)

    def __enter__(self) -> "ManagedDuckDBConnection":
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()

    def __del__(self) -> None:
        try:
            closed = object.__getattribute__(self, "_closed")
        except AttributeError:
            return
        if not closed:
            self._cleanup()


def connect(
    data_root: str | Path | None = None,
    operation: str = "duckdb",
    operation_id: object | None = None,
) -> ManagedDuckDBConnection:
    return ManagedDuckDBConnection(data_root, operation, operation_id)


def source_attach_sql(source: dict) -> tuple[str, str]:
    vendor = (source.get("vendor") or "postgresql").lower()
    if vendor == "postgresql":
        ext = "postgres"
        typ = "postgres"
        conn = (
            f"host={source.get('host', 'localhost')} "
            f"port={source.get('port', 5432)} "
            f"dbname={source.get('database')} "
            f"user={source.get('username')} "
            f"password={source.get('password', '')}"
        )
        return ext, f"ATTACH {sql_literal(conn)} AS src (TYPE {typ}, READ_ONLY)"
    if vendor in {"mysql", "mariadb"}:
        ext = "mysql"
        conn = (
            f"host={source.get('host', 'localhost')} "
            f"port={source.get('port', 3306)} "
            f"database={source.get('database')} "
            f"user={source.get('username')} "
            f"password={source.get('password', '')}"
        )
        return ext, f"ATTACH {sql_literal(conn)} AS src (TYPE mysql, READ_ONLY)"
    raise ValueError(f"unsupported refresh source vendor for duckdb: {vendor}")


def source_table_sql(source: dict, table: dict) -> str:
    schema = table.get("schemaName") or source.get("schemaName")
    name = table.get("tableName")
    if not name:
        raise ValueError("tables[].tableName is required")
    if schema:
        return f"src.{quote_ident(schema)}.{quote_ident(name)}"
    return f"src.{quote_ident(name)}"


def parquet_scan(path: str | Path, alias: str | None = None) -> str:
    sql = f"read_parquet({sql_literal(Path(path).as_posix())})"
    if alias:
        sql += f" AS {quote_ident(alias)}"
    return sql


def json_safe_rows(rows: list[dict]) -> list[dict]:
    return json.loads(json.dumps(rows, default=str, ensure_ascii=False))
