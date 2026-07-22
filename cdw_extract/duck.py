from __future__ import annotations

import json
import os
import shutil
import tempfile
import threading
import time
import uuid
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from .contracts import ResourceBudget
from .errors import WorkerBusy
from .execution_scope import current_execution_resources


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


def _physical_memory_bytes() -> int:
    """Best-effort host memory discovery without a runtime psutil dependency."""

    if os.name == "nt":
        try:
            import ctypes

            class MemoryStatus(ctypes.Structure):
                _fields_ = [
                    ("length", ctypes.c_ulong),
                    ("memory_load", ctypes.c_ulong),
                    ("total_physical", ctypes.c_ulonglong),
                    ("available_physical", ctypes.c_ulonglong),
                    ("total_page_file", ctypes.c_ulonglong),
                    ("available_page_file", ctypes.c_ulonglong),
                    ("total_virtual", ctypes.c_ulonglong),
                    ("available_virtual", ctypes.c_ulonglong),
                    ("available_extended_virtual", ctypes.c_ulonglong),
                ]

            status = MemoryStatus()
            status.length = ctypes.sizeof(MemoryStatus)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
                return int(status.total_physical)
        except (AttributeError, OSError, ValueError):
            pass
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        page_count = int(os.sysconf("SC_PHYS_PAGES"))
        if page_size > 0 and page_count > 0:
            return page_size * page_count
    except (AttributeError, OSError, TypeError, ValueError):
        pass
    # The documented deployment target is 32 GiB.  Operators on a different
    # host should set DUCKDB_TOTAL_MEMORY_BYTES explicitly.
    return 32 * 1024**3


@dataclass(frozen=True, slots=True)
class _ResourceLease:
    threads: int
    memory_bytes: int
    temp_bytes: int


class _ResourceGovernor:
    """FIFO weighted gate for aggregate DuckDB CPU, memory, and spill limits."""

    def __init__(self, total_threads: int, total_memory_bytes: int, total_temp_bytes: int) -> None:
        self.total_threads = total_threads
        self.total_memory_bytes = total_memory_bytes
        self.total_temp_bytes = total_temp_bytes
        self._used_threads = 0
        self._used_memory_bytes = 0
        self._used_temp_bytes = 0
        self._condition = threading.Condition()
        self._waiters: deque[object] = deque()

    def acquire(
        self,
        threads: int,
        memory_bytes: int,
        temp_bytes: int,
        timeout: float,
        cancellation: Any | None = None,
    ) -> _ResourceLease:
        lease = _ResourceLease(
            threads=min(max(1, threads), self.total_threads),
            memory_bytes=min(max(16 * 1024**2, memory_bytes), self.total_memory_bytes),
            temp_bytes=min(max(16 * 1024**2, temp_bytes), self.total_temp_bytes),
        )
        deadline = time.monotonic() + timeout
        ticket = object()
        cancellation_registration = None
        if cancellation is not None:
            def wake_waiter() -> None:
                with self._condition:
                    self._condition.notify_all()

            cancellation_registration = cancellation.register_interrupt(wake_waiter)
        try:
            with self._condition:
                self._waiters.append(ticket)
                try:
                    while True:
                        if cancellation is not None:
                            cancellation.raise_if_cancelled()
                        is_next = self._waiters and self._waiters[0] is ticket
                        has_capacity = (
                            self._used_threads + lease.threads <= self.total_threads
                            and self._used_memory_bytes + lease.memory_bytes <= self.total_memory_bytes
                            and self._used_temp_bytes + lease.temp_bytes <= self.total_temp_bytes
                        )
                        if is_next and has_capacity:
                            self._waiters.popleft()
                            self._used_threads += lease.threads
                            self._used_memory_bytes += lease.memory_bytes
                            self._used_temp_bytes += lease.temp_bytes
                            self._condition.notify_all()
                            return lease
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise WorkerBusy(
                                "DuckDB worker is busy; timed out waiting for aggregate "
                                f"resources after {timeout:g} seconds"
                            )
                        self._condition.wait(remaining)
                finally:
                    if ticket in self._waiters:
                        self._waiters.remove(ticket)
                        self._condition.notify_all()
        finally:
            if cancellation_registration is not None:
                cancellation_registration.close()

    def release(self, lease: _ResourceLease) -> None:
        with self._condition:
            self._used_threads -= lease.threads
            self._used_memory_bytes -= lease.memory_bytes
            self._used_temp_bytes -= lease.temp_bytes
            if min(self._used_threads, self._used_memory_bytes, self._used_temp_bytes) < 0:
                raise RuntimeError("DuckDB resource lease was released more than once")
            self._condition.notify_all()


_resource_governor_guard = threading.Lock()
_resource_governor: _ResourceGovernor | None = None


def _get_resource_governor(temp_free_bytes: int) -> _ResourceGovernor:
    global _resource_governor
    with _resource_governor_guard:
        if _resource_governor is None:
            host_threads = _positive_int_env(
                "DUCKDB_TOTAL_THREADS",
                min(os.cpu_count() or 4, 16),
            )
            default_memory = max(
                256 * 1024**2,
                min(int(_physical_memory_bytes() * 0.75), 24 * 1024**3),
            )
            host_memory = _positive_int_env("DUCKDB_TOTAL_MEMORY_BYTES", default_memory)
            # Leave at least 20% of the current spill filesystem outside the
            # DuckDB quota so logging and atomic publication can still finish.
            safe_temp = max(16 * 1024**2, int(temp_free_bytes * 0.80))
            host_temp = _positive_int_env("DUCKDB_TOTAL_TEMP_BYTES", safe_temp)
            host_temp = min(host_temp, safe_temp)
            if host_memory < 16 * 1024**2 or host_temp < 16 * 1024**2:
                raise ValueError(
                    "DuckDB aggregate memory and temp limits must each be at least 16 MiB"
                )
            _resource_governor = _ResourceGovernor(host_threads, host_memory, host_temp)
        return _resource_governor


def _safe_operation_segment(value: object) -> str:
    normalized = "".join(character if character.isalnum() or character in "-_" else "-" for character in str(value or "duckdb"))
    return normalized.strip("-")[:96] or "duckdb"


def _temp_root(
    data_root: str | Path | None,
    scoped_temp_root: str | Path | None = None,
) -> Path:
    if scoped_temp_root is not None:
        return Path(scoped_temp_root).expanduser().resolve() / "duckdb"
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
        budget: ResourceBudget | None = None,
        temp_root: str | Path | None = None,
    ) -> None:
        started_at = time.monotonic()
        timeout = _positive_float_env("DUCKDB_OPERATION_QUEUE_TIMEOUT_SECONDS", 60.0)
        scoped_resources = current_execution_resources()
        cancellation = scoped_resources.cancellation if scoped_resources is not None else None
        self._slots = _get_operation_slots()
        slot_deadline = started_at + timeout
        while True:
            if cancellation is not None:
                cancellation.raise_if_cancelled()
            remaining = slot_deadline - time.monotonic()
            if remaining <= 0:
                raise WorkerBusy(
                    "DuckDB worker is busy; timed out waiting for an operation slot "
                    f"after {timeout:g} seconds"
                )
            if self._slots.acquire(timeout=min(remaining, 0.1)):
                break
        self._closed = False
        self._connection: duckdb.DuckDBPyConnection | None = None
        self._cancellation_registration = None
        self._governor: _ResourceGovernor | None = None
        self._resource_lease: _ResourceLease | None = None
        self._lifecycle_lock = threading.Lock()
        try:
            effective_budget = budget or (
                scoped_resources.budget if scoped_resources is not None else None
            )
            effective_temp_root = temp_root or (
                scoped_resources.temp_root if scoped_resources is not None else None
            )
            base_temp_directory = _temp_root(data_root, effective_temp_root)
            base_temp_directory.mkdir(parents=True, exist_ok=True)
            self._governor = _get_resource_governor(shutil.disk_usage(base_temp_directory).free)
            maximum_operations = _positive_int_env("DUCKDB_MAX_CONCURRENT_OPERATIONS", 4)
            default_threads = max(1, self._governor.total_threads // maximum_operations)
            default_memory = max(
                16 * 1024**2,
                self._governor.total_memory_bytes // maximum_operations,
            )
            default_temp = max(
                16 * 1024**2,
                self._governor.total_temp_bytes // maximum_operations,
            )
            requested_threads = effective_budget.cpu_threads if effective_budget else default_threads
            requested_memory = effective_budget.memory_bytes if effective_budget else default_memory
            requested_temp = effective_budget.temp_bytes if effective_budget else default_temp
            remaining = timeout - (time.monotonic() - started_at)
            if remaining <= 0:
                raise WorkerBusy(
                    "DuckDB worker is busy; operation queue timeout elapsed before resource allocation"
                )
            self._resource_lease = self._governor.acquire(
                requested_threads,
                requested_memory,
                requested_temp,
                remaining,
                cancellation,
            )
            self.effective_threads = self._resource_lease.threads
            self.effective_memory_bytes = self._resource_lease.memory_bytes
            self.effective_temp_bytes = self._resource_lease.temp_bytes
            label = _safe_operation_segment(operation_id or operation)
            self.temp_directory = base_temp_directory / (
                f"{_safe_operation_segment(operation)}-{label}-{uuid.uuid4().hex}"
            )
            self.temp_directory.mkdir(parents=True, exist_ok=False)
            self._connection = duckdb.connect(database=":memory:")
            self._connection.execute(f"SET temp_directory={sql_literal(self.temp_directory.as_posix())}")
            self._connection.execute(f"SET threads={self.effective_threads}")
            self._connection.execute(f"SET memory_limit='{self.effective_memory_bytes}B'")
            self._connection.execute(
                f"SET max_temp_directory_size='{self.effective_temp_bytes}B'"
            )
            # Platform-wide temporal semantics are fixed to Korean local time.
            self._connection.execute("SET TimeZone='Asia/Seoul'")
            if scoped_resources is not None:
                self._cancellation_registration = scoped_resources.cancellation.register_interrupt(
                    self.interrupt
                )
        except Exception:
            self._cleanup()
            raise

    def _cleanup(self) -> None:
        with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            connection, self._connection = self._connection, None
            cancellation_registration, self._cancellation_registration = (
                self._cancellation_registration,
                None,
            )
            governor, self._governor = self._governor, None
            resource_lease, self._resource_lease = self._resource_lease, None
        try:
            if cancellation_registration is not None:
                cancellation_registration.close()
            if connection is not None:
                connection.close()
        finally:
            temp_directory = getattr(self, "temp_directory", None)
            if temp_directory is not None:
                shutil.rmtree(temp_directory, ignore_errors=True)
            if governor is not None and resource_lease is not None:
                governor.release(resource_lease)
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
    *,
    budget: ResourceBudget | None = None,
    temp_root: str | Path | None = None,
) -> ManagedDuckDBConnection:
    return ManagedDuckDBConnection(data_root, operation, operation_id, budget, temp_root)


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
