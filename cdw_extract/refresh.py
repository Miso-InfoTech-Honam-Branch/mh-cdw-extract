from __future__ import annotations

import logging
import re
import shutil
import time
import uuid
from pathlib import Path

import requests

from .callback import callback_options as normalized_callback_options, post_json_callback
from .clickhouse import write_clickhouse_table_parquet
from .contracts import ResourceBudget
from .duck import connect, quote_ident, source_attach_sql, source_table_sql
from .errors import ResourceLimitExceeded
from .jobs import save_job
from .manifest import save_connection_manifest, utc_now
from .paths import connection_root, safe_path_segment, table_file_name

TABLE_REFRESH = "TABLE_REFRESH"
CALLBACK_MAX_ATTEMPTS = 3
CALLBACK_INITIAL_BACKOFF_SECONDS = 0.1
REDACTED = "[REDACTED]"

logger = logging.getLogger(__name__)

_SENSITIVE_KEY = (
    r"(?:user(?:name)?|password|passwd|pwd|credentials?|token|access[_-]?token|"
    r"refresh[_-]?token|api[_-]?key|secret|client[_-]?secret|authorization|cookie)"
)
_URL_USERINFO_PATTERN = re.compile(r"(?i)\b(?P<scheme>https?://)(?P<userinfo>[^/@\s]+)@")
_QUOTED_SECRET_PATTERN = re.compile(
    rf"(?i)(?P<prefix>[\"']?{_SENSITIVE_KEY}[\"']?\s*[:=]\s*)(?P<quote>[\"'])(?P<value>.*?)(?P=quote)"
)
_UNQUOTED_SECRET_PATTERN = re.compile(
    rf"(?i)(?P<prefix>\b{_SENSITIVE_KEY}\b\s*=\s*)(?P<value>[^&;\s,)\]}}\"']+)"
)
_HEADER_SECRET_PATTERN = re.compile(
    rf"(?im)(?P<prefix>^\s*{_SENSITIVE_KEY}\s*:\s*)(?P<value>[^\r\n]+)$"
)


def sanitize_refresh_text(value: object) -> str:
    """Remove credentials from errors before they cross the worker boundary."""
    text = str(value or "")
    text = _URL_USERINFO_PATTERN.sub(lambda match: f"{match.group('scheme')}{REDACTED}@", text)
    text = _QUOTED_SECRET_PATTERN.sub(
        lambda match: f"{match.group('prefix')}{match.group('quote')}{REDACTED}{match.group('quote')}",
        text,
    )
    text = _UNQUOTED_SECRET_PATTERN.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}",
        text,
    )
    return _HEADER_SECRET_PATTERN.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}",
        text,
    )


def refresh_error_fields(exc: Exception) -> dict:
    message = sanitize_refresh_text(exc)
    return {
        "errorCode": sanitize_refresh_text(type(exc).__name__),
        "error": message,
        "message": message,
    }


def callback_error_fields(exc: Exception) -> dict:
    error = refresh_error_fields(exc)
    return {
        "errorCode": error["errorCode"],
        "message": error["message"],
        "attempts": CALLBACK_MAX_ATTEMPTS,
        "occurredAt": utc_now(),
    }


def save_callback_error(data_root: str | Path, job: dict, exc: Exception) -> dict:
    callback_error = callback_error_fields(exc)
    job["callbackError"] = callback_error
    logger.error(
        "Table refresh callback delivery exhausted. jobId=%s state=%s errorCode=%s message=%s",
        job.get("jobId"),
        job.get("state"),
        callback_error["errorCode"],
        callback_error["message"],
    )
    return save_job(data_root, job)


def success_job_fields(result: dict) -> dict:
    return {
        "state": "COMPLETED",
        "connectionId": result["connectionId"],
        "tableCount": result["tableCount"],
        "rowCount": result["rowCount"],
        "tables": result["tables"],
        "message": result["message"],
    }


def callback_options(request: dict) -> dict:
    return normalized_callback_options(request, legacy_url_key="callbackUrl")


def post_refresh_callback(request: dict, payload: dict) -> dict | None:
    return post_json_callback(
        callback_options(request),
        payload,
        operation="refresh",
        attempts=CALLBACK_MAX_ATTEMPTS,
        backoff_seconds=tuple(
            CALLBACK_INITIAL_BACKOFF_SECONDS * (2 ** index)
            for index in range(CALLBACK_MAX_ATTEMPTS - 1)
        ),
        post=requests.post,
        wait=time.sleep,
    )


def refresh_success_callback_payload(result: dict) -> dict:
    return {
        "jobId": result["jobId"],
        "jobType": TABLE_REFRESH,
        "connectionId": result["connectionId"],
        "state": "SUCCESS",
        "status": "SUCCESS",
        "message": result["message"],
        "tableCount": result["tableCount"],
        "rowCount": result["rowCount"],
        "tables": result["tables"],
    }


def refresh_failure_callback_payload(job_id: str, connection_id: str, exc: Exception) -> dict:
    return {
        "jobId": job_id,
        "jobType": TABLE_REFRESH,
        "connectionId": connection_id,
        "state": "FAILED",
        "status": "FAILED",
        **refresh_error_fields(exc),
    }


def refresh_tables_impl(
    connection_id: str,
    request: dict,
    data_root: str | Path,
    job_id: str,
    budget: ResourceBudget | None = None,
) -> dict:
    source = request.get("sourceConnection") or {}
    tables = request.get("tables") or []
    if not connection_id:
        raise ValueError("connection_id is required")
    if not tables:
        raise ValueError("tables is required")

    compression = ((request.get("options") or {}).get("compression") or "snappy").lower()
    root = connection_root(data_root, connection_id)
    safe_job_id = safe_path_segment(job_id, "jobId")
    snapshot_id = f"{safe_job_id}-{uuid.uuid4().hex[:12]}"
    # Resolve only existing directory generations. On Windows, resolving a
    # path while another thread creates one of its missing ancestors can
    # transiently produce an inconsistent result. Creating and validating
    # each trusted ancestor also lets us reject a redirected _tmp symlink
    # before writing a snapshot beneath it.
    root.mkdir(parents=True, exist_ok=True)
    temporary_root_path = root / "_tmp"
    temporary_root_path.mkdir(parents=True, exist_ok=True)
    temporary_root = temporary_root_path.resolve()
    if not temporary_root.is_relative_to(root):
        raise ValueError("metadata refresh temporary path must remain beneath its connection root")
    # Include the random snapshot generation in the staging path. Queue
    # redelivery should normally be collapsed by CdwEngine, but this also
    # keeps legacy/direct callers and a concurrent retry of the same jobId
    # from writing into (or deleting) one another's temporary directory.
    staging_root = temporary_root / snapshot_id
    staging_root.mkdir()
    staging_root = staging_root.resolve()
    if not staging_root.is_relative_to(temporary_root):
        raise ValueError("metadata refresh staging path must remain beneath its temporary root")
    tmp = staging_root / "tables"
    tmp.mkdir()
    tmp = tmp.resolve()
    if not tmp.is_relative_to(staging_root):
        shutil.rmtree(staging_root, ignore_errors=True)
        raise ValueError("metadata refresh paths must remain beneath their staging root")
    final_tables = root / "tables"

    vendor = (source.get("vendor") or "postgresql").lower()
    conn = None
    artifacts = []
    total_rows = 0
    staged_bytes = 0
    try:
        if vendor != "clickhouse":
            conn = connect(data_root, "refresh", job_id)
            extension, attach_sql = source_attach_sql(source)
            conn.execute(f"INSTALL {extension}")
            conn.execute(f"LOAD {extension}")
            conn.execute(attach_sql)
        for table in tables:
            file_name = table_file_name(table)
            output = (tmp / file_name).resolve()
            if not output.is_relative_to(tmp):
                raise ValueError("metadata refresh output must remain beneath its staging directory")
            if vendor == "clickhouse":
                maximum_bytes = None
                if budget is not None and budget.output_bytes is not None:
                    maximum_bytes = max(0, budget.output_bytes - staged_bytes)
                if maximum_bytes is None:
                    row_count = write_clickhouse_table_parquet(source, table, output)
                else:
                    row_count = write_clickhouse_table_parquet(
                        source,
                        table,
                        output,
                        maximum_bytes=maximum_bytes,
                    )
            else:
                if conn is None:
                    raise RuntimeError("duckdb connection is not initialized")
                select_list = "*"
                if table.get("columns"):
                    select_list = ", ".join(
                        quote_ident(str(column["name"])) for column in table["columns"]
                    )
                source_sql = f"SELECT {select_list} FROM {source_table_sql(source, table)}"
                conn.execute(
                    f"COPY ({source_sql}) TO ? (FORMAT PARQUET, COMPRESSION {compression.upper()})",
                    [output.as_posix()],
                )
                row_count = conn.execute("SELECT count(*) FROM read_parquet(?)", [output.as_posix()]).fetchone()[0]
            total_rows += row_count
            staged_bytes += output.stat().st_size
            artifacts.append(
                {
                    "tableId": table.get("tableId"),
                    "schemaName": table.get("schemaName") or source.get("schemaName"),
                    "tableName": table.get("tableName"),
                    "path": (Path("tables") / snapshot_id / file_name).as_posix(),
                    "rowCount": row_count,
                    "columns": table.get("columns") or [],
                }
            )
            if budget is not None and budget.row_limit is not None and total_rows > budget.row_limit:
                raise ResourceLimitExceeded(
                    f"Metadata refresh exceeded resourceBudget.rowLimit={budget.row_limit}."
                )
            if budget is not None and budget.output_bytes is not None and staged_bytes > budget.output_bytes:
                raise ResourceLimitExceeded(
                    f"Metadata refresh exceeded resourceBudget.outputBytes={budget.output_bytes}."
                )
            if budget is not None and vendor != "clickhouse" and staged_bytes > budget.temp_bytes:
                raise ResourceLimitExceeded(
                    f"Metadata refresh staging exceeded resourceBudget.tempBytes={budget.temp_bytes}."
                )
    except Exception:
        shutil.rmtree(tmp.parent, ignore_errors=True)
        raise
    finally:
        if conn is not None:
            conn.close()

    # Publish an immutable snapshot. Existing extracts keep reading the
    # manifest version they already resolved while new extracts atomically see
    # the new manifest below; no live tables directory is deleted in place.
    final_tables.mkdir(parents=True, exist_ok=True)
    final_tables = final_tables.resolve()
    if not final_tables.is_relative_to(root):
        shutil.rmtree(tmp.parent, ignore_errors=True)
        raise ValueError("metadata refresh tables path must remain beneath its connection root")
    final_snapshot = (final_tables / snapshot_id).resolve()
    if not final_snapshot.is_relative_to(final_tables):
        shutil.rmtree(tmp.parent, ignore_errors=True)
        raise ValueError("metadata refresh snapshot must remain beneath its tables directory")
    try:
        if final_snapshot.exists():
            raise FileExistsError("metadata refresh snapshot already exists for this jobId")
        tmp.replace(final_snapshot)
    except Exception:
        shutil.rmtree(tmp.parent, ignore_errors=True)
        raise
    shutil.rmtree(tmp.parent, ignore_errors=True)

    manifest = {
        "connectionId": connection_id,
        "status": "COMPLETED",
        "snapshotId": snapshot_id,
        "tables": artifacts,
        "updatedAt": utc_now(),
    }
    save_connection_manifest(connection_id, data_root, manifest)
    return {
        "jobId": job_id,
        "jobType": TABLE_REFRESH,
        "connectionId": connection_id,
        "state": "COMPLETED",
        "message": "Table Parquet refresh completed successfully.",
        "tableCount": len(artifacts),
        "rowCount": total_rows,
        "tables": artifacts,
    }


def refresh_tables(connection_id: str, request: dict, data_root: str | Path, job_id: str | None = None) -> dict:
    job_id = job_id or request.get("jobId") or str(uuid.uuid4())
    job = save_job(
        data_root,
        {
            "jobId": job_id,
            "jobType": TABLE_REFRESH,
            "connectionId": connection_id,
            "requestId": request.get("requestId", ""),
            "state": "RUNNING",
        },
    )
    try:
        result = refresh_tables_impl(connection_id, request, data_root, job_id)
    except Exception as exc:
        error_fields = refresh_error_fields(exc)
        job.update({"state": "FAILED", **error_fields})
        save_job(data_root, job)
        try:
            post_refresh_callback(request, refresh_failure_callback_payload(job_id, connection_id, exc))
        except Exception as callback_exc:
            save_callback_error(data_root, job, callback_exc)
        raise RuntimeError(error_fields["message"]) from None

    job.update(success_job_fields(result))
    save_job(data_root, job)
    try:
        post_refresh_callback(request, refresh_success_callback_payload(result))
    except Exception as callback_exc:
        save_callback_error(data_root, job, callback_exc)
    return result


def prepare_refresh_tables_job(connection_id: str, request: dict, data_root: str | Path, job_id: str | None = None) -> dict:
    tables = request.get("tables") or []
    if not connection_id:
        raise ValueError("connection_id is required")
    if not tables:
        raise ValueError("tables is required")

    job_id = job_id or request.get("jobId") or str(uuid.uuid4())
    job = save_job(
        data_root,
        {
            "jobId": job_id,
            "jobType": TABLE_REFRESH,
            "connectionId": connection_id,
            "requestId": request.get("requestId", ""),
            "state": "ACCEPTED",
            "tableCount": len(tables),
        },
    )
    return {
        "jobId": job["jobId"],
        "jobType": TABLE_REFRESH,
        "connectionId": connection_id,
        "requestId": job.get("requestId", ""),
        "state": "ACCEPTED",
        "tableCount": len(tables),
    }


def run_refresh_tables_job(connection_id: str, request: dict, data_root: str | Path, job_id: str) -> None:
    job = save_job(
        data_root,
        {
            "jobId": job_id,
            "jobType": TABLE_REFRESH,
            "connectionId": connection_id,
            "requestId": request.get("requestId", ""),
            "state": "RUNNING",
            "tableCount": len(request.get("tables") or []),
        },
    )
    try:
        result = refresh_tables_impl(connection_id, request, data_root, job_id)
    except Exception as exc:
        job.update({"state": "FAILED", **refresh_error_fields(exc)})
        save_job(data_root, job)
        try:
            post_refresh_callback(request, refresh_failure_callback_payload(job_id, connection_id, exc))
        except Exception as callback_exc:
            save_callback_error(data_root, job, callback_exc)
        return

    job.update(success_job_fields(result))
    save_job(data_root, job)
    try:
        post_refresh_callback(request, refresh_success_callback_payload(result))
    except Exception as callback_exc:
        save_callback_error(data_root, job, callback_exc)
