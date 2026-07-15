from __future__ import annotations

import shutil
import uuid
from pathlib import Path

import requests

from .clickhouse import write_clickhouse_table_parquet
from .duck import connect, source_attach_sql, source_table_sql
from .jobs import save_job
from .manifest import save_connection_manifest, utc_now
from .paths import connection_root, table_file_name

TABLE_REFRESH = "TABLE_REFRESH"


def callback_options(request: dict) -> dict:
    callback = request.get("callback") or {}
    if not isinstance(callback, dict):
        callback = {}
    if request.get("callbackUrl") and not callback.get("url"):
        callback = {**callback, "url": request["callbackUrl"]}
    return callback


def post_refresh_callback(request: dict, payload: dict) -> dict | None:
    callback = callback_options(request)
    url = callback.get("url")
    if not url:
        return None

    headers = callback.get("headers") or {}
    timeout = float(callback.get("timeoutSeconds") or callback.get("timeout") or 10)
    response = requests.post(url, json=payload, headers=headers, timeout=timeout)
    if response.status_code < 200 or response.status_code >= 300:
        message = response.text[:4096].strip()
        raise RuntimeError(f"refresh callback failed status={response.status_code} body={message}")
    return {"url": url, "statusCode": response.status_code}


def refresh_success_callback_payload(result: dict) -> dict:
    return {
        "jobId": result["jobId"],
        "jobType": TABLE_REFRESH,
        "connectionId": result["connectionId"],
        "state": "SUCCESS",
        "status": "SUCCESS",
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
        "errorCode": type(exc).__name__,
        "error": str(exc),
        "message": str(exc),
    }


def refresh_tables_impl(connection_id: str, request: dict, data_root: str | Path, job_id: str) -> dict:
    source = request.get("sourceConnection") or {}
    tables = request.get("tables") or []
    if not connection_id:
        raise ValueError("connection_id is required")
    if not tables:
        raise ValueError("tables is required")

    compression = ((request.get("options") or {}).get("compression") or "snappy").lower()
    root = connection_root(data_root, connection_id)
    tmp = root / "_tmp" / job_id / "tables"
    final_tables = root / "tables"
    tmp.mkdir(parents=True, exist_ok=True)

    vendor = (source.get("vendor") or "postgresql").lower()
    conn = None
    artifacts = []
    total_rows = 0
    try:
        if vendor != "clickhouse":
            conn = connect(data_root, "refresh", job_id)
            extension, attach_sql = source_attach_sql(source)
            conn.execute(f"INSTALL {extension}")
            conn.execute(f"LOAD {extension}")
            conn.execute(attach_sql)
        for table in tables:
            file_name = table_file_name(table)
            output = tmp / file_name
            if vendor == "clickhouse":
                row_count = write_clickhouse_table_parquet(source, table, output)
            else:
                if conn is None:
                    raise RuntimeError("duckdb connection is not initialized")
                select_list = "*"
                if table.get("columns"):
                    select_list = ", ".join(f'"{c["name"]}"' for c in table["columns"])
                source_sql = f"SELECT {select_list} FROM {source_table_sql(source, table)}"
                conn.execute(
                    f"COPY ({source_sql}) TO ? (FORMAT PARQUET, COMPRESSION {compression.upper()})",
                    [output.as_posix()],
                )
                row_count = conn.execute("SELECT count(*) FROM read_parquet(?)", [output.as_posix()]).fetchone()[0]
            total_rows += row_count
            artifacts.append(
                {
                    "tableId": table.get("tableId"),
                    "schemaName": table.get("schemaName") or source.get("schemaName"),
                    "tableName": table.get("tableName"),
                    "path": f"tables/{file_name}",
                    "rowCount": row_count,
                    "columns": table.get("columns") or [],
                }
            )
    finally:
        if conn is not None:
            conn.close()

    if final_tables.exists():
        shutil.rmtree(final_tables)
    final_tables.parent.mkdir(parents=True, exist_ok=True)
    tmp.replace(final_tables)
    extracts = root / "extracts"
    if extracts.exists():
        shutil.rmtree(extracts)

    manifest = {
        "connectionId": connection_id,
        "status": "COMPLETED",
        "tables": artifacts,
        "updatedAt": utc_now(),
    }
    save_connection_manifest(connection_id, data_root, manifest)
    return {
        "jobId": job_id,
        "jobType": TABLE_REFRESH,
        "connectionId": connection_id,
        "state": "COMPLETED",
        "tableCount": len(artifacts),
        "rowCount": total_rows,
        "tables": artifacts,
    }


def refresh_tables(connection_id: str, request: dict, data_root: str | Path, job_id: str | None = None) -> dict:
    job_id = job_id or str(uuid.uuid4())
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
        job.update({"state": "FAILED", "error": str(exc)})
        save_job(data_root, job)
        try:
            post_refresh_callback(request, refresh_failure_callback_payload(job_id, connection_id, exc))
        except Exception:
            pass
        raise

    job.update(
        {
            "state": "COMPLETED",
            "tableCount": result["tableCount"],
            "rowCount": result["rowCount"],
        }
    )
    save_job(data_root, job)
    post_refresh_callback(request, refresh_success_callback_payload(result))
    return result


def prepare_refresh_tables_job(connection_id: str, request: dict, data_root: str | Path, job_id: str | None = None) -> dict:
    tables = request.get("tables") or []
    if not connection_id:
        raise ValueError("connection_id is required")
    if not tables:
        raise ValueError("tables is required")

    job_id = job_id or str(uuid.uuid4())
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
        job.update(
            {
                "state": "FAILED",
                "errorCode": type(exc).__name__,
                "error": str(exc),
                "message": str(exc),
            }
        )
        save_job(data_root, job)
        try:
            post_refresh_callback(request, refresh_failure_callback_payload(job_id, connection_id, exc))
        except Exception:
            pass
        return

    job.update(
        {
            "state": "COMPLETED",
            "tableCount": result["tableCount"],
            "rowCount": result["rowCount"],
        }
    )
    save_job(data_root, job)
    try:
        post_refresh_callback(request, refresh_success_callback_payload(result))
    except Exception as exc:
        job.update({"callbackError": str(exc)})
        save_job(data_root, job)
