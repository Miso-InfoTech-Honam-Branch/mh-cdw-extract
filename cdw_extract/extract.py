from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from .duck import connect
from .jobs import (
    ExportCancellation,
    JobCancelled,
    TERMINAL_STATES,
    cancellable_export,
    job_dir,
    load_job,
    save_job,
    update_job,
)
from .query import final_query
from .transforms.runtime import compile_pipeline_request
from .user_dataset import (
    dataset_file_manifest_path,
    dataset_file_parquet_path,
    parquet_columns,
    parquet_row_count,
    post_callback,
    publish_dataset_file_artifact,
    safe_segment,
    user_dataset_root,
    utc_now,
)


EXPORT = "EXPORT"
SUPPORTED_OUTPUT_FORMATS = {"parquet", "csv"}
RESULT_TARGET_KIND = "USER_DATST"


def normalize_result_target(request: dict) -> dict | None:
    raw_target = request.get("resultTarget")
    if raw_target is None:
        return None
    if not isinstance(raw_target, dict):
        raise ValueError("resultTarget must be a JSON object")

    kind = str(raw_target.get("kind") or RESULT_TARGET_KIND).strip().upper()
    if kind != RESULT_TARGET_KIND:
        raise ValueError(f"resultTarget.kind must be {RESULT_TARGET_KIND}")
    target = {
        "kind": kind,
        "userId": safe_segment(raw_target.get("userId"), "resultTarget.userId"),
        "userDatasetId": safe_segment(raw_target.get("userDatasetId"), "resultTarget.userDatasetId"),
        "userDatasetFileId": safe_segment(raw_target.get("userDatasetFileId"), "resultTarget.userDatasetFileId"),
        "idempotencyKey": str(raw_target.get("idempotencyKey") or "").strip(),
    }
    if not target["idempotencyKey"]:
        raise ValueError("resultTarget.idempotencyKey is required")
    if not str(request.get("datasetId") or "").strip():
        raise ValueError("datasetId is required when resultTarget is present")
    if not str(request.get("runId") or "").strip():
        raise ValueError("runId is required when resultTarget is present")
    return target


def validate_extract_request(connection_id: str, request: dict) -> str:
    if not str(connection_id or "").strip():
        raise ValueError("connection_id is required")
    if not isinstance(request, dict):
        raise ValueError("request must be a JSON object")

    output_format = (request.get("outputFormat") or "parquet").lower()
    if output_format not in SUPPORTED_OUTPUT_FORMATS:
        raise ValueError("outputFormat must be one of: parquet, csv")

    source_type = str(request.get("sourceType") or "").strip().lower()
    if source_type not in {"table", "join"}:
        raise ValueError("sourceType must be one of: table, join")

    target = normalize_result_target(request)
    if target is not None and output_format != "parquet":
        raise ValueError("resultTarget requires outputFormat=parquet")
    return output_format


def job_fields(connection_id: str, request: dict, job_id: str, state: str) -> dict:
    target = normalize_result_target(request)
    fields = {
        "jobId": job_id,
        "jobType": EXPORT,
        "connectionId": connection_id,
        "requestId": request.get("requestId", ""),
        "datasetId": request.get("datasetId", ""),
        "runId": request.get("runId", ""),
        "state": state,
    }
    if target is not None:
        fields.update(
            {
                "resultTarget": target,
                "resultUserId": target["userId"],
                "resultUserDatasetId": target["userDatasetId"],
                "resultUserDatasetFileId": target["userDatasetFileId"],
            }
        )
    return fields


def accepted_response(job: dict) -> dict:
    response = {
        "jobId": job["jobId"],
        "jobType": EXPORT,
        "connectionId": job["connectionId"],
        "requestId": job.get("requestId", ""),
        "datasetId": job.get("datasetId", ""),
        "runId": job.get("runId", ""),
        "state": "ACCEPTED",
    }
    if job.get("resultTarget") is not None:
        response["resultTarget"] = job["resultTarget"]
    return response


def prepare_extract_job(
    connection_id: str,
    request: dict,
    data_root: str | Path,
    job_id: str | None = None,
) -> dict:
    output_format = validate_extract_request(connection_id, request)
    job_id = job_id or str(uuid.uuid4())
    job = save_job(
        data_root,
        {
            **job_fields(connection_id, request, job_id, "ACCEPTED"),
            "outputFormat": output_format,
        },
    )
    return accepted_response(job)


def execute_extract(
    connection_id: str,
    request: dict,
    data_root: str | Path,
    job_id: str,
    cancellation: ExportCancellation | None = None,
) -> dict:
    output_format = validate_extract_request(connection_id, request)
    target = normalize_result_target(request)
    temporary_result_root: Path | None = None
    published_new = False

    if target is None:
        out_dir = job_dir(data_root, job_id)
        out_dir.mkdir(parents=True, exist_ok=True)
        suffix = "csv" if output_format == "csv" else "parquet"
        output = out_dir / f"result.{suffix}"
        staged_output = out_dir / f"result.{suffix}.tmp"
    else:
        temporary_result_root = user_dataset_root(data_root) / "_tmp" / job_id
        shutil.rmtree(temporary_result_root, ignore_errors=True)
        staged_output = temporary_result_root / "artifact" / "parquet" / "data.parquet"
        output = dataset_file_parquet_path(
            data_root,
            target["userId"],
            target["userDatasetId"],
            target["userDatasetFileId"],
        )

    staged_output.parent.mkdir(parents=True, exist_ok=True)
    staged_output.unlink(missing_ok=True)
    conn = None
    compiled = None
    try:
        if cancellation is not None:
            cancellation.raise_if_requested()
        conn = connect(data_root, "extract", job_id)
        if cancellation is not None:
            cancellation.attach(conn)
            cancellation.raise_if_requested()
        copy_format = "CSV, HEADER true" if output_format == "csv" else "PARQUET"
        if request.get("pipeline"):
            compiled = compile_pipeline_request(connection_id, request, data_root, conn)
            conn.execute(
                f"CREATE TEMP TABLE __pipeline_result AS {compiled.sql}",
                compiled.parameters,
            )
            conn.execute(
                f"COPY __pipeline_result TO ? (FORMAT {copy_format})",
                [staged_output.as_posix()],
            )
        else:
            sql = final_query(connection_id, data_root, request)
            conn.execute(f"COPY ({sql}) TO ? (FORMAT {copy_format})", [staged_output.as_posix()])
        if cancellation is not None:
            cancellation.raise_if_requested()

        if target is None:
            row_count = conn.execute(
                "SELECT count(*) FROM read_csv_auto(?)" if output_format == "csv" else "SELECT count(*) FROM read_parquet(?)",
                [staged_output.as_posix()],
            ).fetchone()[0]
            if cancellation is not None:
                cancellation.raise_if_requested()
            staged_output.replace(output)
            if cancellation is not None:
                cancellation.raise_if_requested()
            return {
                "outputFormat": output_format,
                "filePath": output.as_posix(),
                "filePaths": [output.as_posix()],
                "rowCount": int(row_count),
            }

        row_count = parquet_row_count(staged_output, connection=conn)
        columns = parquet_columns(staged_output, connection=conn)
        if compiled is not None:
            for index,column in enumerate(columns):
                if index >= len(compiled.output_schema):
                    break
                output_column=compiled.output_schema[index]
                column["originalName"]=column.get("originalName") or column.get("name")
                column["name"]=output_column.label
        if cancellation is not None:
            cancellation.raise_if_requested()
        manifest = publish_dataset_file_artifact(
            staged_output,
            data_root,
            target["userId"],
            target["userDatasetId"],
            target["userDatasetFileId"],
            {
                "manifestVersion": 1,
                "artifactKind": "EXTRACT_RESULT",
                "requestId": request.get("requestId", ""),
                "jobId": job_id,
                "jobType": EXPORT,
                "extractDatasetId": request.get("datasetId", ""),
                "runId": request.get("runId", ""),
                "connectionId": connection_id,
                "idempotencyKey": target["idempotencyKey"],
                "rowCount": row_count,
                "columns": columns,
                "createdAt": utc_now(),
            },
        )
        # publish_dataset_file_artifact moves the staged artifact only when this
        # invocation wins publication. If an idempotent artifact already exists,
        # it returns that manifest and leaves our staged directory in place.
        published_new = not staged_output.parent.parent.exists()
        if cancellation is not None:
            cancellation.raise_if_requested()
        final_output = dataset_file_parquet_path(
            data_root,
            target["userId"],
            target["userDatasetId"],
            target["userDatasetFileId"],
        )
        final_manifest = dataset_file_manifest_path(
            data_root,
            target["userId"],
            target["userDatasetId"],
            target["userDatasetFileId"],
        )
        published_columns = manifest.get("columns") or columns
        published_row_count = int(manifest.get("rowCount") if manifest.get("rowCount") is not None else row_count)
        return {
            "outputFormat": "parquet",
            "filePath": final_output.as_posix(),
            "filePaths": [final_output.as_posix()],
            "manifestPath": final_manifest.as_posix(),
            "rowCount": published_row_count,
            "resultColumns": published_columns,
            "artifact": {
                "kind": "USER_DATASET_FILE",
                "userId": target["userId"],
                "userDatasetId": target["userDatasetId"],
                "userDatasetFileId": target["userDatasetFileId"],
                "format": "PARQUET",
                "path": manifest["path"],
                "manifestPath": manifest["manifestPath"],
                "rowCount": published_row_count,
                "sizeBytes": manifest.get("sizeBytes"),
                "sha256Checksum": manifest.get("sha256Checksum"),
                "columns": published_columns,
            },
            "_resultTarget": True,
            "_publishedNew": published_new,
        }
    except Exception as exc:
        staged_output.unlink(missing_ok=True)
        if target is not None and published_new and output.parent.parent.exists():
            shutil.rmtree(output.parent.parent, ignore_errors=True)
        if target is None and cancellation is not None and cancellation.requested.is_set():
            output.unlink(missing_ok=True)
        if cancellation is not None and cancellation.requested.is_set() and not isinstance(exc, JobCancelled):
            raise JobCancelled("Running DuckDB extract was interrupted.") from exc
        raise
    finally:
        if conn is not None:
            if cancellation is not None:
                cancellation.detach(conn)
            conn.close()
        if temporary_result_root is not None:
            shutil.rmtree(temporary_result_root, ignore_errors=True)


def callback_payload(job: dict) -> dict:
    file_paths = job.get("filePaths")
    if not file_paths and job.get("filePath"):
        file_paths = [job["filePath"]]
    return {
        "status": job.get("state"),
        "state": job.get("state"),
        "jobId": job.get("jobId"),
        "jobType": job.get("jobType", EXPORT),
        "requestId": job.get("requestId"),
        "datasetId": job.get("datasetId"),
        "runId": job.get("runId"),
        "rowCount": job.get("rowCount"),
        "duplicateCount": job.get("duplicateCount", 0),
        "filePath": job.get("filePath"),
        "filePaths": file_paths or [],
        "message": job.get("message"),
        "errorCode": job.get("errorCode"),
        "resultUserId": job.get("resultUserId"),
        "resultUserDatasetId": job.get("resultUserDatasetId"),
        "resultUserDatasetFileId": job.get("resultUserDatasetFileId"),
        "resultColumns": job.get("resultColumns") or [],
        "resultManifestPath": job.get("manifestPath") or (job.get("artifact") or {}).get("manifestPath"),
        "resultSha256": (job.get("artifact") or {}).get("sha256Checksum"),
        "artifact": job.get("artifact"),
    }


def save_terminal_job(data_root: str | Path, request: dict, job: dict) -> dict:
    saved = save_job(data_root, job)
    try:
        delivery = post_callback(request, callback_payload(saved))
    except Exception as exc:
        saved["callbackError"] = {
            "errorCode": type(exc).__name__,
            "message": str(exc),
            "occurredAt": utc_now(),
        }
        return save_job(data_root, saved)
    if delivery is not None:
        saved["callbackDelivery"] = {**delivery, "deliveredAt": utc_now()}
        saved.pop("callbackError", None)
        return save_job(data_root, saved)
    return saved


def _discard_extract_result(result: dict | None) -> None:
    if not result:
        return
    file_path = result.get("filePath")
    if not file_path:
        return
    path = Path(file_path)
    if result.get("_resultTarget"):
        if result.get("_publishedNew"):
            shutil.rmtree(path.parent.parent, ignore_errors=True)
    else:
        path.unlink(missing_ok=True)


def _cancelled_job(job: dict, connection_id: str, request: dict, job_id: str, message: str) -> dict:
    job.update(
        {
            **job_fields(connection_id, request, job_id, "CANCELLED"),
            "cancelSupported": True,
            "message": message,
        }
    )
    job.pop("error", None)
    job.pop("errorCode", None)
    return job


def run_extract_job(connection_id: str, request: dict, data_root: str | Path, job_id: str) -> None:
    with cancellable_export(job_id) as cancellation:
        job = update_job(
            data_root,
            job_id,
            lambda current: current
            if current.get("state") in TERMINAL_STATES
            else {**current, **job_fields(connection_id, request, job_id, "RUNNING")},
        )
        if job.get("state") != "RUNNING":
            return

        result: dict | None = None
        try:
            result = execute_extract(connection_id, request, data_root, job_id, cancellation)
        except Exception as exc:
            if cancellation.requested.is_set() or isinstance(exc, JobCancelled):
                terminal = update_job(
                    data_root,
                    job_id,
                    lambda current: _cancelled_job(
                        current,
                        connection_id,
                        request,
                        job_id,
                        "Running extract was cancelled.",
                    ),
                )
            else:
                terminal = update_job(
                    data_root,
                    job_id,
                    lambda current: {
                        **current,
                        "state": "FAILED",
                        "errorCode": type(exc).__name__,
                        "error": str(exc),
                        "message": str(exc),
                    },
                )
            save_terminal_job(data_root, request, terminal)
            return

        def finish(current: dict) -> dict:
            if cancellation.requested.is_set():
                return _cancelled_job(
                    current,
                    connection_id,
                    request,
                    job_id,
                    "Running extract was cancelled before completion was committed.",
                )
            public_result = {key: value for key, value in (result or {}).items() if not key.startswith("_")}
            return {**current, "state": "COMPLETED", **public_result}

        terminal = update_job(data_root, job_id, finish)
        if terminal.get("state") == "CANCELLED":
            _discard_extract_result(result)
        save_terminal_job(data_root, request, terminal)


def extract(connection_id: str, request: dict, data_root: str | Path, job_id: str | None = None) -> dict:
    output_format = validate_extract_request(connection_id, request)
    job_id = job_id or str(uuid.uuid4())
    try:
        job = load_job(data_root, job_id)
    except FileNotFoundError:
        job = {}
    job.update(
        {
            **job_fields(connection_id, request, job_id, "RUNNING"),
            "outputFormat": output_format,
        }
    )
    job = save_job(data_root, job)
    try:
        result = execute_extract(connection_id, request, data_root, job_id)
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
        raise

    public_result = {key: value for key, value in result.items() if not key.startswith("_")}
    job.update({"state": "COMPLETED", **public_result})
    return save_job(data_root, job)
