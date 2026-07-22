from __future__ import annotations

import csv
import hashlib
import json
import shutil
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests

from .callback import callback_options as normalized_callback_options, post_json_callback
from .contracts import ResourceBudget
from .errors import ResourceLimitExceeded
from openpyxl import load_workbook

from .duck import connect, sql_literal

USER_DATASET_DIR = "user-datasets"
USER_DATASET_CONVERT = "USER_DATASET_CONVERT"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_segment(value: object, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    if any(ch in text for ch in {"/", "\\", "\x00"}):
        raise ValueError(f"{field_name} must not contain path separators")
    if text in {".", ".."}:
        raise ValueError(f"{field_name} is invalid")
    return text


def user_dataset_root(data_root: str | Path) -> Path:
    return Path(data_root) / USER_DATASET_DIR


def dataset_file_root(data_root: str | Path, user_id: str, user_dataset_id: str, user_dataset_file_id: str) -> Path:
    return (
        user_dataset_root(data_root)
        / safe_segment(user_id, "userId")
        / safe_segment(user_dataset_id, "userDatasetId")
        / "files"
        / safe_segment(user_dataset_file_id, "userDatasetFileId")
    )


def dataset_file_parquet_path(data_root: str | Path, user_id: str, user_dataset_id: str, user_dataset_file_id: str) -> Path:
    return dataset_file_root(data_root, user_id, user_dataset_id, user_dataset_file_id) / "parquet" / "data.parquet"


def dataset_file_manifest_path(data_root: str | Path, user_id: str, user_dataset_id: str, user_dataset_file_id: str) -> Path:
    return dataset_file_root(data_root, user_id, user_dataset_id, user_dataset_file_id) / "meta" / "manifest.json"


def dataset_file_relative_path(user_id: str, user_dataset_id: str, user_dataset_file_id: str) -> str:
    return (
        f"{USER_DATASET_DIR}/{safe_segment(user_id, 'userId')}/"
        f"{safe_segment(user_dataset_id, 'userDatasetId')}/files/"
        f"{safe_segment(user_dataset_file_id, 'userDatasetFileId')}/parquet/data.parquet"
    )


def dataset_file_manifest_relative_path(user_id: str, user_dataset_id: str, user_dataset_file_id: str) -> str:
    return (
        f"{USER_DATASET_DIR}/{safe_segment(user_id, 'userId')}/"
        f"{safe_segment(user_dataset_id, 'userDatasetId')}/files/"
        f"{safe_segment(user_dataset_file_id, 'userDatasetFileId')}/meta/manifest.json"
    )


def load_dataset_file_manifest(data_root: str | Path, user_id: str, user_dataset_id: str, user_dataset_file_id: str) -> dict:
    path = dataset_file_manifest_path(data_root, user_id, user_dataset_id, user_dataset_file_id)
    if not path.exists():
        raise FileNotFoundError(f"user dataset file manifest not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def save_dataset_file_manifest(
    data_root: str | Path,
    user_id: str,
    user_dataset_id: str,
    user_dataset_file_id: str,
    manifest: dict,
) -> dict:
    path = dataset_file_manifest_path(data_root, user_id, user_dataset_id, user_dataset_file_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    schema_path = path.parent / "schema.json"
    schema_path.write_text(json.dumps(manifest.get("columns") or [], ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def publish_dataset_file_artifact(
    staged_parquet_path: Path,
    data_root: str | Path,
    user_id: str,
    user_dataset_id: str,
    user_dataset_file_id: str,
    manifest: dict,
) -> dict:
    """Atomically publishes Parquet, manifest, and schema as one USER_DATST artifact."""
    if not staged_parquet_path.exists():
        raise FileNotFoundError(f"staged Parquet file not found: {staged_parquet_path}")

    staged_file_root = staged_parquet_path.parent.parent
    final_file_root = dataset_file_root(data_root, user_id, user_dataset_id, user_dataset_file_id)
    final_manifest_path = dataset_file_manifest_path(data_root, user_id, user_dataset_id, user_dataset_file_id)
    normalized_manifest = {
        **manifest,
        "userId": safe_segment(user_id, "userId"),
        "userDatasetId": safe_segment(user_dataset_id, "userDatasetId"),
        "userDatasetFileId": safe_segment(user_dataset_file_id, "userDatasetFileId"),
        "path": dataset_file_relative_path(user_id, user_dataset_id, user_dataset_file_id),
        "manifestPath": dataset_file_manifest_relative_path(user_id, user_dataset_id, user_dataset_file_id),
        "fileType": "PARQUET",
        "sizeBytes": staged_parquet_path.stat().st_size,
        "sha256Checksum": file_sha256(staged_parquet_path),
        "status": "SUCCESS",
    }

    if final_file_root.exists():
        existing = load_dataset_file_manifest(data_root, user_id, user_dataset_id, user_dataset_file_id)
        same_key = existing.get("idempotencyKey") == normalized_manifest.get("idempotencyKey")
        same_checksum = existing.get("sha256Checksum") == normalized_manifest.get("sha256Checksum")
        if same_key and same_checksum and existing.get("status") == "SUCCESS":
            return existing
        raise FileExistsError(f"user dataset result target already exists: {final_file_root}")

    staged_meta_root = staged_file_root / "meta"
    staged_meta_root.mkdir(parents=True, exist_ok=True)
    (staged_meta_root / "manifest.json").write_text(
        json.dumps(normalized_manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (staged_meta_root / "schema.json").write_text(
        json.dumps(normalized_manifest.get("columns") or [], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    final_file_root.parent.mkdir(parents=True, exist_ok=True)
    staged_file_root.replace(final_file_root)
    if not final_manifest_path.exists():
        raise FileNotFoundError(f"published user dataset manifest not found: {final_manifest_path}")
    return normalized_manifest


def delete_user_dataset_file(data_root: str | Path, user_id: str, user_dataset_id: str, user_dataset_file_id: str) -> dict:
    root = dataset_file_root(data_root, user_id, user_dataset_id, user_dataset_file_id)
    shutil.rmtree(root, ignore_errors=True)
    return {
        "userId": safe_segment(user_id, "userId"),
        "userDatasetId": safe_segment(user_dataset_id, "userDatasetId"),
        "userDatasetFileId": safe_segment(user_dataset_file_id, "userDatasetFileId"),
        "state": "DELETED",
    }


def bool_option(value: object, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def normalize_delimiter(value: str | None) -> str:
    if value is None or value == "":
        return ","
    if value == "\\t":
        return "\t"
    return value


def normalize_request_options(request: dict | None) -> dict:
    request = request or {}
    options = request.get("options") if isinstance(request.get("options"), dict) else {}
    return {
        **request,
        "fileType": request.get("fileType") or options.get("fileType"),
        "headerYn": request.get("headerYn") if request.get("headerYn") is not None else options.get("header"),
        "delimiter": request.get("delimiter") or options.get("delimiter"),
        "fileEncoding": request.get("fileEncoding") or options.get("encoding"),
        "sheetName": request.get("sheetName") or options.get("sheetName"),
    }


def upload_suffix(filename: str | None, file_type: str | None = None) -> str:
    if file_type:
        normalized = file_type.strip().lower().lstrip(".")
        if normalized in {"csv", "xlsx", "parquet"}:
            return f".{normalized}"
        raise ValueError("fileType must be one of: CSV, XLSX, PARQUET")

    suffix = Path(filename or "").suffix.lower()
    if suffix not in {".csv", ".xlsx", ".parquet"}:
        raise ValueError("file type must be one of: csv, xlsx, parquet")
    return suffix


def copy_upload_file(upload: Any, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if hasattr(upload.file, "seek"):
        upload.file.seek(0)
    with output.open("wb") as target:
        shutil.copyfileobj(upload.file, target)


def unique_headers(values: list[Any]) -> list[str]:
    headers: list[str] = []
    seen: dict[str, int] = {}
    for index, value in enumerate(values, start=1):
        text = str(value).strip() if value is not None else ""
        name = text or f"column_{index}"
        count = seen.get(name, 0)
        seen[name] = count + 1
        headers.append(name if count == 0 else f"{name}_{count + 1}")
    return headers


def csv_cell(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return value


def padded_row(row: list[Any], width: int) -> list[Any]:
    values = list(row or [])
    values.extend([None] * (width - len(values)))
    return values[:width]


def generated_headers(width: int) -> list[str]:
    return [f"column_{index}" for index in range(1, width + 1)]


class _ByteBudgetWriter:
    def __init__(self, stream: Any, maximum_bytes: int | None, used_bytes: int = 0) -> None:
        self.stream = stream
        self.maximum_bytes = maximum_bytes
        self.used_bytes = used_bytes

    def write(self, value: str) -> int:
        encoded_size = len(value.encode("utf-8"))
        if self.maximum_bytes is not None and self.used_bytes + encoded_size > self.maximum_bytes:
            raise ResourceLimitExceeded(
                f"CSV/XLSX normalization exceeded resourceBudget.tempBytes={self.maximum_bytes}."
            )
        written = self.stream.write(value)
        self.used_bytes += encoded_size
        return written


def write_normalized_csv(
    rows: Any,
    output_path: Path,
    header: bool,
    max_temporary_bytes: int | None = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    spool_path = output_path.with_name(f".{output_path.name}.{uuid.uuid4().hex}.rows")
    staged_output_path = output_path.with_name(
        f".{output_path.name}.{uuid.uuid4().hex}.normalized"
    )
    first_row: list[Any] | None = None
    max_width = 0
    try:
        # The widest row determines the final header width.  Spool rows to
        # disk so a large CSV/XLSX never has to be retained in Python memory.
        with spool_path.open("w", newline="", encoding="utf-8") as spool:
            spool_budget = _ByteBudgetWriter(spool, max_temporary_bytes)
            spool_writer = csv.writer(spool_budget)
            for row in rows:
                values = list(row or [])
                if first_row is None:
                    if not values or not any(value is not None and value != "" for value in values):
                        continue
                    first_row = values
                max_width = max(max_width, len(values))
                spool_writer.writerow([csv_cell(value) for value in values])
            spooled_bytes = spool_budget.used_bytes

        if first_row is None:
            raise ValueError("file has no rows")

        headers = (
            unique_headers(padded_row(first_row, max_width))
            if header
            else generated_headers(max_width)
        )
        with spool_path.open("r", newline="", encoding="utf-8") as spool, staged_output_path.open(
            "w", newline="", encoding="utf-8"
        ) as output:
            reader = csv.reader(spool)
            output_budget = _ByteBudgetWriter(output, max_temporary_bytes, spooled_bytes)
            writer = csv.writer(output_budget)
            writer.writerow(headers)
            for index, row in enumerate(reader):
                if header and index == 0:
                    continue
                writer.writerow(padded_row(row, max_width))
        # A failed budget check or encoding/write error must never leave a
        # partial normalized file at the caller-visible staging path.
        staged_output_path.replace(output_path)
    finally:
        spool_path.unlink(missing_ok=True)
        staged_output_path.unlink(missing_ok=True)


def csv_to_csv(
    input_path: Path,
    output_path: Path,
    delimiter: str,
    encoding: str | None,
    header: bool,
    max_temporary_bytes: int | None = None,
) -> None:
    selected_encoding = encoding or "utf-8-sig"
    with input_path.open("r", newline="", encoding=selected_encoding) as file:
        reader = csv.reader(file, delimiter=delimiter)
        write_normalized_csv(reader, output_path, header, max_temporary_bytes)


def xlsx_to_csv(
    input_path: Path,
    output_path: Path,
    sheet_name: str | None = None,
    header: bool = True,
    max_temporary_bytes: int | None = None,
) -> None:
    workbook = load_workbook(input_path, read_only=True, data_only=True)
    try:
        if sheet_name:
            if sheet_name not in workbook.sheetnames:
                raise ValueError(f"sheetName not found: {sheet_name}")
            sheet = workbook[sheet_name]
        else:
            sheet = workbook[workbook.sheetnames[0]]
        rows = sheet.iter_rows(values_only=True)
        write_normalized_csv(rows, output_path, header, max_temporary_bytes)
    finally:
        workbook.close()


def source_sql_for_upload(input_path: Path, suffix: str) -> str:
    path = sql_literal(input_path.as_posix())
    if suffix == ".parquet":
        return f"read_parquet({path})"
    return f"read_csv_auto({path}, HEADER=TRUE)"


def write_parquet_from_upload(
    input_path: Path,
    suffix: str,
    output_path: Path,
    data_root: str | Path | None = None,
    operation_id: object | None = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(data_root, "user-dataset-convert", operation_id)
    try:
        conn.execute(
            f"COPY (SELECT * FROM {source_sql_for_upload(input_path, suffix)}) TO ? (FORMAT PARQUET)",
            [output_path.as_posix()],
        )
    finally:
        conn.close()


def parquet_columns(
    output_path: Path,
    data_root: str | Path | None = None,
    operation_id: object | None = None,
    connection=None,
) -> list[dict]:
    conn = connection or connect(data_root, "parquet-schema", operation_id)
    try:
        rows = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet({sql_literal(output_path.as_posix())})"
        ).fetchall()
    finally:
        if connection is None:
            conn.close()
    return [
        {
            "originalName": row[0],
            "name": row[0],
            "type": row[1],
            "ordinal": index,
            "nullable": True,
        }
        for index, row in enumerate(rows, start=1)
    ]


def parquet_row_count(
    output_path: Path,
    data_root: str | Path | None = None,
    operation_id: object | None = None,
    connection=None,
) -> int:
    conn = connection or connect(data_root, "parquet-count", operation_id)
    try:
        return int(conn.execute("SELECT count(*) FROM read_parquet(?)", [output_path.as_posix()]).fetchone()[0])
    finally:
        if connection is None:
            conn.close()


def callback_options(request: dict) -> dict:
    return normalized_callback_options(request)


def post_callback(request: dict, payload: dict) -> dict | None:
    delivery = post_json_callback(
        callback_options(request),
        payload,
        operation="user dataset",
        post=requests.post,
    )
    if delivery is not None:
        delivery.pop("attempts", None)
    return delivery


def convert_user_dataset_file_from_path(
    input_upload_path: Path,
    data_root: str | Path,
    request: dict,
    *,
    budget: ResourceBudget | None = None,
    workspace: Path | None = None,
) -> dict:
    request = normalize_request_options(request)
    user_id = safe_segment(request.get("userId"), "userId")
    user_dataset_id = safe_segment(request.get("userDatasetId"), "userDatasetId")
    user_dataset_file_id = safe_segment(request.get("userDatasetFileId"), "userDatasetFileId")
    original_file_name = request.get("originalFileName") or input_upload_path.name
    suffix = upload_suffix(original_file_name, request.get("fileType"))
    header = bool_option(request.get("headerYn"), default=True)
    delimiter = normalize_delimiter(request.get("delimiter"))
    encoding = request.get("fileEncoding")
    sheet_name = request.get("sheetName")
    job_id = str(request.get("jobId") or uuid.uuid4())
    request_id = str(request.get("requestId") or user_dataset_file_id)
    tmp_root = Path(workspace).resolve() if workspace is not None else user_dataset_root(data_root) / "_tmp" / job_id
    final_parquet_path = dataset_file_parquet_path(data_root, user_id, user_dataset_id, user_dataset_file_id)
    staged_parquet_path = tmp_root / "parquet" / "data.parquet"

    try:
        if suffix == ".xlsx":
            csv_path = tmp_root / "upload.csv"
            xlsx_to_csv(
                input_upload_path,
                csv_path,
                sheet_name=sheet_name,
                header=header,
                max_temporary_bytes=budget.temp_bytes if budget is not None else None,
            )
            input_path = csv_path
            input_suffix = ".csv"
        elif suffix == ".csv":
            csv_path = tmp_root / "upload.normalized.csv"
            csv_to_csv(
                input_upload_path,
                csv_path,
                delimiter=delimiter,
                encoding=encoding,
                header=header,
                max_temporary_bytes=budget.temp_bytes if budget is not None else None,
            )
            input_path = csv_path
            input_suffix = ".csv"
        else:
            input_path = input_upload_path
            input_suffix = suffix

        write_parquet_from_upload(input_path, input_suffix, staged_parquet_path, data_root, job_id)
        row_count = parquet_row_count(staged_parquet_path, data_root, job_id)
        if budget is not None and budget.row_limit is not None and row_count > budget.row_limit:
            raise ResourceLimitExceeded(
                f"User dataset output exceeded resourceBudget.rowLimit={budget.row_limit}."
            )
        output_size = staged_parquet_path.stat().st_size
        if budget is not None and budget.output_bytes is not None and output_size > budget.output_bytes:
            raise ResourceLimitExceeded(
                f"User dataset output exceeded resourceBudget.outputBytes={budget.output_bytes}."
            )
        if budget is not None:
            temporary_size = sum(
                path.stat().st_size for path in tmp_root.rglob("*") if path.is_file()
            )
            if temporary_size > budget.temp_bytes:
                raise ResourceLimitExceeded(
                    f"User dataset staging exceeded resourceBudget.tempBytes={budget.temp_bytes}."
                )
        columns = parquet_columns(staged_parquet_path, data_root, job_id)
        manifest = {
            "requestId": request_id,
            "jobId": job_id,
            "jobType": USER_DATASET_CONVERT,
            "userId": user_id,
            "userDatasetId": user_dataset_id,
            "userDatasetFileId": user_dataset_file_id,
            "originalFileName": original_file_name,
            "path": dataset_file_relative_path(user_id, user_dataset_id, user_dataset_file_id),
            "rowCount": row_count,
            "columns": columns,
            "fileType": suffix.lstrip(".").upper(),
            "headerYn": header,
            "delimiter": delimiter if suffix == ".csv" else None,
            "fileEncoding": encoding,
            "sheetName": sheet_name,
            "status": "SUCCESS",
            "createdAt": utc_now(),
        }
        final_parquet_path.parent.mkdir(parents=True, exist_ok=True)
        staged_parquet_path.replace(final_parquet_path)
        save_dataset_file_manifest(data_root, user_id, user_dataset_id, user_dataset_file_id, manifest)
        return {
            "requestId": request_id,
            "jobId": job_id,
            "jobType": USER_DATASET_CONVERT,
            "userId": user_id,
            "userDatasetId": user_dataset_id,
            "userDatasetFileId": user_dataset_file_id,
            "status": "SUCCESS",
            "rowCount": row_count,
            "columns": columns,
            "errorCode": None,
            "message": None,
        }
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)


def convert_user_dataset_file(upload: Any, data_root: str | Path, request: dict) -> dict:
    request = normalize_request_options(request)
    job_id = str(request.get("jobId") or uuid.uuid4())
    tmp_root = user_dataset_root(data_root) / "_tmp" / job_id
    suffix = upload_suffix(getattr(upload, "filename", None), request.get("fileType"))
    upload_path = tmp_root / f"upload{suffix}"
    copy_upload_file(upload, upload_path)
    request = {**request, "jobId": job_id, "originalFileName": request.get("originalFileName") or getattr(upload, "filename", None)}
    return convert_user_dataset_file_from_path(upload_path, data_root, request)
