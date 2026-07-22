"""사용자 데이터셋 변환의 비동기 작업 상태와 콜백을 관리한다."""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

from .jobs import create_job, job_failure_fields, normalize_job_id, save_job, update_job
from .user_dataset import (
    USER_DATASET_CONVERT,
    convert_user_dataset_file_from_path,
    copy_upload_file,
    post_callback,
    safe_segment,
    upload_suffix,
    user_dataset_root,
)


def user_dataset_failed_payload(request: dict, exc: Exception) -> dict:
    """업로드 변환 예외를 Boot 호환 실패 콜백 본문으로 변환한다."""

    return {
        "requestId": request.get("requestId") or request.get("userDatasetFileId"),
        "jobId": request.get("jobId"),
        "jobType": USER_DATASET_CONVERT,
        "userId": request.get("userId"),
        "userDatasetId": request.get("userDatasetId"),
        "userDatasetFileId": request.get("userDatasetFileId"),
        "status": "FAILED",
        "rowCount": None,
        "columns": [],
        "errorCode": type(exc).__name__,
        "message": str(exc),
    }


def prepare_user_dataset_convert_job(
    upload: Any,
    data_root: str | Path,
    user_dataset_id: str,
    user_dataset_file_id: str,
    request: dict,
) -> tuple[dict, Path | None, dict]:
    """업로드를 한 번만 저장하고 멱등적인 ACCEPTED 변환 작업을 준비한다."""

    job_id = normalize_job_id(str(request.get("jobId") or uuid.uuid4()))
    user_id = safe_segment(request.get("userId"), "userId")
    user_dataset_id = safe_segment(user_dataset_id, "userDatasetId")
    user_dataset_file_id = safe_segment(user_dataset_file_id, "userDatasetFileId")
    normalized_request = {
        **request,
        "jobId": job_id,
        "requestId": request.get("requestId") or user_dataset_file_id,
        "userId": user_id,
        "userDatasetId": user_dataset_id,
        "userDatasetFileId": user_dataset_file_id,
        "originalFileName": request.get("originalFileName") or getattr(upload, "filename", None),
    }
    suffix = upload_suffix(getattr(upload, "filename", None), normalized_request.get("fileType"))
    accepted, created = create_job(
        data_root,
        {
            "jobId": job_id,
            "jobType": USER_DATASET_CONVERT,
            "requestId": normalized_request["requestId"],
            "userId": normalized_request.get("userId"),
            "userDatasetId": user_dataset_id,
            "userDatasetFileId": user_dataset_file_id,
            "state": "PREPARING",
        },
    )
    expected = {
        "jobType": USER_DATASET_CONVERT,
        "requestId": normalized_request["requestId"],
        "userId": user_id,
        "userDatasetId": user_dataset_id,
        "userDatasetFileId": user_dataset_file_id,
    }
    if any(str(accepted.get(key) or "") != str(value or "") for key, value in expected.items()):
        raise ValueError("jobId is already assigned to a different upload conversion")

    upload_path = user_dataset_root(data_root) / "_tmp" / job_id / f"upload{suffix}"
    if created:
        try:
            copy_upload_file(upload, upload_path)
            accepted = update_job(data_root, job_id, lambda current: {**current, "state": "ACCEPTED"})
        except Exception as exc:
            failure_fields = job_failure_fields(exc, include_error=True)
            update_job(
                data_root,
                job_id,
                lambda current: {
                    **current,
                    "state": "FAILED",
                    **failure_fields,
                },
            )
            raise
    return (
        {
            "requestId": accepted["requestId"],
            "jobId": accepted["jobId"],
            "jobType": USER_DATASET_CONVERT,
            "state": "ACCEPTED",
        },
        upload_path if created else None,
        normalized_request,
    )


def run_user_dataset_convert_job(upload_path: str | Path, data_root: str | Path, request: dict) -> None:
    """수락된 변환을 단일 워커가 선점해 상태 저장과 종단 콜백까지 수행한다."""

    job_id = request["jobId"]
    runner_token = uuid.uuid4().hex
    claimed = update_job(
        data_root,
        job_id,
        lambda current: {
            **current,
            "state": "RUNNING",
            "runnerToken": runner_token,
        }
        if current.get("state") == "ACCEPTED"
        else current,
    )
    if claimed.get("runnerToken") != runner_token:
        return
    try:
        result = convert_user_dataset_file_from_path(Path(upload_path), data_root, request)
        save_job(
            data_root,
            {
                "jobId": job_id,
                "jobType": USER_DATASET_CONVERT,
                "requestId": result.get("requestId"),
                "userId": result.get("userId"),
                "userDatasetId": result.get("userDatasetId"),
                "userDatasetFileId": result.get("userDatasetFileId"),
                "state": "SUCCESS",
                "rowCount": result.get("rowCount"),
            },
        )
        try:
            post_callback(request, result)
        except Exception as callback_exc:
            save_job(
                data_root,
                {
                    "jobId": job_id,
                    "jobType": USER_DATASET_CONVERT,
                    "requestId": result.get("requestId"),
                    "userId": result.get("userId"),
                    "userDatasetId": result.get("userDatasetId"),
                    "userDatasetFileId": result.get("userDatasetFileId"),
                    "state": "SUCCESS",
                    "rowCount": result.get("rowCount"),
                    "callbackError": str(callback_exc),
                },
            )
    except Exception as exc:
        payload = user_dataset_failed_payload(request, exc)
        save_job(
            data_root,
            {
                "jobId": job_id,
                "jobType": USER_DATASET_CONVERT,
                "requestId": request.get("requestId") or request.get("userDatasetFileId"),
                "userId": request.get("userId"),
                "userDatasetId": request.get("userDatasetId"),
                "userDatasetFileId": request.get("userDatasetFileId"),
                "state": "FAILED",
                "error": str(exc),
            },
        )
        try:
            post_callback(request, payload)
        except Exception:
            pass
