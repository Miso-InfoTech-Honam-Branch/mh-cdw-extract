from __future__ import annotations

import os
import json

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile, status
from fastapi.responses import FileResponse

from cdw_extract.analytics import run_analytics_detail, run_analytics_query
from cdw_extract.analytics_artifacts import (
    analysis_artifact_download,
    delete_analysis_artifact,
    prepare_analysis_artifact_job,
    run_analysis_artifact_job,
)
from cdw_extract.analytics_models import (
    AnalyticsArtifactAccepted,
    AnalyticsArtifactRequest,
    AnalyticsDetailRequest,
    AnalyticsDetailResponse,
    AnalyticsQueryRequest,
    AnalyticsQueryResponse,
)
from cdw_extract import (
    cancel_job,
    delete_connection,
    delete_user_dataset_file,
    job_download_file,
    load_job,
    preview,
    prepare_extract_job,
    prepare_refresh_tables_job,
    refresh_tables,
    run_extract_job,
    run_refresh_tables_job,
)
from cdw_extract.config import data_root, load_dotenv
from cdw_extract.user_dataset_jobs import prepare_user_dataset_convert_job, run_user_dataset_convert_job

load_dotenv()

app = FastAPI(title="newExtract function test API")


def root_path() -> str:
    return str(data_root())


def to_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, FileNotFoundError):
        status_code = status.HTTP_404_NOT_FOUND
    elif isinstance(exc, TimeoutError):
        status_code = status.HTTP_408_REQUEST_TIMEOUT
    else:
        status_code = status.HTTP_400_BAD_REQUEST
    return HTTPException(status_code=status_code, detail=str(exc))


def parse_request_form(value: str | None) -> dict:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError("request must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("request must be a JSON object")
    return parsed


@app.get("/health")
async def health() -> dict:
    return {"status": "UP"}


@app.post(
    "/api/v1/analytics/query",
    response_model=AnalyticsQueryResponse,
    status_code=status.HTTP_200_OK,
)
def analytics_query_route(request: AnalyticsQueryRequest) -> AnalyticsQueryResponse:
    try:
        return run_analytics_query(request, root_path())
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.post(
    "/api/v1/analytics/detail",
    response_model=AnalyticsDetailResponse,
    status_code=status.HTTP_200_OK,
)
def analytics_detail_route(request: AnalyticsDetailRequest) -> AnalyticsDetailResponse:
    try:
        return run_analytics_detail(request, root_path())
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.post(
    "/api/v1/analytics/artifacts",
    response_model=AnalyticsArtifactAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
def create_analysis_artifact_route(
    request: AnalyticsArtifactRequest,
    background_tasks: BackgroundTasks,
) -> AnalyticsArtifactAccepted:
    try:
        accepted = prepare_analysis_artifact_job(request, root_path())
        background_tasks.add_task(run_analysis_artifact_job, request, root_path())
        return AnalyticsArtifactAccepted.model_validate(accepted)
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.get("/api/v1/analytics/artifacts/{userId}/{artifactId}/download")
def download_analysis_artifact_route(userId: str, artifactId: str) -> FileResponse:
    try:
        path, manifest = analysis_artifact_download(root_path(), userId, artifactId)
        return FileResponse(
            path,
            media_type=manifest["contentType"],
            filename=manifest["fileName"],
        )
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.delete("/api/v1/analytics/artifacts/{userId}/{artifactId}")
def delete_analysis_artifact_route(userId: str, artifactId: str) -> dict:
    try:
        return delete_analysis_artifact(root_path(), userId, artifactId)
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.post("/api/v1/connections/{connection_id}/tables/refresh-sync")
def refresh_connection_sync(connection_id: str, request: dict) -> dict:
    try:
        return refresh_tables(connection_id, request, root_path())
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.post("/api/v1/connections/{connection_id}/tables/refresh")
def refresh_connection_alias(connection_id: str, background_tasks: BackgroundTasks, request: dict) -> dict:
    try:
        accepted = prepare_refresh_tables_job(connection_id, request, root_path())
        background_tasks.add_task(run_refresh_tables_job, connection_id, request, root_path(), accepted["jobId"])
        return accepted
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.post("/api/v1/connections/{connection_id}/preview")
def preview_connection_route(connection_id: str, request: dict) -> dict:
    try:
        return preview(connection_id, request, root_path())
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.post("/api/v1/connections/{connection_id}/extracts", status_code=status.HTTP_202_ACCEPTED)
def extract_connection_route(connection_id: str, background_tasks: BackgroundTasks, request: dict) -> dict:
    try:
        extract_root = root_path()
        accepted = prepare_extract_job(connection_id, request, extract_root)
        background_tasks.add_task(run_extract_job, connection_id, request, extract_root, accepted["jobId"])
        return accepted
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.post("/api/v1/connections/{connection_id}/delete")
def delete_connection_route(connection_id: str) -> dict:
    try:
        return delete_connection(connection_id, root_path())
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.post("/api/v1/user-datasets/{user_dataset_id}/files/{user_dataset_file_id}/convert")
def convert_user_dataset_file_route(
    user_dataset_id: str,
    user_dataset_file_id: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    request_json: str | None = Form(None, alias="request"),
) -> dict:
    try:
        request = parse_request_form(request_json)
        accepted, upload_path, normalized_request = prepare_user_dataset_convert_job(
            file,
            root_path(),
            user_dataset_id,
            user_dataset_file_id,
            request,
        )
        if upload_path is not None:
            background_tasks.add_task(run_user_dataset_convert_job, upload_path, root_path(), normalized_request)
        return accepted
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.delete("/api/v1/user-datasets/{user_id}/{user_dataset_id}/files/{user_dataset_file_id}")
def delete_user_dataset_file_route(user_id: str, user_dataset_id: str, user_dataset_file_id: str) -> dict:
    try:
        return delete_user_dataset_file(root_path(), user_id, user_dataset_id, user_dataset_file_id)
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.get("/api/v1/jobs/{job_id}")
def get_job_route(job_id: str) -> dict:
    try:
        return load_job(root_path(), job_id)
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.post("/api/v1/jobs/{job_id}/cancel")
def cancel_job_route(job_id: str) -> dict:
    try:
        return cancel_job(root_path(), job_id)
    except Exception as exc:
        raise to_http_error(exc) from exc


@app.get("/api/v1/jobs/{job_id}/download")
def download_job_route(job_id: str) -> FileResponse:
    try:
        path, job = job_download_file(root_path(), job_id)
        output_format = job.get("outputFormat") or path.suffix.lstrip(".") or "parquet"
        media_type = "text/csv" if output_format == "csv" else "application/octet-stream"
        return FileResponse(path, media_type=media_type, filename=f"{job_id}.{output_format}")
    except Exception as exc:
        raise to_http_error(exc) from exc


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8091")))
