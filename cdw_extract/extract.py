"""추출 작업의 검증, 실행, 상태 저장, 콜백 전달을 조정한다."""

from __future__ import annotations

import shutil
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from .contracts import ResourceBudget
from .duck import connect, sql_literal
from .errors import ResourceLimitExceeded
from .jobs import (
    ExportCancellation,
    JobCancelled,
    TERMINAL_STATES,
    cancellable_export,
    job_dir,
    job_failure_fields,
    load_job,
    save_job,
    update_job,
)
from .query import final_query
from .transforms.compiler import CompiledPipeline
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


@dataclass(frozen=True, slots=True)
class ExtractResultTarget:
    """내부 추출 실행에서 사용하는 정규화된 사용자 데이터셋 목적지다."""

    kind: str
    user_id: str
    user_dataset_id: str
    user_dataset_file_id: str
    idempotency_key: str

    @classmethod
    def from_request(cls, request: dict) -> "ExtractResultTarget | None":
        raw_target = request.get("resultTarget")
        if raw_target is None:
            return None
        if not isinstance(raw_target, dict):
            raise ValueError("resultTarget must be a JSON object")

        kind = str(raw_target.get("kind") or RESULT_TARGET_KIND).strip().upper()
        if kind != RESULT_TARGET_KIND:
            raise ValueError(f"resultTarget.kind must be {RESULT_TARGET_KIND}")
        target = cls(
            kind=kind,
            user_id=safe_segment(raw_target.get("userId"), "resultTarget.userId"),
            user_dataset_id=safe_segment(
                raw_target.get("userDatasetId"),
                "resultTarget.userDatasetId",
            ),
            user_dataset_file_id=safe_segment(
                raw_target.get("userDatasetFileId"),
                "resultTarget.userDatasetFileId",
            ),
            idempotency_key=str(raw_target.get("idempotencyKey") or "").strip(),
        )
        if not target.idempotency_key:
            raise ValueError("resultTarget.idempotencyKey is required")
        if not str(request.get("datasetId") or "").strip():
            raise ValueError("datasetId is required when resultTarget is present")
        if not str(request.get("runId") or "").strip():
            raise ValueError("runId is required when resultTarget is present")
        return target

    def transport_dict(self) -> dict[str, str]:
        """기존 Boot 요청·작업 저장 형식의 camelCase 객체로 변환한다."""

        return {
            "kind": self.kind,
            "userId": self.user_id,
            "userDatasetId": self.user_dataset_id,
            "userDatasetFileId": self.user_dataset_file_id,
            "idempotencyKey": self.idempotency_key,
        }


@dataclass(frozen=True, slots=True)
class ValidatedExtractRequest:
    """외부 요청 경계에서 한 번 검증된 추출 실행의 핵심 값이다."""

    connection_id: str
    source_type: str
    output_format: str
    result_target: ExtractResultTarget | None

    @classmethod
    def from_raw(cls, connection_id: str, request: dict) -> "ValidatedExtractRequest":
        normalized_connection_id = str(connection_id or "").strip()
        if not normalized_connection_id:
            raise ValueError("connection_id is required")
        if not isinstance(request, dict):
            raise ValueError("request must be a JSON object")

        output_format = str(request.get("outputFormat") or "parquet").lower()
        if output_format not in SUPPORTED_OUTPUT_FORMATS:
            raise ValueError("outputFormat must be one of: parquet, csv")
        source_type = str(request.get("sourceType") or "").strip().lower()
        if source_type not in {"table", "join"}:
            raise ValueError("sourceType must be one of: table, join")

        result_target = ExtractResultTarget.from_request(request)
        if result_target is not None and output_format != "parquet":
            raise ValueError("resultTarget requires outputFormat=parquet")
        return cls(
            connection_id=normalized_connection_id,
            source_type=source_type,
            output_format=output_format,
            result_target=result_target,
        )


def normalize_result_target(request: dict) -> dict | None:
    """선택적 사용자 데이터셋 결과 목적지를 검증하고 정규화한다."""

    target = ExtractResultTarget.from_request(request)
    return target.transport_dict() if target is not None else None


def validate_extract_request(connection_id: str, request: dict) -> str:
    """추출 소스·출력 형식·결과 목적지 조합을 검증한다."""

    return ValidatedExtractRequest.from_raw(connection_id, request).output_format


def _job_fields(
    validated: ValidatedExtractRequest,
    request: dict,
    job_id: str,
    state: str,
) -> dict:
    fields = {
        "jobId": job_id,
        "jobType": EXPORT,
        "connectionId": validated.connection_id,
        "requestId": request.get("requestId", ""),
        "datasetId": request.get("datasetId", ""),
        "runId": request.get("runId", ""),
        "state": state,
    }
    target = validated.result_target
    if target is not None:
        fields.update(
            {
                "resultTarget": target.transport_dict(),
                "resultUserId": target.user_id,
                "resultUserDatasetId": target.user_dataset_id,
                "resultUserDatasetFileId": target.user_dataset_file_id,
            }
        )
    return fields


def job_fields(connection_id: str, request: dict, job_id: str, state: str) -> dict:
    """추출 작업 상태에 공통으로 저장할 식별자 필드를 만든다."""

    validated = ValidatedExtractRequest.from_raw(connection_id, request)
    return _job_fields(validated, request, job_id, state)


def accepted_response(job: dict) -> dict:
    """내부 작업 상태에서 공개 비동기 수락 응답을 만든다."""

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
    """추출 요청을 검증해 ACCEPTED 상태 작업을 준비한다."""

    validated = ValidatedExtractRequest.from_raw(connection_id, request)
    job_id = job_id or str(uuid.uuid4())
    job = save_job(
        data_root,
        {
            **_job_fields(validated, request, job_id, "ACCEPTED"),
            "outputFormat": validated.output_format,
        },
    )
    return accepted_response(job)


@dataclass(frozen=True, slots=True)
class _ExtractDestination:
    """staging 파일과 최종 게시 경로의 수명·정리 규칙을 함께 보관한다."""

    staged_path: Path
    final_path: Path
    temporary_root: Path | None
    result_target: ExtractResultTarget | None

    @classmethod
    def create(
        cls,
        data_root: str | Path,
        job_id: str,
        request: ValidatedExtractRequest,
    ) -> "_ExtractDestination":
        target = request.result_target
        if target is None:
            output_root = job_dir(data_root, job_id)
            suffix = "csv" if request.output_format == "csv" else "parquet"
            final_path = output_root / f"result.{suffix}"
            return cls(
                staged_path=output_root / f"result.{suffix}.tmp",
                final_path=final_path,
                temporary_root=None,
                result_target=None,
            )

        temporary_root = user_dataset_root(data_root) / "_tmp" / job_id
        return cls(
            staged_path=temporary_root / "artifact" / "parquet" / "data.parquet",
            final_path=dataset_file_parquet_path(
                data_root,
                target.user_id,
                target.user_dataset_id,
                target.user_dataset_file_id,
            ),
            temporary_root=temporary_root,
            result_target=target,
        )

    def prepare(self) -> None:
        if self.temporary_root is not None:
            shutil.rmtree(self.temporary_root, ignore_errors=True)
        self.staged_path.parent.mkdir(parents=True, exist_ok=True)
        self.staged_path.unlink(missing_ok=True)

    def cleanup_failed(self, *, published_new: bool, cancelled: bool) -> None:
        self.staged_path.unlink(missing_ok=True)
        if self.result_target is not None:
            if published_new and self.final_path.parent.parent.exists():
                shutil.rmtree(self.final_path.parent.parent, ignore_errors=True)
        elif cancelled:
            self.final_path.unlink(missing_ok=True)

    def cleanup_temporary(self) -> None:
        if self.temporary_root is not None:
            shutil.rmtree(self.temporary_root, ignore_errors=True)


@dataclass(frozen=True, slots=True)
class _CopyOutcome:
    row_count: int | None
    compiled_pipeline: CompiledPipeline | None


@dataclass(frozen=True, slots=True)
class _DatasetPublication:
    result: dict[str, Any]
    published_new: bool


def _raise_if_cancelled(cancellation: ExportCancellation | None) -> None:
    if cancellation is not None:
        cancellation.raise_if_requested()


@contextmanager
def _extract_connection(
    data_root: str | Path,
    job_id: str,
    cancellation: ExportCancellation | None,
) -> Iterator[Any]:
    """DuckDB 연결과 취소 인터럽트 등록을 같은 수명 경계에서 정리한다."""

    _raise_if_cancelled(cancellation)
    connection = connect(data_root, "extract", job_id)
    try:
        if cancellation is not None:
            cancellation.attach(connection)
        _raise_if_cancelled(cancellation)
        yield connection
    finally:
        if cancellation is not None:
            cancellation.detach(connection)
        connection.close()


def _limit_query(sql: str, budget: ResourceBudget | None) -> str:
    if budget is None or budget.row_limit is None:
        return sql
    # 제한값보다 한 행 더 기록해 전체 결과를 다시 세지 않고도 초과를 판별한다.
    return f"SELECT * FROM ({sql}) AS __budget_limited LIMIT {budget.row_limit + 1}"


def _copy_row_count(copy_result: Any) -> int | None:
    row = copy_result.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _copy_to_staging(
    connection: Any,
    validated: ValidatedExtractRequest,
    request: dict,
    data_root: str | Path,
    destination: _ExtractDestination,
    budget: ResourceBudget | None,
) -> _CopyOutcome:
    copy_format = "CSV, HEADER true" if validated.output_format == "csv" else "PARQUET"
    if request.get("pipeline"):
        compiled = compile_pipeline_request(
            validated.connection_id,
            request,
            data_root,
            connection,
        )
        copy_result = connection.execute(
            f"COPY ({_limit_query(compiled.sql, budget)}) "
            f"TO {sql_literal(destination.staged_path.as_posix())} (FORMAT {copy_format})",
            compiled.parameters,
        )
        return _CopyOutcome(_copy_row_count(copy_result), compiled)

    query = final_query(validated.connection_id, data_root, request)
    copy_result = connection.execute(
        f"COPY ({_limit_query(query, budget)}) TO ? (FORMAT {copy_format})",
        [destination.staged_path.as_posix()],
    )
    return _CopyOutcome(_copy_row_count(copy_result), None)


def _require_staged_output_within_budget(
    destination: _ExtractDestination,
    copied_row_count: int | None,
    budget: ResourceBudget | None,
) -> None:
    if budget is None:
        return
    if budget.row_limit is not None and copied_row_count is not None:
        if copied_row_count > budget.row_limit:
            raise ResourceLimitExceeded(
                f"Extract output exceeded resourceBudget.rowLimit={budget.row_limit}."
            )
    if (
        budget.output_bytes is not None
        and destination.staged_path.stat().st_size > budget.output_bytes
    ):
        raise ResourceLimitExceeded(
            f"Extract output exceeded resourceBudget.outputBytes={budget.output_bytes}."
        )


def _resolved_row_count(
    connection: Any,
    destination: _ExtractDestination,
    output_format: str,
    copied_row_count: int | None,
) -> int:
    if copied_row_count is not None:
        return copied_row_count
    if destination.result_target is not None:
        return int(parquet_row_count(destination.staged_path, connection=connection))
    count_query = (
        "SELECT count(*) FROM read_csv_auto(?)"
        if output_format == "csv"
        else "SELECT count(*) FROM read_parquet(?)"
    )
    return int(connection.execute(count_query, [destination.staged_path.as_posix()]).fetchone()[0])


def _require_row_count_within_budget(
    row_count: int,
    budget: ResourceBudget | None,
) -> None:
    if budget is not None and budget.row_limit is not None and row_count > budget.row_limit:
        raise ResourceLimitExceeded(
            f"Extract output exceeded resourceBudget.rowLimit={budget.row_limit}."
        )


def _publish_job_output(
    destination: _ExtractDestination,
    output_format: str,
    row_count: int,
    cancellation: ExportCancellation | None,
) -> dict[str, Any]:
    _raise_if_cancelled(cancellation)
    destination.staged_path.replace(destination.final_path)
    _raise_if_cancelled(cancellation)
    return {
        "outputFormat": output_format,
        "filePath": destination.final_path.as_posix(),
        "filePaths": [destination.final_path.as_posix()],
        "rowCount": row_count,
    }


def _result_columns(
    destination: _ExtractDestination,
    connection: Any,
    compiled: CompiledPipeline | None,
) -> list[dict]:
    columns = parquet_columns(destination.staged_path, connection=connection)
    if compiled is None:
        return columns
    for column, output_column in zip(columns, compiled.output_schema):
        column["originalName"] = column.get("originalName") or column.get("name")
        column["name"] = output_column.label
    return columns


def _publish_dataset_output(
    validated: ValidatedExtractRequest,
    request: dict,
    data_root: str | Path,
    job_id: str,
    destination: _ExtractDestination,
    row_count: int,
    columns: list[dict],
) -> _DatasetPublication:
    target = validated.result_target
    if target is None:  # pragma: no cover - 호출 경계의 타입 불변식
        raise RuntimeError("dataset publication requires a result target")
    manifest = publish_dataset_file_artifact(
        destination.staged_path,
        data_root,
        target.user_id,
        target.user_dataset_id,
        target.user_dataset_file_id,
        {
            "manifestVersion": 1,
            "artifactKind": "EXTRACT_RESULT",
            "requestId": request.get("requestId", ""),
            "jobId": job_id,
            "jobType": EXPORT,
            "extractDatasetId": request.get("datasetId", ""),
            "runId": request.get("runId", ""),
            "connectionId": validated.connection_id,
            "idempotencyKey": target.idempotency_key,
            "rowCount": row_count,
            "columns": columns,
            "createdAt": utc_now(),
        },
    )
    # 새 게시가 이겼으면 publish 함수가 artifact 디렉터리를 최종 위치로 옮긴다.
    published_new = not destination.staged_path.parent.parent.exists()
    final_manifest = dataset_file_manifest_path(
        data_root,
        target.user_id,
        target.user_dataset_id,
        target.user_dataset_file_id,
    )
    published_columns = manifest.get("columns") or columns
    manifest_row_count = manifest.get("rowCount")
    published_row_count = int(
        manifest_row_count if manifest_row_count is not None else row_count
    )
    result = {
        "outputFormat": "parquet",
        "filePath": destination.final_path.as_posix(),
        "filePaths": [destination.final_path.as_posix()],
        "manifestPath": final_manifest.as_posix(),
        "rowCount": published_row_count,
        "resultColumns": published_columns,
        "artifact": {
            "kind": "USER_DATASET_FILE",
            "userId": target.user_id,
            "userDatasetId": target.user_dataset_id,
            "userDatasetFileId": target.user_dataset_file_id,
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
    return _DatasetPublication(result=result, published_new=published_new)


def execute_extract(
    connection_id: str,
    request: dict,
    data_root: str | Path,
    job_id: str,
    cancellation: ExportCancellation | None = None,
    budget: ResourceBudget | None = None,
) -> dict:
    """추출 SQL 또는 고급 파이프라인을 파일로 실행하고 결과 메타데이터를 반환한다."""

    validated = ValidatedExtractRequest.from_raw(connection_id, request)
    destination = _ExtractDestination.create(data_root, job_id, validated)
    destination.prepare()
    published_new = False
    try:
        with _extract_connection(data_root, job_id, cancellation) as connection:
            copied = _copy_to_staging(
                connection,
                validated,
                request,
                data_root,
                destination,
                budget,
            )
            _require_staged_output_within_budget(
                destination,
                copied.row_count,
                budget,
            )
            _raise_if_cancelled(cancellation)
            row_count = _resolved_row_count(
                connection,
                destination,
                validated.output_format,
                copied.row_count,
            )
            _require_row_count_within_budget(row_count, budget)

            if validated.result_target is None:
                return _publish_job_output(
                    destination,
                    validated.output_format,
                    row_count,
                    cancellation,
                )

            columns = _result_columns(
                destination,
                connection,
                copied.compiled_pipeline,
            )
            _raise_if_cancelled(cancellation)
            publication = _publish_dataset_output(
                validated,
                request,
                data_root,
                job_id,
                destination,
                row_count,
                columns,
            )
            published_new = publication.published_new
            _raise_if_cancelled(cancellation)
            return publication.result
    except Exception as exc:
        cancelled = cancellation is not None and cancellation.requested.is_set()
        destination.cleanup_failed(published_new=published_new, cancelled=cancelled)
        if cancelled and not isinstance(exc, JobCancelled):
            raise JobCancelled("Running DuckDB extract was interrupted.") from exc
        raise
    finally:
        destination.cleanup_temporary()


def callback_payload(job: dict) -> dict:
    """저장된 작업 상태를 Boot 호환 추출 콜백 본문으로 변환한다."""

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
    """종단 상태를 먼저 저장한 뒤 콜백 전달 결과도 별도로 기록한다."""

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
    """ACCEPTED 추출을 선점해 취소와 콜백을 포함한 종단 상태까지 진행한다."""

    validated = ValidatedExtractRequest.from_raw(connection_id, request)
    with cancellable_export(job_id) as cancellation:
        job = update_job(
            data_root,
            job_id,
            lambda current: current
            if current.get("state") in TERMINAL_STATES
            else {**current, **_job_fields(validated, request, job_id, "RUNNING")},
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
                failure_fields = job_failure_fields(exc, include_error=True)
                terminal = update_job(
                    data_root,
                    job_id,
                    lambda current: {
                        **current,
                        "state": "FAILED",
                        **failure_fields,
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
    """호환 호출자를 위해 추출을 동기 실행하고 파일 기반 작업 상태를 남긴다."""

    validated = ValidatedExtractRequest.from_raw(connection_id, request)
    job_id = job_id or str(uuid.uuid4())
    try:
        job = load_job(data_root, job_id)
    except FileNotFoundError:
        job = {}
    job.update(
        {
            **_job_fields(validated, request, job_id, "RUNNING"),
            "outputFormat": validated.output_format,
        }
    )
    job = save_job(data_root, job)
    try:
        result = execute_extract(connection_id, request, data_root, job_id)
    except Exception as exc:
        job.update(
            {
                "state": "FAILED",
                **job_failure_fields(exc, include_error=True),
            }
        )
        save_job(data_root, job)
        raise

    public_result = {key: value for key, value in result.items() if not key.startswith("_")}
    job.update({"state": "COMPLETED", **public_result})
    return save_job(data_root, job)
