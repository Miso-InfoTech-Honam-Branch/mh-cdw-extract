"""원격 데이터베이스 테이블을 불변 Parquet 스냅샷으로 갱신한다."""

from __future__ import annotations

import logging
import re
import shutil
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

from .callback import callback_options as normalized_callback_options, post_json_callback
from .clickhouse import write_clickhouse_table_parquet
from .contracts import ResourceBudget
from .duck import connect, quote_ident, source_attach_sql, source_table_sql
from .errors import ResourceLimitExceeded
from .jobs import save_job
from .manifest import save_connection_manifest, utc_now
from .parquet_metadata import parquet_file_metadata
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
    """오류가 워커 경계를 넘기 전에 자격 증명과 비밀값을 제거한다."""
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
    """예외를 외부 저장이 가능한 비밀 제거 오류 필드로 변환한다."""

    message = sanitize_refresh_text(exc)
    return {
        "errorCode": sanitize_refresh_text(type(exc).__name__),
        "error": message,
        "message": message,
    }


def callback_error_fields(exc: Exception) -> dict:
    """콜백 재시도 소진 정보를 저장할 오류 필드로 변환한다."""

    error = refresh_error_fields(exc)
    return {
        "errorCode": error["errorCode"],
        "message": error["message"],
        "attempts": CALLBACK_MAX_ATTEMPTS,
        "occurredAt": utc_now(),
    }


def save_callback_error(data_root: str | Path, job: dict, exc: Exception) -> dict:
    """종단 작업 결과를 유지하면서 콜백 전달 실패를 별도 기록한다."""

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
    """갱신 결과에서 완료 작업에 저장할 공개 필드를 만든다."""

    return {
        "state": "COMPLETED",
        "connectionId": result["connectionId"],
        "tableCount": result["tableCount"],
        "rowCount": result["rowCount"],
        "tables": result["tables"],
        "message": result["message"],
    }


def callback_options(request: dict) -> dict:
    """현재 및 레거시 필드에서 테이블 갱신 콜백 설정을 정규화한다."""

    return normalized_callback_options(request, legacy_url_key="callbackUrl")


def post_refresh_callback(request: dict, payload: dict) -> dict | None:
    """지수형 대기와 제한된 재시도로 테이블 갱신 콜백을 전송한다."""

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
    """성공한 테이블 갱신 결과를 Boot 콜백 본문으로 변환한다."""

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
    """실패한 테이블 갱신을 비밀 제거된 Boot 콜백 본문으로 변환한다."""

    return {
        "jobId": job_id,
        "jobType": TABLE_REFRESH,
        "connectionId": connection_id,
        "state": "FAILED",
        "status": "FAILED",
        **refresh_error_fields(exc),
    }


@dataclass(frozen=True, slots=True)
class _RefreshSnapshotPlan:
    """한 번의 갱신에 사용할 스테이징 및 불변 게시 경로를 검증하고 관리한다."""

    root: Path
    temporary_root: Path
    staging_root: Path
    staging_tables: Path
    final_tables: Path
    snapshot_id: str

    @classmethod
    def create(
        cls,
        connection_id: str,
        data_root: str | Path,
        job_id: str,
    ) -> "_RefreshSnapshotPlan":
        root = connection_root(data_root, connection_id)
        snapshot_id = f"{safe_path_segment(job_id, 'jobId')}-{uuid.uuid4().hex[:12]}"
        root.mkdir(parents=True, exist_ok=True)

        temporary_root_path = root / "_tmp"
        temporary_root_path.mkdir(parents=True, exist_ok=True)
        temporary_root = temporary_root_path.resolve()
        if not temporary_root.is_relative_to(root):
            raise ValueError(
                "metadata refresh temporary path must remain beneath its connection root"
            )

        staging_root = temporary_root / snapshot_id
        staging_root.mkdir()
        staging_root = staging_root.resolve()
        if not staging_root.is_relative_to(temporary_root):
            raise ValueError(
                "metadata refresh staging path must remain beneath its temporary root"
            )

        staging_tables = staging_root / "tables"
        staging_tables.mkdir()
        staging_tables = staging_tables.resolve()
        if not staging_tables.is_relative_to(staging_root):
            shutil.rmtree(staging_root, ignore_errors=True)
            raise ValueError(
                "metadata refresh paths must remain beneath their staging root"
            )
        return cls(
            root=root,
            temporary_root=temporary_root,
            staging_root=staging_root,
            staging_tables=staging_tables,
            final_tables=root / "tables",
            snapshot_id=snapshot_id,
        )

    def output_path(self, table: dict) -> tuple[str, Path]:
        file_name = table_file_name(table)
        output = (self.staging_tables / file_name).resolve()
        if not output.is_relative_to(self.staging_tables):
            raise ValueError(
                "metadata refresh output must remain beneath its staging directory"
            )
        return file_name, output

    def published_path(self, file_name: str) -> str:
        return (Path("tables") / self.snapshot_id / file_name).as_posix()

    def result_path(self, file_name: str) -> str:
        """DATA_ROOT 기준으로 Boot에 전달할 전체 상대 저장소 키를 만든다."""

        return (
            Path("connections")
            / self.root.name
            / self.published_path(file_name)
        ).as_posix()

    def manifest_path(self, result_path: str) -> str:
        """전체 상대 저장소 키를 connection manifest 기준 경로로 변환한다."""

        prefix = (Path("connections") / self.root.name).as_posix() + "/"
        if not result_path.startswith(prefix):
            raise ValueError("metadata refresh result path is outside its connection")
        return result_path[len(prefix) :]

    def publish(self) -> None:
        final_tables = self._validated_final_tables()
        final_snapshot = (final_tables / self.snapshot_id).resolve()
        if not final_snapshot.is_relative_to(final_tables):
            raise ValueError(
                "metadata refresh snapshot must remain beneath its tables directory"
            )
        if final_snapshot.exists():
            raise FileExistsError(
                "metadata refresh snapshot already exists for this jobId"
            )
        self.staging_tables.replace(final_snapshot)
        self.cleanup()

    def cleanup(self) -> None:
        shutil.rmtree(self.staging_root, ignore_errors=True)

    def _validated_final_tables(self) -> Path:
        self.final_tables.mkdir(parents=True, exist_ok=True)
        final_tables = self.final_tables.resolve()
        if not final_tables.is_relative_to(self.root):
            raise ValueError(
                "metadata refresh tables path must remain beneath its connection root"
            )
        return final_tables


@dataclass(slots=True)
class _RefreshBudgetUsage:
    """갱신 누적 사용량을 추적하고 공개 자원 예산을 검사한다."""

    budget: ResourceBudget | None
    vendor: str
    total_rows: int = 0
    staged_bytes: int = 0

    def remaining_output_bytes(self) -> int | None:
        if self.budget is None or self.budget.output_bytes is None:
            return None
        return max(0, self.budget.output_bytes - self.staged_bytes)

    def record(self, row_count: int, output_size: int) -> None:
        self.total_rows += row_count
        self.staged_bytes += output_size
        if self.budget is None:
            return
        if self.budget.row_limit is not None and self.total_rows > self.budget.row_limit:
            raise ResourceLimitExceeded(
                f"Metadata refresh exceeded resourceBudget.rowLimit={self.budget.row_limit}."
            )
        if (
            self.budget.output_bytes is not None
            and self.staged_bytes > self.budget.output_bytes
        ):
            raise ResourceLimitExceeded(
                "Metadata refresh exceeded "
                f"resourceBudget.outputBytes={self.budget.output_bytes}."
            )
        if self.vendor != "clickhouse" and self.staged_bytes > self.budget.temp_bytes:
            raise ResourceLimitExceeded(
                "Metadata refresh staging exceeded "
                f"resourceBudget.tempBytes={self.budget.temp_bytes}."
            )


@dataclass(slots=True)
class _RefreshExportRun:
    """요청한 원천 테이블 전체를 아직 게시하지 않은 스냅샷으로 내보낸다."""

    source: dict
    tables: list[dict]
    compression: str
    data_root: str | Path
    job_id: str
    plan: _RefreshSnapshotPlan
    budget: ResourceBudget | None
    artifacts: list[dict] = field(default_factory=list)
    usage: _RefreshBudgetUsage = field(init=False)
    vendor: str = field(init=False)

    def __post_init__(self) -> None:
        self.vendor = (self.source.get("vendor") or "postgresql").lower()
        self.usage = _RefreshBudgetUsage(self.budget, self.vendor)

    def execute(self) -> list[dict]:
        connection = self._source_connection()
        metadata_connection = connection or connect(
            self.data_root,
            "refresh-parquet-metadata",
            self.job_id,
            budget=self.budget,
        )
        try:
            self._attach_source(connection)
            for table in self.tables:
                self._export_table(connection, metadata_connection, table)
        finally:
            if metadata_connection is not connection:
                metadata_connection.close()
            if connection is not None:
                connection.close()
        return self.artifacts

    def _source_connection(self) -> Any | None:
        if self.vendor == "clickhouse":
            return None
        return connect(self.data_root, "refresh", self.job_id)

    def _attach_source(self, connection: Any | None) -> None:
        if connection is None:
            return
        extension, attach_sql = source_attach_sql(self.source)
        connection.execute(f"INSTALL {extension}")
        connection.execute(f"LOAD {extension}")
        connection.execute(attach_sql)

    def _export_table(
        self,
        connection: Any | None,
        metadata_connection: Any,
        table: dict,
    ) -> None:
        file_name, output = self.plan.output_path(table)
        row_count = self._write_table(connection, table, output)
        metadata = parquet_file_metadata(output, metadata_connection)
        self.usage.record(row_count, metadata["sizeBytes"])
        self.artifacts.append(
            self._artifact(table, file_name, row_count, metadata)
        )

    def _write_table(self, connection: Any | None, table: dict, output: Path) -> int:
        if self.vendor == "clickhouse":
            return self._write_clickhouse_table(table, output)
        if connection is None:
            raise RuntimeError("duckdb connection is not initialized")
        return self._write_duckdb_table(connection, table, output)

    def _write_clickhouse_table(self, table: dict, output: Path) -> int:
        maximum_bytes = self.usage.remaining_output_bytes()
        if maximum_bytes is None:
            return write_clickhouse_table_parquet(self.source, table, output)
        return write_clickhouse_table_parquet(
            self.source,
            table,
            output,
            maximum_bytes=maximum_bytes,
        )

    def _write_duckdb_table(self, connection: Any, table: dict, output: Path) -> int:
        select_list = "*"
        if table.get("columns"):
            select_list = ", ".join(
                quote_ident(str(column["name"])) for column in table["columns"]
            )
        source_sql = f"SELECT {select_list} FROM {source_table_sql(self.source, table)}"
        connection.execute(
            f"COPY ({source_sql}) TO ? "
            f"(FORMAT PARQUET, COMPRESSION {self.compression.upper()})",
            [output.as_posix()],
        )
        return connection.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [output.as_posix()],
        ).fetchone()[0]

    def _artifact(
        self,
        table: dict,
        file_name: str,
        row_count: int,
        metadata: dict,
    ) -> dict:
        return {
            "tableId": table.get("tableId"),
            "schemaName": table.get("schemaName") or self.source.get("schemaName"),
            "tableName": table.get("tableName"),
            "path": self.plan.result_path(file_name),
            "rowCount": row_count,
            "columns": table.get("columns") or [],
            "sizeBytes": metadata["sizeBytes"],
            "sha256Checksum": metadata["sha256Checksum"],
            "schemaHash": metadata["schemaHash"],
        }


def _refresh_request_parts(
    connection_id: str,
    request: dict,
) -> tuple[dict, list[dict], str]:
    """기존 요청 기본값과 오류 문구를 유지하며 갱신 입력을 정규화한다."""

    source = request.get("sourceConnection") or {}
    tables = request.get("tables") or []
    if not connection_id:
        raise ValueError("connection_id is required")
    if not tables:
        raise ValueError("tables is required")
    compression = ((request.get("options") or {}).get("compression") or "snappy").lower()
    return source, tables, compression


def _save_refresh_manifest(
    connection_id: str,
    data_root: str | Path,
    plan: _RefreshSnapshotPlan,
    artifacts: list[dict],
) -> None:
    """게시가 끝난 불변 스냅샷을 현재 연결 매니페스트로 전환한다."""

    save_connection_manifest(
        connection_id,
        data_root,
        {
            "connectionId": connection_id,
            "status": "COMPLETED",
            "snapshotId": plan.snapshot_id,
            "tables": [
                {
                    **artifact,
                    "path": plan.manifest_path(artifact["path"]),
                }
                for artifact in artifacts
            ],
            "updatedAt": utc_now(),
        },
    )


def refresh_tables_impl(
    connection_id: str,
    request: dict,
    data_root: str | Path,
    job_id: str,
    budget: ResourceBudget | None = None,
) -> dict:
    """요청 테이블을 새 Parquet 세대로 적재하고 매니페스트를 전환한다."""

    source, tables, compression = _refresh_request_parts(connection_id, request)
    plan = _RefreshSnapshotPlan.create(connection_id, data_root, job_id)
    export = _RefreshExportRun(
        source=source,
        tables=tables,
        compression=compression,
        data_root=data_root,
        job_id=job_id,
        plan=plan,
        budget=budget,
    )
    try:
        artifacts = export.execute()
        plan.publish()
    except Exception:
        plan.cleanup()
        raise

    # 새 세대는 불변 스냅샷으로 게시한다. 이미 시작한 추출은 해석을 끝낸
    # 이전 경로를 계속 읽고, 새 추출만 원자 교체된 매니페스트를 본다.
    # 따라서 사용 중인 tables 디렉터리를 제자리에서 삭제하지 않는다.
    _save_refresh_manifest(connection_id, data_root, plan, artifacts)
    return {
        "jobId": job_id,
        "jobType": TABLE_REFRESH,
        "connectionId": connection_id,
        "state": "COMPLETED",
        "message": "Table Parquet refresh completed successfully.",
        "tableCount": len(artifacts),
        "rowCount": export.usage.total_rows,
        "tables": artifacts,
    }


def refresh_tables(connection_id: str, request: dict, data_root: str | Path, job_id: str | None = None) -> dict:
    """테이블 갱신을 동기 실행하고 파일 작업 상태와 콜백을 기록한다."""

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
    """테이블 갱신 요청을 검증해 ACCEPTED 비동기 작업을 준비한다."""

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
    """수락된 테이블 갱신을 실행해 성공 또는 실패 콜백까지 완료한다."""

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
