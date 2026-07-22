"""Stable, queue-neutral contracts for embedding the extraction engine.

The models in this module are transport safe.  In particular, they contain no
callback URLs, process-local state, or absolute filesystem paths.  Queue and
HTTP hosts may add their own delivery metadata outside :class:`JobEnvelope`.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


_JAVA_LONG_MAX = (1 << 63) - 1
_JAVA_INTEGER_MAX = (1 << 31) - 1
_MAX_CALLBACK_METRICS_BYTES = 2 * 1024 * 1024


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, frozen=True)

    def transport_dict(self) -> dict[str, Any]:
        """Return the canonical JSON-ready representation used on the wire."""

        return self.model_dump(by_alias=True, mode="json", exclude_none=True)

    def transport_json(self) -> str:
        """Serialize with contract aliases instead of Python field names."""

        return self.model_dump_json(by_alias=True, exclude_none=True)


class JobType(str, Enum):
    EXTRACT = "EXTRACT"
    METADATA_REFRESH = "METADATA_REFRESH"
    DATASET_CONVERT = "DATASET_CONVERT"
    ANALYSIS_ARTIFACT = "ANALYSIS_ARTIFACT"


class JobStatus(str, Enum):
    DISPATCH_PENDING = "DISPATCH_PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class JobErrorCategory(str, Enum):
    VALIDATION = "VALIDATION"
    DATA = "DATA"
    DEPENDENCY = "DEPENDENCY"
    RESOURCE = "RESOURCE"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    INTERNAL = "INTERNAL"


class ArtifactStoreType(str, Enum):
    LOCAL = "LOCAL"
    S3 = "S3"
    MINIO = "MINIO"
    SFTP = "SFTP"
    DATABASE = "DATABASE"


class ArtifactFormatType(str, Enum):
    PARQUET = "PARQUET"
    CSV = "CSV"
    XLSX = "XLSX"
    PNG = "PNG"
    PDF = "PDF"
    JSON = "JSON"
    OTHER = "OTHER"


class ResourceBudget(ContractModel):
    """Per-job resource ceiling supplied by the dispatching host.

    DuckDB settings accept byte quantities, so retaining bytes in the public
    contract avoids unit ambiguity between Boot, queue workers, and Python.
    """

    cpu_threads: Annotated[int, Field(ge=1, le=64)] = Field(default=2, alias="cpuThreads")
    memory_bytes: Annotated[int, Field(ge=16 * 1024 * 1024, le=_JAVA_LONG_MAX)] = Field(
        default=256 * 1024 * 1024,
        alias="memoryBytes",
    )
    temp_bytes: Annotated[int, Field(ge=16 * 1024 * 1024, le=_JAVA_LONG_MAX)] = Field(
        default=2 * 1024 * 1024 * 1024,
        alias="tempBytes",
    )
    input_bytes: Annotated[int, Field(ge=0, le=_JAVA_LONG_MAX)] | None = Field(
        default=None,
        alias="inputBytes",
    )
    output_bytes: Annotated[int, Field(ge=0, le=_JAVA_LONG_MAX)] | None = Field(
        default=None,
        alias="outputBytes",
    )
    row_limit: Annotated[int, Field(ge=1, le=_JAVA_LONG_MAX)] | None = Field(
        default=None,
        alias="rowLimit",
    )
    deadline: datetime | None = None

    @field_validator("deadline")
    @classmethod
    def require_timezone(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("resourceBudget.deadline must include a timezone")
        return value.astimezone(timezone.utc)


class ArtifactDescriptor(ContractModel):
    store: ArtifactStoreType
    key: Annotated[str, Field(min_length=1, max_length=1000)]
    version: Annotated[str, Field(min_length=1, max_length=255)] | None = None
    sha256: Annotated[str, Field(pattern=r"^[0-9a-fA-F]{64}$")]
    size_bytes: Annotated[int, Field(ge=0, le=_JAVA_LONG_MAX)] = Field(alias="sizeBytes")
    row_count: Annotated[int, Field(ge=0, le=_JAVA_LONG_MAX)] | None = Field(
        default=None,
        alias="rowCount",
    )
    content_type: Annotated[str, Field(min_length=1, max_length=120)] = Field(alias="contentType")
    format: ArtifactFormatType
    schema_hash: Annotated[str, Field(pattern=r"^[0-9a-fA-F]{64}$")] | None = Field(
        default=None,
        alias="schemaHash",
    )

    @field_validator("store", mode="before")
    @classmethod
    def normalize_store(cls, value: str | ArtifactStoreType) -> str:
        return str(value.value if isinstance(value, ArtifactStoreType) else value).strip().upper()

    @field_validator("sha256")
    @classmethod
    def normalize_sha256(cls, value: str) -> str:
        return value.lower()

    @field_validator("format", mode="before")
    @classmethod
    def normalize_format(cls, value: str | ArtifactFormatType) -> str:
        return str(value.value if isinstance(value, ArtifactFormatType) else value).strip().upper()

    @field_validator("schema_hash")
    @classmethod
    def normalize_schema_hash(cls, value: str | None) -> str | None:
        return value.lower() if value is not None else None

    @field_validator("key")
    @classmethod
    def reject_absolute_local_path(cls, value: str) -> str:
        normalized = value.replace("\\", "/")
        if normalized.startswith("/") or (len(normalized) >= 3 and normalized[1:3] == ":/"):
            raise ValueError("artifact key must be store-relative, not an absolute local path")
        if any(part == ".." for part in normalized.split("/")):
            raise ValueError("artifact key must not traverse parent directories")
        return normalized


class JobError(ContractModel):
    code: Annotated[str, Field(min_length=1, max_length=100)]
    message: Annotated[str, Field(min_length=1, max_length=2000)]
    retryable: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


class JobEnvelope(ContractModel):
    schema_version: Literal[2] = Field(default=2, alias="schemaVersion")
    job_id: UUID = Field(alias="jobId")
    job_type: JobType = Field(alias="jobType")
    idempotency_key: Annotated[str, Field(min_length=1, max_length=255)] = Field(alias="idempotencyKey")
    command: dict[str, Any]
    resource_budget: ResourceBudget = Field(default_factory=ResourceBudget, alias="resourceBudget")


class CancellationEnvelope(ContractModel):
    """Queue-neutral, idempotent cancellation command.

    Cancellation delivery is intentionally separate from :class:`JobEnvelope`:
    it targets an existing execution and therefore has neither ``jobType`` nor
    a new resource budget.
    """

    schema_version: Literal[2] = Field(default=2, alias="schemaVersion")
    job_id: UUID = Field(alias="jobId")
    idempotency_key: Annotated[str, Field(min_length=1, max_length=255)] = Field(
        alias="idempotencyKey"
    )
    reason: Annotated[str, Field(min_length=1, max_length=2000)] | None = None


class JobResult(ContractModel):
    schema_version: Literal[2] = Field(default=2, alias="schemaVersion")
    job_id: UUID = Field(alias="jobId")
    job_type: JobType = Field(alias="jobType")
    status: JobStatus
    artifacts: tuple[ArtifactDescriptor, ...] = ()
    metrics: dict[str, Any] = Field(default_factory=dict)
    error: JobError | None = None

    @field_validator("status")
    @classmethod
    def require_terminal_status(cls, value: JobStatus) -> JobStatus:
        if value not in {JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.CANCELLED}:
            raise ValueError("JobResult status must be SUCCESS, FAILED, or CANCELLED")
        return value

    @model_validator(mode="after")
    def validate_error_state(self) -> "JobResult":
        if self.status == JobStatus.FAILED and self.error is None:
            raise ValueError("FAILED JobResult requires error")
        if self.status != JobStatus.FAILED and self.error is not None:
            raise ValueError("JobResult error is valid only when status is FAILED")
        if self.status != JobStatus.SUCCESS and self.artifacts:
            raise ValueError("JobResult artifacts are valid only when status is SUCCESS")
        return self


class JobCallbackError(ContractModel):
    code: Annotated[str, Field(min_length=1, max_length=100)]
    category: JobErrorCategory
    retryable: bool = False
    message: Annotated[str, Field(min_length=1, max_length=2000)]
    diagnostic_ref: Annotated[str, Field(min_length=1, max_length=255)] | None = Field(
        default=None,
        alias="diagnosticRef",
    )


class JobCallbackEvent(ContractModel):
    """Boot callback body that a queue host can build from an engine result.

    The queue host owns the durable sequence counter.  ``eventId`` is derived
    from ``jobId`` and ``sequence`` by :meth:`from_result`, which makes a
    redelivery of the same event idempotent without process-local state.
    """

    schema_version: Literal[2] = Field(default=2, alias="schemaVersion")
    event_id: UUID = Field(alias="eventId")
    sequence: Annotated[int, Field(ge=1, le=_JAVA_LONG_MAX)]
    job_id: UUID = Field(alias="jobId")
    queue_job_id: Annotated[str, Field(min_length=1, max_length=100)] | None = Field(
        default=None,
        alias="queueJobId",
    )
    event_type: Annotated[str, Field(min_length=1, max_length=40)] = Field(
        default="JOB_STATUS",
        alias="eventType",
    )
    status: JobStatus
    stage: Annotated[str, Field(min_length=1, max_length=64)] | None = None
    progress: Annotated[float, Field(ge=0, le=100)] | None = None
    attempt: Annotated[int, Field(ge=1, le=_JAVA_INTEGER_MAX)]
    processed_rows: Annotated[int, Field(ge=0, le=_JAVA_LONG_MAX)] | None = Field(
        default=None,
        alias="processedRows",
    )
    processed_bytes: Annotated[int, Field(ge=0, le=_JAVA_LONG_MAX)] | None = Field(
        default=None,
        alias="processedBytes",
    )
    artifacts: tuple[ArtifactDescriptor, ...] = ()
    error: JobCallbackError | None = None
    metrics: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_terminal_body(self) -> "JobCallbackEvent":
        if self.artifacts and self.status != JobStatus.SUCCESS:
            raise ValueError("Callback artifacts are valid only when status is SUCCESS")
        if self.status == JobStatus.FAILED and self.error is None:
            raise ValueError("FAILED callback requires error")
        if self.status != JobStatus.FAILED and self.error is not None:
            raise ValueError("Callback error is valid only when status is FAILED")
        if self.metrics is not None:
            try:
                encoded_metrics = json.dumps(
                    self.metrics,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode("utf-8")
            except (TypeError, ValueError) as exc:
                raise ValueError("Callback metrics must contain JSON-compatible values") from exc
            if len(encoded_metrics) > _MAX_CALLBACK_METRICS_BYTES:
                raise ValueError("Callback metrics must not exceed 2 MiB")
        return self

    @classmethod
    def from_result(
        cls,
        result: JobResult,
        *,
        sequence: int,
        attempt: int,
        queue_job_id: str | None = None,
        stage: str | None = None,
        error_category: JobErrorCategory = JobErrorCategory.INTERNAL,
        diagnostic_ref: str | None = None,
    ) -> "JobCallbackEvent":
        metrics = result.metrics
        processed_rows = metrics.get("processedRows", metrics.get("rowCount"))
        processed_bytes = metrics.get("processedBytes", metrics.get("sizeBytes"))
        error = None
        if result.error is not None:
            category = {
                "DEADLINE_EXCEEDED": JobErrorCategory.TIMEOUT,
                "RESOURCE_LIMIT_EXCEEDED": JobErrorCategory.RESOURCE,
                "WORKER_BUSY": JobErrorCategory.RESOURCE,
                "NO_JOB_HANDLER": JobErrorCategory.VALIDATION,
                "PIPELINE_COMPILER_VERSION_MISMATCH": JobErrorCategory.VALIDATION,
                "PIPELINE_SNAPSHOT_MISMATCH": JobErrorCategory.VALIDATION,
                "PIPELINE_SOURCE_SCHEMA_CHANGED": JobErrorCategory.VALIDATION,
            }.get(result.error.code, error_category)
            error = JobCallbackError(
                code=result.error.code,
                category=category,
                retryable=result.error.retryable,
                message=result.error.message,
                diagnosticRef=diagnostic_ref,
            )
        progress = 100.0 if result.status == JobStatus.SUCCESS else None
        return cls(
            eventId=uuid5(NAMESPACE_URL, f"cdw-job-callback:v2:{result.job_id}:{sequence}"),
            sequence=sequence,
            jobId=result.job_id,
            queueJobId=queue_job_id,
            status=result.status,
            stage=stage,
            progress=progress,
            attempt=attempt,
            processedRows=processed_rows,
            processedBytes=processed_bytes,
            artifacts=result.artifacts,
            error=error,
            metrics=result.metrics or None,
        )
