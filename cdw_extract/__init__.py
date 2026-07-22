"""CDW 추출 엔진을 호스트에 내장하기 위한 공개 API를 제공한다.

큐 중립 계약과 엔진은 즉시 노출하고, 파일 작업 상태나 콜백 전송을 불필요하게
로딩하지 않도록 기존 호환 함수는 지연 방식으로 제공한다.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from .contracts import (
    ArtifactDescriptor,
    ArtifactFormatType,
    ArtifactStoreType,
    CancellationEnvelope,
    JobCallbackError,
    JobCallbackEvent,
    JobEnvelope,
    JobError,
    JobErrorCategory,
    JobResult,
    JobStatus,
    JobType,
    ResourceBudget,
)
from .engine import CdwEngine
from .runtime import CancellationRegistry, CancellationToken, ExecutionContext, RuntimeServices
from .spi import ArtifactStore, JobEventSink, JobHandler, SecretProvider


_LEGACY_EXPORTS = {
    "extract": ("cdw_extract.extract", "extract"),
    "prepare_extract_job": ("cdw_extract.extract", "prepare_extract_job"),
    "run_extract_job": ("cdw_extract.extract", "run_extract_job"),
    "cancel_job": ("cdw_extract.jobs", "cancel_job"),
    "job_download_file": ("cdw_extract.jobs", "job_download_file"),
    "load_job": ("cdw_extract.jobs", "load_job"),
    "delete_connection": ("cdw_extract.manifest", "delete_connection"),
    "load_connection_manifest": ("cdw_extract.manifest", "load_connection_manifest"),
    "preview": ("cdw_extract.preview", "preview"),
    "prepare_refresh_tables_job": ("cdw_extract.refresh", "prepare_refresh_tables_job"),
    "refresh_tables": ("cdw_extract.refresh", "refresh_tables"),
    "run_refresh_tables_job": ("cdw_extract.refresh", "run_refresh_tables_job"),
    "prepare_user_dataset_convert_job": (
        "cdw_extract.user_dataset_jobs",
        "prepare_user_dataset_convert_job",
    ),
    "run_user_dataset_convert_job": (
        "cdw_extract.user_dataset_jobs",
        "run_user_dataset_convert_job",
    ),
    "convert_user_dataset_file": (
        "cdw_extract.user_dataset",
        "convert_user_dataset_file",
    ),
    "convert_user_dataset_file_from_path": (
        "cdw_extract.user_dataset",
        "convert_user_dataset_file_from_path",
    ),
    "delete_user_dataset_file": (
        "cdw_extract.user_dataset",
        "delete_user_dataset_file",
    ),
    "load_dataset_file_manifest": (
        "cdw_extract.user_dataset",
        "load_dataset_file_manifest",
    ),
    "post_callback": ("cdw_extract.user_dataset", "post_callback"),
}


def __getattr__(name: str) -> Any:
    target = _LEGACY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute_name = target
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *_LEGACY_EXPORTS})


__all__ = [
    "ArtifactDescriptor",
    "ArtifactFormatType",
    "ArtifactStore",
    "ArtifactStoreType",
    "CancellationEnvelope",
    "CancellationRegistry",
    "CancellationToken",
    "CdwEngine",
    "ExecutionContext",
    "JobEnvelope",
    "JobCallbackError",
    "JobCallbackEvent",
    "JobError",
    "JobErrorCategory",
    "JobEventSink",
    "JobHandler",
    "JobResult",
    "JobStatus",
    "JobType",
    "ResourceBudget",
    "RuntimeServices",
    "SecretProvider",
    *_LEGACY_EXPORTS,
]
