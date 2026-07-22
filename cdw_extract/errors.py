"""실행 코어와 여러 호스트가 공유하는 분류 가능한 오류를 정의한다."""

from __future__ import annotations


class CdwExecutionError(RuntimeError):
    """실행 코어에서 발생하는 분류 가능한 오류의 기본형이다."""

    code = "EXECUTION_ERROR"
    retryable = False


class JobCancelled(CdwExecutionError):
    """호스트 또는 사용자가 작업 취소를 요청했음을 나타낸다."""

    code = "JOB_CANCELLED"


class ResourceLimitExceeded(CdwExecutionError):
    """작업이 할당된 자원 예산을 초과했음을 나타낸다."""

    code = "RESOURCE_LIMIT_EXCEEDED"


class DeadlineExceeded(ResourceLimitExceeded):
    """작업 실행 기한이 만료되었음을 나타낸다."""

    code = "DEADLINE_EXCEEDED"


class NoJobHandlerError(CdwExecutionError):
    """요청한 작업 유형의 실행 핸들러가 등록되지 않았음을 나타낸다."""

    code = "NO_JOB_HANDLER"


class WorkerBusy(TimeoutError):
    """가용 실행 슬롯이나 자원을 제한 시간 안에 얻지 못했음을 나타낸다."""

    code = "WORKER_BUSY"
    retryable = True


class PipelineCompilerVersionMismatch(ValueError):
    """요청 스냅샷과 현재 파이프라인 컴파일러 버전이 다름을 나타낸다."""

    code = "PIPELINE_COMPILER_VERSION_MISMATCH"
    retryable = False


class PipelineSnapshotMismatch(ValueError):
    """저장된 파이프라인 스냅샷 해시가 요청 내용과 다름을 나타낸다."""

    code = "PIPELINE_SNAPSHOT_MISMATCH"
    retryable = False


class PipelineSourceSchemaChanged(ValueError):
    """컴파일 이후 원본 스키마가 변경되었음을 나타낸다."""

    code = "PIPELINE_SOURCE_SCHEMA_CHANGED"
    retryable = False
