"""Typed failures shared by core execution hosts."""

from __future__ import annotations


class CdwExecutionError(RuntimeError):
    code = "EXECUTION_ERROR"
    retryable = False


class JobCancelled(CdwExecutionError):
    code = "JOB_CANCELLED"


class ResourceLimitExceeded(CdwExecutionError):
    code = "RESOURCE_LIMIT_EXCEEDED"


class DeadlineExceeded(ResourceLimitExceeded):
    code = "DEADLINE_EXCEEDED"


class NoJobHandlerError(CdwExecutionError):
    code = "NO_JOB_HANDLER"


class WorkerBusy(TimeoutError):
    code = "WORKER_BUSY"
    retryable = True


class PipelineCompilerVersionMismatch(ValueError):
    code = "PIPELINE_COMPILER_VERSION_MISMATCH"
    retryable = False


class PipelineSnapshotMismatch(ValueError):
    code = "PIPELINE_SNAPSHOT_MISMATCH"
    retryable = False


class PipelineSourceSchemaChanged(ValueError):
    code = "PIPELINE_SOURCE_SCHEMA_CHANGED"
    retryable = False
