"""Queue-agnostic execution facade.

This module deliberately does not import FastAPI, callback delivery, or the
filesystem job repository.  A host injects job handlers and owns all durable
state transitions around this deterministic execution boundary.
"""

from __future__ import annotations

import tempfile
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from .contracts import (
    ArtifactDescriptor,
    JobEnvelope,
    JobError,
    JobResult,
    JobStatus,
    ResourceBudget,
)
from .errors import DeadlineExceeded, JobCancelled, NoJobHandlerError, ResourceLimitExceeded
from .execution_scope import ExecutionResources, execution_resource_scope
from .runtime import ExecutionContext, RuntimeServices


def _safe_error_message(exc: Exception, workspace: Path | None) -> str:
    message = str(exc).strip() or type(exc).__name__
    if workspace is not None:
        message = message.replace(str(workspace), "<job-workspace>")
        message = message.replace(workspace.as_posix(), "<job-workspace>")
    return message[:2000]


@contextmanager
def _job_workspace(context: ExecutionContext, services: RuntimeServices, job_id: str) -> Iterator[Path]:
    if context.workspace is not None:
        root = Path(context.workspace).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        yield root
        return

    base = None
    if services.workspace_root is not None:
        base = Path(services.workspace_root).expanduser().resolve()
        base.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f"cdw-{job_id[:12]}-", dir=base) as temporary:
        yield Path(temporary).resolve()


class CdwEngine:
    def __init__(self, services: RuntimeServices | None = None) -> None:
        self.services = services or RuntimeServices()

    def validate(self, envelope: JobEnvelope | Mapping[str, Any]) -> JobEnvelope:
        if isinstance(envelope, JobEnvelope):
            return envelope
        return JobEnvelope.model_validate(envelope)

    def execute(
        self,
        envelope: JobEnvelope | Mapping[str, Any],
        context: ExecutionContext | None = None,
    ) -> JobResult:
        normalized = self.validate(envelope)
        execution_context = context or ExecutionContext()
        budget = execution_context.budget or normalized.resource_budget
        sink = execution_context.events or self.services.events
        workspace: Path | None = None
        timer: threading.Timer | None = None
        cancellation_registration = None
        deadline_reached = threading.Event()
        event_sequence = execution_context.event_sequence_start

        def emit(status: JobStatus, details: Mapping[str, Any] | None = None) -> None:
            nonlocal event_sequence
            event_sequence += 1
            sink.emit(
                normalized,
                status,
                {
                    "eventSequence": event_sequence,
                    "attempt": execution_context.attempt,
                    **dict(details or {}),
                },
            )

        try:
            cancellation_registration = self.services.cancellations.register(
                normalized.job_id,
                execution_context.cancellation,
            )
            if (
                execution_context.job_id is not None
                and execution_context.job_id != normalized.job_id
            ):
                raise ValueError("ExecutionContext job_id does not match JobEnvelope jobId.")
            execution_context.cancellation.raise_if_cancelled()
            if budget.deadline is not None:
                remaining = (budget.deadline - datetime.now(timezone.utc)).total_seconds()
                if remaining <= 0:
                    raise DeadlineExceeded("Job resource deadline has already elapsed.")

                def expire() -> None:
                    deadline_reached.set()
                    execution_context.cancellation.cancel()

                timer = threading.Timer(remaining, expire)
                timer.daemon = True
                timer.start()

            handler = self.services.handler_for(normalized.job_type)
            if handler is None:
                raise NoJobHandlerError(f"No handler is registered for jobType={normalized.job_type.value}.")

            emit(JobStatus.RUNNING)
            with _job_workspace(execution_context, self.services, str(normalized.job_id)) as workspace:
                resources = ExecutionResources(
                    budget=budget,
                    temp_root=workspace,
                    cancellation=execution_context.cancellation,
                )
                with execution_resource_scope(resources):
                    output = handler(normalized, execution_context, self.services)
                    execution_context.cancellation.raise_if_cancelled()

            result = self._success_result(normalized, output)
            if result.status == JobStatus.SUCCESS:
                self._require_result_within_budget(result, budget)
                emit(
                    result.status,
                    {
                        "metrics": result.metrics,
                        "artifacts": [artifact.transport_dict() for artifact in result.artifacts],
                    },
                )
            elif result.status == JobStatus.FAILED:
                emit(
                    result.status,
                    {
                        "error": result.error.transport_dict(),
                        "errorCode": result.error.code,
                        "retryable": result.error.retryable,
                    },
                )
            else:
                emit(result.status, {"message": "Job handler returned a cancelled result."})
            return result
        except JobCancelled as exc:
            if deadline_reached.is_set():
                error = JobError(
                    code=DeadlineExceeded.code,
                    message="Job execution exceeded resourceBudget.deadline.",
                    retryable=DeadlineExceeded.retryable,
                )
                result = JobResult(
                    jobId=normalized.job_id,
                    jobType=normalized.job_type,
                    status=JobStatus.FAILED,
                    artifacts=(),
                    metrics={},
                    error=error,
                )
                emit(
                    result.status,
                    {
                        "error": error.transport_dict(),
                        "errorCode": error.code,
                        "retryable": error.retryable,
                    },
                )
                return result
            result = JobResult(
                jobId=normalized.job_id,
                jobType=normalized.job_type,
                status=JobStatus.CANCELLED,
                artifacts=(),
                metrics={},
            )
            emit(result.status, {"message": _safe_error_message(exc, workspace)})
            return result
        except Exception as exc:
            if execution_context.cancellation.is_cancelled and not deadline_reached.is_set():
                result = JobResult(
                    jobId=normalized.job_id,
                    jobType=normalized.job_type,
                    status=JobStatus.CANCELLED,
                    artifacts=(),
                    metrics={},
                )
                emit(result.status, {"message": "Job execution was cancelled."})
                return result
            if deadline_reached.is_set():
                exc = DeadlineExceeded("Job execution exceeded resourceBudget.deadline.")
            code = getattr(exc, "code", type(exc).__name__)
            retryable = bool(getattr(exc, "retryable", False))
            error = JobError(
                code=str(code),
                message=_safe_error_message(exc, workspace),
                retryable=retryable,
            )
            result = JobResult(
                jobId=normalized.job_id,
                jobType=normalized.job_type,
                status=JobStatus.FAILED,
                artifacts=(),
                metrics={},
                error=error,
            )
            emit(
                result.status,
                {
                    "error": error.transport_dict(),
                    "errorCode": error.code,
                    "retryable": error.retryable,
                },
            )
            return result
        finally:
            if timer is not None:
                timer.cancel()
            if cancellation_registration is not None:
                cancellation_registration.close()

    @staticmethod
    def _success_result(envelope: JobEnvelope, output: Any) -> JobResult:
        if isinstance(output, JobResult):
            if output.job_id != envelope.job_id or output.job_type != envelope.job_type:
                raise ValueError("Job handler returned a result with mismatched job correlation.")
            return output

        if output is None:
            artifacts: tuple[ArtifactDescriptor, ...] = ()
            metrics: dict[str, Any] = {}
        elif isinstance(output, Mapping):
            raw_artifacts = output.get("artifacts") or ()
            artifacts = tuple(
                item if isinstance(item, ArtifactDescriptor) else ArtifactDescriptor.model_validate(item)
                for item in raw_artifacts
            )
            raw_metrics = output.get("metrics")
            if raw_metrics is None:
                raw_metrics = {
                    key: value
                    for key, value in output.items()
                    if key not in {"artifacts", "error", "status"}
                }
            if not isinstance(raw_metrics, Mapping):
                raise TypeError("Job handler metrics must be a mapping.")
            metrics = dict(raw_metrics)
        else:
            raise TypeError("Job handler must return JobResult, a mapping, or None.")

        return JobResult(
            jobId=envelope.job_id,
            jobType=envelope.job_type,
            status=JobStatus.SUCCESS,
            artifacts=artifacts,
            metrics=metrics,
        )

    @staticmethod
    def _require_result_within_budget(result: JobResult, budget: ResourceBudget) -> None:
        artifact_bytes = sum(artifact.size_bytes for artifact in result.artifacts)
        metric_bytes = result.metrics.get("outputBytes", result.metrics.get("sizeBytes"))
        observed_bytes = artifact_bytes
        if not result.artifacts and isinstance(metric_bytes, int) and not isinstance(metric_bytes, bool):
            observed_bytes = metric_bytes
        if budget.output_bytes is not None and observed_bytes > budget.output_bytes:
            raise ResourceLimitExceeded(
                f"Job output used {observed_bytes} bytes, exceeding resourceBudget.outputBytes="
                f"{budget.output_bytes}."
            )

        artifact_rows = sum(
            artifact.row_count for artifact in result.artifacts if artifact.row_count is not None
        )
        metric_rows = result.metrics.get("processedRows", result.metrics.get("rowCount"))
        observed_rows = artifact_rows
        if not any(artifact.row_count is not None for artifact in result.artifacts):
            if isinstance(metric_rows, int) and not isinstance(metric_rows, bool):
                observed_rows = metric_rows
        if budget.row_limit is not None and observed_rows > budget.row_limit:
            raise ResourceLimitExceeded(
                f"Job output produced {observed_rows} rows, exceeding resourceBudget.rowLimit="
                f"{budget.row_limit}."
            )
