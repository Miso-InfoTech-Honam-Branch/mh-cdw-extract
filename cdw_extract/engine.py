"""큐 구현과 무관한 작업 실행 진입점을 제공한다.

FastAPI, 콜백 전달, 파일 작업 저장소를 직접 가져오지 않는다. 호스트가 처리기를 주입하고
이 결정적 실행 경계 밖의 모든 영속 상태 전이를 책임진다.
"""

from __future__ import annotations

import tempfile
import threading
from contextlib import contextmanager
from dataclasses import dataclass
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
from .spi import JobEventSink


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


@dataclass(slots=True)
class _EventPublisher:
    """한 실행 시도에서 단조 증가하는 상태 이벤트 순서를 소유한다."""

    envelope: JobEnvelope
    sink: JobEventSink
    attempt: int
    sequence: int

    def emit(self, status: JobStatus, details: Mapping[str, Any] | None = None) -> None:
        self.sequence += 1
        self.sink.emit(
            self.envelope,
            status,
            {
                "eventSequence": self.sequence,
                "attempt": self.attempt,
                **dict(details or {}),
            },
        )

    def emit_result(self, result: JobResult) -> None:
        if result.status == JobStatus.SUCCESS:
            self.emit(
                result.status,
                {
                    "metrics": result.metrics,
                    "artifacts": [
                        artifact.transport_dict() for artifact in result.artifacts
                    ],
                },
            )
            return
        if result.status == JobStatus.FAILED:
            # JobResult가 FAILED이면 Pydantic 계약상 error가 반드시 존재한다.
            error = result.error
            if error is None:  # pragma: no cover - 계약 훼손을 조기에 드러내는 방어선
                raise RuntimeError("FAILED JobResult is missing its error.")
            self.emit(
                result.status,
                {
                    "error": error.transport_dict(),
                    "errorCode": error.code,
                    "retryable": error.retryable,
                },
            )
            return
        self.emit(result.status, {"message": "Job handler returned a cancelled result."})


class _DeadlineGuard:
    """deadline 타이머의 시작·취소와 만료 여부를 한 수명 경계에서 관리한다."""

    def __init__(self, budget: ResourceBudget, context: ExecutionContext) -> None:
        self._deadline = budget.deadline
        self._cancellation = context.cancellation
        self._timer: threading.Timer | None = None
        self.reached = threading.Event()

    def __enter__(self) -> "_DeadlineGuard":
        if self._deadline is None:
            return self
        remaining = (self._deadline - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            raise DeadlineExceeded("Job resource deadline has already elapsed.")

        def expire() -> None:
            # 타이머 스레드는 취소 토큰만 전환한다. 실제 DuckDB/스트림
            # 중단은 토큰에 등록된 인터럽트 훅이 같은 경계에서 수행한다.
            self.reached.set()
            self._cancellation.cancel()

        self._timer = threading.Timer(remaining, expire)
        self._timer.daemon = True
        self._timer.start()
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        if self._timer is not None:
            self._timer.cancel()


class _ExecutionRun:
    """작업 한 건의 상관관계, 자원 경계와 종단 결과 변환을 조정한다."""

    def __init__(
        self,
        envelope: JobEnvelope,
        context: ExecutionContext,
        services: RuntimeServices,
    ) -> None:
        self.envelope = envelope
        self.context = context
        self.services = services
        self.budget = context.budget or envelope.resource_budget
        self.workspace: Path | None = None
        self.events = _EventPublisher(
            envelope=envelope,
            sink=context.events or services.events,
            attempt=context.attempt,
            sequence=context.event_sequence_start,
        )
        self.deadline = _DeadlineGuard(self.budget, context)

    def execute(self) -> JobResult:
        try:
            with self.services.cancellations.register(
                self.envelope.job_id,
                self.context.cancellation,
            ):
                self._validate_correlation()
                self.context.cancellation.raise_if_cancelled()
                with self.deadline:
                    return self._invoke_handler()
        except JobCancelled as exc:
            return self._cancelled_or_deadline_result(exc)
        except Exception as exc:
            return self._exception_result(exc)

    def _validate_correlation(self) -> None:
        if (
            self.context.job_id is not None
            and self.context.job_id != self.envelope.job_id
        ):
            raise ValueError("ExecutionContext job_id does not match JobEnvelope jobId.")

    def _invoke_handler(self) -> JobResult:
        handler = self.services.handler_for(self.envelope.job_type)
        if handler is None:
            raise NoJobHandlerError(
                f"No handler is registered for jobType={self.envelope.job_type.value}."
            )

        self.events.emit(JobStatus.RUNNING)
        # 작업 공간과 실행 자원을 컨텍스트로 묶어 하위 어댑터가 호스트나
        # 큐 구현을 직접 알지 않고도 동일한 예산과 취소 신호를 사용하게 한다.
        with _job_workspace(
            self.context,
            self.services,
            str(self.envelope.job_id),
        ) as workspace:
            self.workspace = workspace
            resources = ExecutionResources(
                budget=self.budget,
                temp_root=workspace,
                cancellation=self.context.cancellation,
            )
            with execution_resource_scope(resources):
                output = handler(self.envelope, self.context, self.services)
                self.context.cancellation.raise_if_cancelled()

        result = _success_result(self.envelope, output)
        if result.status == JobStatus.SUCCESS:
            _require_result_within_budget(result, self.budget)
        self.events.emit_result(result)
        return result

    def _cancelled_or_deadline_result(self, exc: JobCancelled) -> JobResult:
        if self.deadline.reached.is_set():
            return self._failed_result(
                DeadlineExceeded("Job execution exceeded resourceBudget.deadline.")
            )
        return self._cancelled_result(_safe_error_message(exc, self.workspace))

    def _exception_result(self, exc: Exception) -> JobResult:
        if (
            self.context.cancellation.is_cancelled
            and not self.deadline.reached.is_set()
        ):
            return self._cancelled_result("Job execution was cancelled.")
        if self.deadline.reached.is_set():
            exc = DeadlineExceeded("Job execution exceeded resourceBudget.deadline.")
        return self._failed_result(exc)

    def _cancelled_result(self, message: str) -> JobResult:
        result = JobResult(
            jobId=self.envelope.job_id,
            jobType=self.envelope.job_type,
            status=JobStatus.CANCELLED,
            artifacts=(),
            metrics={},
        )
        self.events.emit(result.status, {"message": message})
        return result

    def _failed_result(self, exc: Exception) -> JobResult:
        error = JobError(
            code=str(getattr(exc, "code", type(exc).__name__)),
            message=_safe_error_message(exc, self.workspace),
            retryable=bool(getattr(exc, "retryable", False)),
        )
        result = JobResult(
            jobId=self.envelope.job_id,
            jobType=self.envelope.job_type,
            status=JobStatus.FAILED,
            artifacts=(),
            metrics={},
            error=error,
        )
        self.events.emit_result(result)
        return result


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
            item
            if isinstance(item, ArtifactDescriptor)
            else ArtifactDescriptor.model_validate(item)
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


class CdwEngine:
    """호스트가 주입한 처리기를 자원·취소 경계 안에서 실행하는 진입점이다."""

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
        return _ExecutionRun(normalized, execution_context, self.services).execute()

    @staticmethod
    def _success_result(envelope: JobEnvelope, output: Any) -> JobResult:
        return _success_result(envelope, output)

    @staticmethod
    def _require_result_within_budget(result: JobResult, budget: ResourceBudget) -> None:
        _require_result_within_budget(result, budget)
