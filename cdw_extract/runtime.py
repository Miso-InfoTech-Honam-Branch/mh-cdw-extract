"""큐 중립 작업의 실행 컨텍스트와 호스트 제공 서비스를 정의한다."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping
from uuid import UUID

from .contracts import CancellationEnvelope, JobType, ResourceBudget
from .errors import JobCancelled
from .spi import ArtifactStore, JobEventSink, JobHandler, SecretProvider


class _CancellationRegistration:
    def __init__(self, token: "CancellationToken", registration_id: int) -> None:
        self._token = token
        self._registration_id = registration_id
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._token._unregister(self._registration_id)

    def __enter__(self) -> "_CancellationRegistration":
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()


class _JobCancellationRegistration:
    def __init__(self, registry: "CancellationRegistry", job_id: UUID, token: "CancellationToken") -> None:
        self._registry = registry
        self._job_id = job_id
        self._token = token
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._registry._unregister(self._job_id, self._token)

    def __enter__(self) -> "_JobCancellationRegistration":
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()


class CancellationToken:
    """DuckDB 인터럽트 훅을 지원하는 스레드 안전 협력형 취소 토큰이다."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._callbacks: dict[int, Callable[[], None]] = {}
        self._next_registration_id = 1

    @property
    def is_cancelled(self) -> bool:
        return self._event.is_set()

    def cancel(self) -> bool:
        with self._lock:
            if self._event.is_set():
                return False
            self._event.set()
            callbacks = tuple(self._callbacks.values())
        for callback in callbacks:
            try:
                callback()
            except Exception:
                # Cancellation must remain monotonic even if an adapter has
                # already disposed the underlying resource.
                pass
        return True

    def raise_if_cancelled(self) -> None:
        if self.is_cancelled:
            raise JobCancelled("Job execution was cancelled.")

    def register_interrupt(self, callback: Callable[[], None]) -> _CancellationRegistration:
        with self._lock:
            registration_id = self._next_registration_id
            self._next_registration_id += 1
            if not self._event.is_set():
                self._callbacks[registration_id] = callback
                return _CancellationRegistration(self, registration_id)
        callback()
        return _CancellationRegistration(self, registration_id)

    def _unregister(self, registration_id: int) -> None:
        with self._lock:
            self._callbacks.pop(registration_id, None)


class CancellationRegistry:
    """큐 취소 명령을 실행 중 작업에 연결하는 프로세스 로컬 레지스트리이다.

    실행 전에 도착한 취소도 tombstone으로 남아 이후 등록된 토큰을 즉시 취소한다. 영속
    취소 상태는 큐 호스트가 소유하며 종단 결과 확인 뒤 :meth:`forget`을 호출해야 한다.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active: dict[UUID, CancellationToken] = {}
        self._tombstones: set[UUID] = set()

    def register(
        self,
        job_id: UUID | str,
        token: CancellationToken,
    ) -> _JobCancellationRegistration:
        normalized = job_id if isinstance(job_id, UUID) else UUID(str(job_id))
        with self._lock:
            existing = self._active.get(normalized)
            if existing is not None and existing is not token:
                raise RuntimeError(f"Job is already executing in this process: {normalized}")
            self._active[normalized] = token
            cancelled_before_start = normalized in self._tombstones
        if cancelled_before_start:
            token.cancel()
        return _JobCancellationRegistration(self, normalized, token)

    def cancel(self, command: CancellationEnvelope | UUID | str) -> bool:
        normalized = command.job_id if isinstance(command, CancellationEnvelope) else (
            command if isinstance(command, UUID) else UUID(str(command))
        )
        with self._lock:
            was_new = normalized not in self._tombstones
            self._tombstones.add(normalized)
            token = self._active.get(normalized)
        interrupted = token.cancel() if token is not None else False
        return was_new or interrupted

    def forget(self, job_id: UUID | str) -> None:
        normalized = job_id if isinstance(job_id, UUID) else UUID(str(job_id))
        with self._lock:
            self._tombstones.discard(normalized)

    def _unregister(self, job_id: UUID, token: CancellationToken) -> None:
        with self._lock:
            if self._active.get(job_id) is token:
                self._active.pop(job_id, None)


class NullEventSink:
    """상태 이벤트가 필요 없는 임베딩 환경용 무동작 이벤트 수신기이다."""

    def emit(self, _envelope, _status, _details=None) -> None:
        return None


@dataclass(slots=True)
class ExecutionContext:
    """한 작업 시도의 취소, 예산, 작업 공간과 이벤트 순서를 전달한다."""

    job_id: UUID | str | None = None
    attempt: int = 1
    event_sequence_start: int = 0
    cancellation: CancellationToken = field(default_factory=CancellationToken)
    budget: ResourceBudget | None = None
    workspace: Path | None = None
    events: JobEventSink | None = None

    def __post_init__(self) -> None:
        if self.job_id is not None and not isinstance(self.job_id, UUID):
            self.job_id = UUID(str(self.job_id))
        if self.attempt < 1:
            raise ValueError("ExecutionContext.attempt must be at least 1")
        if self.event_sequence_start < 0:
            raise ValueError("ExecutionContext.event_sequence_start must not be negative")


@dataclass(slots=True)
class RuntimeServices:
    """HTTP 또는 큐 호스트가 소유하고 주입하는 장기 수명 의존성이다."""

    handlers: Mapping[JobType | str, JobHandler] = field(default_factory=dict)
    artifact_store: ArtifactStore | None = None
    secret_provider: SecretProvider | None = None
    workspace_root: Path | None = None
    events: JobEventSink = field(default_factory=NullEventSink)
    cancellations: CancellationRegistry = field(default_factory=CancellationRegistry)
    extensions: Mapping[str, Any] = field(default_factory=dict)

    def handler_for(self, job_type: JobType) -> JobHandler | None:
        return self.handlers.get(job_type) or self.handlers.get(job_type.value)
