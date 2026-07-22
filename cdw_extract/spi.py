"""Host supplied interfaces used by the queue-neutral execution core."""

from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, BinaryIO, Mapping, Protocol, TYPE_CHECKING

from .contracts import ArtifactDescriptor, JobEnvelope, JobStatus

if TYPE_CHECKING:
    from .runtime import ExecutionContext, RuntimeServices


class MaterializedArtifact(AbstractContextManager[Path], Protocol):
    def __enter__(self) -> Path: ...
    def __exit__(self, exc_type, exc, traceback) -> None: ...


class ArtifactStore(Protocol):
    """Logical artifact storage; keys are store-relative and never local paths."""

    def materialize(self, descriptor: ArtifactDescriptor, workspace: Path) -> MaterializedArtifact: ...
    def open(self, descriptor: ArtifactDescriptor) -> BinaryIO: ...
    def describe(self, key: str) -> ArtifactDescriptor: ...
    def publish(
        self,
        source: Path,
        key: str,
        *,
        idempotency_key: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> ArtifactDescriptor: ...


class SecretLease(AbstractContextManager[Mapping[str, str]], Protocol):
    def __enter__(self) -> Mapping[str, str]: ...
    def __exit__(self, exc_type, exc, traceback) -> None: ...


class SecretProvider(Protocol):
    def resolve(self, reference: str, *, purpose: str) -> SecretLease: ...


class JobEventSink(Protocol):
    def emit(
        self,
        envelope: JobEnvelope,
        status: JobStatus,
        details: Mapping[str, Any] | None = None,
    ) -> None: ...


class JobHandler(Protocol):
    def __call__(
        self,
        envelope: JobEnvelope,
        context: "ExecutionContext",
        services: "RuntimeServices",
    ) -> Any: ...
