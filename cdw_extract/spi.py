"""큐 중립 실행 코어가 호스트로부터 주입받는 인터페이스를 정의한다."""

from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, BinaryIO, Mapping, Protocol, TYPE_CHECKING

from .contracts import ArtifactDescriptor, JobEnvelope, JobStatus

if TYPE_CHECKING:
    from .runtime import ExecutionContext, RuntimeServices


class MaterializedArtifact(AbstractContextManager[Path], Protocol):
    """작업 공간에 임시로 물질화된 산출물 경로의 컨텍스트 계약이다."""

    def __enter__(self) -> Path: ...
    def __exit__(self, exc_type, exc, traceback) -> None: ...


class ArtifactStore(Protocol):
    """저장소 상대 키만 사용하며 로컬 절대 경로를 노출하지 않는 산출물 저장소이다."""

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
    """수명과 폐기를 호스트가 관리하는 비밀값 조회 결과이다."""

    def __enter__(self) -> Mapping[str, str]: ...
    def __exit__(self, exc_type, exc, traceback) -> None: ...


class SecretProvider(Protocol):
    """논리 참조를 작업 목적에 맞는 단기 비밀값으로 해석한다."""

    def resolve(self, reference: str, *, purpose: str) -> SecretLease: ...


class JobEventSink(Protocol):
    """작업 상태 전이를 외부 큐·모니터링 계층에 전달한다."""

    def emit(
        self,
        envelope: JobEnvelope,
        status: JobStatus,
        details: Mapping[str, Any] | None = None,
    ) -> None: ...


class JobHandler(Protocol):
    """큐 중립 실행 엔진에 등록되는 작업 유형별 처리기 계약이다."""

    def __call__(
        self,
        envelope: JobEnvelope,
        context: "ExecutionContext",
        services: "RuntimeServices",
    ) -> Any: ...
