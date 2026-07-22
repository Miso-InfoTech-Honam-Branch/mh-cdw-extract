"""하위 어댑터가 사용하는 실행별 자원을 컨텍스트 로컬로 전달한다."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Any

from .contracts import ResourceBudget


@dataclass(frozen=True, slots=True)
class ExecutionResources:
    """현재 실행에 할당된 자원 예산, 임시 경로, 취소 토큰이다."""

    budget: ResourceBudget
    temp_root: Path
    cancellation: Any


_resources: ContextVar[ExecutionResources | None] = ContextVar(
    "cdw_extract_execution_resources",
    default=None,
)


def current_execution_resources() -> ExecutionResources | None:
    """현재 컨텍스트에 바인딩된 실행 자원을 반환한다."""

    return _resources.get()


@contextmanager
def execution_resource_scope(resources: ExecutionResources) -> Iterator[None]:
    """하위 호출에서 실행 자원을 조회할 수 있도록 컨텍스트에 임시 바인딩한다."""

    token = _resources.set(resources)
    try:
        yield
    finally:
        _resources.reset(token)
