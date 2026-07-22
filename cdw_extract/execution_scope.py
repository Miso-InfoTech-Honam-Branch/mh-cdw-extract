"""Context-local execution resources consumed by lower-level adapters."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Any

from .contracts import ResourceBudget


@dataclass(frozen=True, slots=True)
class ExecutionResources:
    budget: ResourceBudget
    temp_root: Path
    cancellation: Any


_resources: ContextVar[ExecutionResources | None] = ContextVar(
    "cdw_extract_execution_resources",
    default=None,
)


def current_execution_resources() -> ExecutionResources | None:
    return _resources.get()


@contextmanager
def execution_resource_scope(resources: ExecutionResources) -> Iterator[None]:
    token = _resources.set(resources)
    try:
        yield
    finally:
        _resources.reset(token)
