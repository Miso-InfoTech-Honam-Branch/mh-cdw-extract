"""운영 모듈의 한국어 문서화 기준이 퇴행하지 않는지 검사한다."""

from __future__ import annotations

import ast
from pathlib import Path


PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "cdw_extract"
EXPECTED_MODULES = {
    "__init__.py",
    "adapters/__init__.py",
    "adapters/legacy.py",
    "adapters/local.py",
    "analytics.py",
    "analytics_artifacts.py",
    "analytics_compiler.py",
    "analytics_models.py",
    "callback.py",
    "clickhouse.py",
    "config.py",
    "contracts.py",
    "duck.py",
    "engine.py",
    "errors.py",
    "execution_scope.py",
    "extract.py",
    "jobs.py",
    "manifest.py",
    "paths.py",
    "preview.py",
    "query.py",
    "refresh.py",
    "runtime.py",
    "spi.py",
    "transforms/__init__.py",
    "transforms/compiler.py",
    "transforms/runtime.py",
    "transforms/schema.py",
    "user_dataset.py",
    "user_dataset_jobs.py",
}


def _contains_hangul(value: str | None) -> bool:
    return any("가" <= character <= "힣" for character in value or "")


def _operational_modules() -> dict[str, Path]:
    return {
        path.relative_to(PACKAGE_ROOT).as_posix(): path
        for path in PACKAGE_ROOT.rglob("*.py")
    }


def test_all_operational_modules_have_korean_module_docstrings() -> None:
    modules = _operational_modules()
    assert set(modules) == EXPECTED_MODULES

    missing: list[str] = []
    for name, path in sorted(modules.items()):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if not _contains_hangul(ast.get_docstring(tree, clean=False)):
            missing.append(name)

    assert not missing, f"한국어 모듈 docstring 누락: {missing}"


def test_public_module_apis_have_korean_docstrings() -> None:
    missing: list[str] = []
    for module_name, path in sorted(_operational_modules().items()):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if not isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name.startswith("_"):
                continue
            if not _contains_hangul(ast.get_docstring(node, clean=False)):
                missing.append(f"{module_name}:{node.name}")

    assert not missing, f"한국어 공개 API docstring 누락: {missing}"
