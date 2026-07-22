"""환경 파일과 데이터 저장소 경로 설정을 읽는다."""

from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: str | Path = ".env") -> None:
    """기존 환경 변수를 보존하면서 단순 KEY=VALUE 환경 파일을 읽는다."""

    env_path = Path(path)
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"'")
        os.environ.setdefault(key, value)


def data_root(value: str | None = None) -> Path:
    """명시값 또는 환경 변수에서 정규화된 데이터 루트 경로를 반환한다."""

    return Path(value or os.environ.get("DATA_ROOT") or "/Users/root1/cdw").expanduser().resolve()
