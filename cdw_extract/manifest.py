"""연결별 Parquet 스냅샷 매니페스트를 원자적으로 관리한다."""

from __future__ import annotations

import json
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .paths import connection_root


_MANIFEST_REPLACE_ATTEMPTS = 8
_MANIFEST_REPLACE_INITIAL_BACKOFF_SECONDS = 0.005


def utc_now() -> str:
    """UTC 현재 시각을 ISO 8601 문자열로 반환한다."""

    return datetime.now(timezone.utc).isoformat()


def load_connection_manifest(connection_id: str, data_root: str | Path) -> dict:
    """연결 매니페스트를 읽고 객체 형태와 연결 식별자를 검증한다."""

    path = connection_root(data_root, connection_id) / "manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"connection manifest not found: {path}")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise ValueError("connection manifest must be a JSON object")
    manifest_connection_id = manifest.get("connectionId")
    if manifest_connection_id is not None and str(manifest_connection_id) != str(connection_id):
        raise ValueError("connection manifest identity does not match connectionId")
    return manifest


def save_connection_manifest(connection_id: str, data_root: str | Path, manifest: dict) -> dict:
    """연결 매니페스트를 임시 파일을 거쳐 원자적으로 게시한다."""

    if not isinstance(manifest, dict):
        raise TypeError("connection manifest must be a mapping")
    manifest_connection_id = manifest.get("connectionId")
    if manifest_connection_id is not None and str(manifest_connection_id) != str(connection_id):
        raise ValueError("connection manifest identity does not match connectionId")
    root = connection_root(data_root, connection_id)
    root.mkdir(parents=True, exist_ok=True)
    path = root / "manifest.json"
    tmp = root / f".manifest.{uuid.uuid4().hex}.tmp"
    try:
        tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        for attempt in range(_MANIFEST_REPLACE_ATTEMPTS):
            try:
                tmp.replace(path)
                break
            except PermissionError:
                # Windows cannot replace a destination while a concurrent
                # reader or writer briefly holds it open. The source temp file
                # is generation-unique, so a bounded retry preserves the same
                # atomic last-completer-wins manifest publication semantics.
                if attempt + 1 >= _MANIFEST_REPLACE_ATTEMPTS:
                    raise
                time.sleep(
                    min(
                        _MANIFEST_REPLACE_INITIAL_BACKOFF_SECONDS * (2 ** attempt),
                        0.1,
                    )
                )
    finally:
        tmp.unlink(missing_ok=True)
    return manifest


def delete_connection(connection_id: str, data_root: str | Path) -> dict:
    """연결의 로컬 스냅샷과 매니페스트 디렉터리를 삭제한다."""

    root = connection_root(data_root, connection_id)
    shutil.rmtree(root, ignore_errors=True)
    return {"connectionId": connection_id, "state": "DELETED"}
