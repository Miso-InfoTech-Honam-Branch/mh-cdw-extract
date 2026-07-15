from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from .paths import connection_root


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_connection_manifest(connection_id: str, data_root: str | Path) -> dict:
    path = connection_root(data_root, connection_id) / "manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"connection manifest not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def save_connection_manifest(connection_id: str, data_root: str | Path, manifest: dict) -> dict:
    root = connection_root(data_root, connection_id)
    root.mkdir(parents=True, exist_ok=True)
    path = root / "manifest.json"
    tmp = root / "manifest.json.tmp"
    tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
    return manifest


def delete_connection(connection_id: str, data_root: str | Path) -> dict:
    root = connection_root(data_root, connection_id)
    shutil.rmtree(root, ignore_errors=True)
    return {"connectionId": connection_id, "state": "DELETED"}
