"""데이터 루트 아래의 안전한 저장 경로와 파일명을 생성한다."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path


_SAFE_FILE_STUB = re.compile(r"[^A-Za-z0-9_-]+")
_WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


def safe_path_segment(value: object, field_name: str) -> str:
    """호출자가 제공한 파일시스템 식별자 한 조각을 안전하게 검증한다."""

    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    if text in {".", ".."} or any(
        ord(character) < 32 or character in '<>:"/\\|?*\x00'
        for character in text
    ):
        raise ValueError(f"{field_name} contains characters that are not safe in a file path")
    if text.endswith((".", " ")):
        raise ValueError(f"{field_name} must not end with a dot or space")
    if text.split(".", 1)[0].upper() in _WINDOWS_RESERVED_NAMES:
        raise ValueError(f"{field_name} is a reserved file name")
    return text


def connection_root(data_root: str | Path, connection_id: str) -> Path:
    """검증된 연결 식별자에 대응하는 DATA_ROOT 하위 경로를 반환한다."""

    root = Path(data_root).expanduser().resolve()
    safe_connection_id = safe_path_segment(connection_id, "connectionId")
    # Materialize the trusted collection directory before resolving children.
    # pathlib on Windows can otherwise observe inconsistent results when one
    # thread resolves a missing ancestor while another creates it.
    connections_path = root / "connections"
    connections_path.mkdir(parents=True, exist_ok=True)
    connections = connections_path.resolve()
    if not connections.is_relative_to(root):
        raise ValueError("connectionId resolves outside DATA_ROOT")
    candidate_path = connections / safe_connection_id
    candidate = candidate_path.resolve() if candidate_path.exists() else candidate_path
    if not candidate.is_relative_to(connections):
        raise ValueError("connectionId resolves outside DATA_ROOT")
    return candidate


def tables_dir(data_root: str | Path, connection_id: str) -> Path:
    """연결의 테이블 스냅샷 디렉터리를 반환한다."""

    return connection_root(data_root, connection_id) / "tables"


def extracts_dir(data_root: str | Path, connection_id: str) -> Path:
    """연결의 추출 결과 디렉터리를 반환한다."""

    return connection_root(data_root, connection_id) / "extracts"


def table_file_name(table: dict) -> str:
    """DB 식별자를 경로에 노출하지 않는 안정적인 Parquet 파일명을 만든다."""

    table_id = str(table.get("tableId") or "").strip()
    schema = str(table.get("schemaName") or "").strip()
    name = str(table.get("tableName") or "").strip()
    if not table_id and not name:
        raise ValueError("tableId or tableName is required")

    # Database identifiers may legally contain path separators and platform
    # metacharacters. Keep those identifiers in manifest metadata, but never
    # use them as a filesystem path. A short readable stub plus the complete
    # identity hash is stable and collision resistant.
    raw_stub = table_id or name or "table"
    stub = _SAFE_FILE_STUB.sub("-", raw_stub).strip("-_")[:48] or "table"
    identity = json.dumps(
        {"tableId": table_id, "schemaName": schema, "tableName": name},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"{stub}-{digest}.parquet"
