"""Parquet 파일의 물리 스키마와 무결성 메타데이터를 계산한다."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


_HASH_CHUNK_SIZE = 8 * 1024 * 1024


def file_sha256(path: str | Path) -> str:
    """파일 전체를 일정한 크기의 청크로 읽어 SHA-256 체크섬을 계산한다."""

    digest = hashlib.sha256()
    with Path(path).open("rb") as file:
        for chunk in iter(lambda: file.read(_HASH_CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parquet_schema_columns(path: str | Path, connection: Any) -> list[dict]:
    """실제 Parquet 파일을 조회해 순서가 보존된 물리 컬럼 스키마를 반환한다."""

    parquet_path = Path(path)
    rows = connection.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)",
        [parquet_path.as_posix()],
    ).fetchall()
    return [
        {
            "originalName": str(row[0]),
            "name": str(row[0]),
            "type": _canonical_type(row[1]),
            "ordinal": index,
            "nullable": _nullable(row),
        }
        for index, row in enumerate(rows, start=1)
    ]


def parquet_schema_hash(columns: Iterable[Mapping[str, object]]) -> str:
    """컬럼 순서·이름·타입·NULL 허용 여부를 정규화해 SHA-256을 계산한다."""

    canonical_columns = [
        {
            "name": str(column.get("originalName") or column.get("name") or ""),
            "nullable": bool(column.get("nullable", True)),
            "ordinal": index,
            "type": _canonical_type(column.get("type")),
        }
        for index, column in enumerate(columns, start=1)
    ]
    canonical = json.dumps(
        canonical_columns,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parquet_file_metadata(path: str | Path, connection: Any) -> dict:
    """실제 Parquet 파일의 크기, 체크섬, 물리 스키마 해시를 함께 반환한다."""

    parquet_path = Path(path)
    columns = parquet_schema_columns(parquet_path, connection)
    return {
        "sizeBytes": parquet_path.stat().st_size,
        "sha256Checksum": file_sha256(parquet_path),
        "schemaHash": parquet_schema_hash(columns),
        "columns": columns,
    }


def _canonical_type(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def _nullable(row: tuple) -> bool:
    return len(row) < 3 or str(row[2] or "YES").strip().upper() != "NO"
