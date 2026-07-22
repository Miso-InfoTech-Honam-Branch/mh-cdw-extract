"""Local filesystem ArtifactStore used by the compatibility HTTP adapter."""

from __future__ import annotations

import hashlib
import mimetypes
import os
import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, BinaryIO, Iterator, Mapping

from ..contracts import ArtifactDescriptor
from ..paths import safe_path_segment


_KNOWN_FORMATS = {"PARQUET", "CSV", "XLSX", "PNG", "PDF", "JSON"}


def _stable_file_digest(path: Path) -> tuple[str, int, int]:
    """Hash one stable file generation and return digest, size, mtime ns."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        before = os.fstat(stream.fileno())
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
        after = os.fstat(stream.fileno())
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or before.st_ino != after.st_ino
    ):
        raise RuntimeError("artifact changed while its checksum was being calculated")
    return digest.hexdigest(), int(after.st_size), int(after.st_mtime_ns)


def _format_for_key(key: str) -> str:
    suffix = Path(key).suffix.lstrip(".").upper()
    return suffix if suffix in _KNOWN_FORMATS else "OTHER"


class LocalArtifactStore:
    """Store-relative local artifacts with atomic, idempotent publication."""

    def __init__(self, root: str | Path, store_name: str = "LOCAL") -> None:
        self.root = Path(root).expanduser().resolve()
        self.store_name = store_name.strip().upper()

    def _path(self, key: str) -> Path:
        descriptor = ArtifactDescriptor(
            store=self.store_name,
            key=key,
            sha256="0" * 64,
            sizeBytes=0,
            contentType="application/octet-stream",
            format="OTHER",
        )
        parts = descriptor.key.split("/")
        for index, part in enumerate(parts):
            safe_path_segment(part, f"artifact key segment {index + 1}")
        # Resolve the parent independently and append the final file name.
        # This keeps symlink/junction containment while avoiding Windows
        # Path.resolve races when another publisher creates the final file.
        parent = (self.root / Path(*parts[:-1])).resolve() if len(parts) > 1 else self.root
        if not parent.is_relative_to(self.root):
            raise ValueError("artifact key resolves outside the local store")
        path = parent / parts[-1]
        if path.exists():
            resolved = path.resolve()
            if not resolved.is_relative_to(self.root):
                raise ValueError("artifact key resolves outside the local store")
            return resolved
        return path

    @contextmanager
    def materialize(self, descriptor: ArtifactDescriptor, workspace: Path) -> Iterator[Path]:
        if descriptor.store != self.store_name:
            raise ValueError(f"unsupported artifact store: {descriptor.store}")
        source_path = self._path(descriptor.key)
        if not source_path.is_file():
            raise FileNotFoundError(f"artifact not found: {descriptor.key}")

        materialization_root = Path(workspace).expanduser().resolve()
        materialization_root.mkdir(parents=True, exist_ok=True)
        suffix = Path(descriptor.key).suffix
        target = (
            materialization_root
            / f"artifact-{descriptor.sha256[:16]}-{uuid.uuid4().hex}{suffix}"
        ).resolve()
        if not target.is_relative_to(materialization_root):
            raise ValueError("materialized artifact path resolves outside its job workspace")
        temporary = target.with_name(f".{target.name}.tmp")
        digest = hashlib.sha256()
        copied_bytes = 0
        try:
            with source_path.open("rb") as source, temporary.open("xb") as destination:
                before = os.fstat(source.fileno())
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    destination.write(chunk)
                    digest.update(chunk)
                    copied_bytes += len(chunk)
                destination.flush()
                os.fsync(destination.fileno())
                after = os.fstat(source.fileno())
            if (
                before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
                or before.st_ino != after.st_ino
            ):
                raise ValueError("artifact changed while it was being materialized")
            if digest.hexdigest() != descriptor.sha256 or copied_bytes != descriptor.size_bytes:
                raise ValueError("materialized artifact does not match its declared checksum or size")
            if descriptor.version is not None and str(after.st_mtime_ns) != descriptor.version:
                raise ValueError("materialized artifact does not match its declared version")
            os.replace(temporary, target)
            yield target
        finally:
            temporary.unlink(missing_ok=True)
            target.unlink(missing_ok=True)

    def open(self, descriptor: ArtifactDescriptor) -> BinaryIO:
        if descriptor.store != self.store_name:
            raise ValueError(f"unsupported artifact store: {descriptor.store}")
        path = self._path(descriptor.key)
        if not path.is_file():
            raise FileNotFoundError(f"artifact not found: {descriptor.key}")
        stream = path.open("rb")
        try:
            before = os.fstat(stream.fileno())
            digest = hashlib.sha256()
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
            after = os.fstat(stream.fileno())
            if (
                before.st_size != after.st_size
                or before.st_mtime_ns != after.st_mtime_ns
                or before.st_ino != after.st_ino
            ):
                raise ValueError("artifact changed while it was being opened")
            if digest.hexdigest() != descriptor.sha256 or after.st_size != descriptor.size_bytes:
                raise ValueError("opened artifact does not match its declared checksum or size")
            if descriptor.version is not None and str(after.st_mtime_ns) != descriptor.version:
                raise ValueError("opened artifact does not match its declared version")
            stream.seek(0)
            return stream
        except Exception:
            stream.close()
            raise

    def describe(self, key: str) -> ArtifactDescriptor:
        path = self._path(key)
        if not path.is_file():
            raise FileNotFoundError(f"artifact not found: {key}")
        digest, size_bytes, modified_ns = _stable_file_digest(path)
        content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
        return ArtifactDescriptor(
            store=self.store_name,
            key=key,
            version=str(modified_ns),
            sha256=digest,
            sizeBytes=size_bytes,
            contentType=content_type,
            format=_format_for_key(key),
        )

    def publish(
        self,
        source: Path,
        key: str,
        *,
        idempotency_key: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> ArtifactDescriptor:
        if not idempotency_key.strip():
            raise ValueError("idempotency_key is required")
        source = Path(source).resolve()
        if not source.is_file():
            raise FileNotFoundError(f"artifact source not found: {source}")
        target = self._path(key)
        target.parent.mkdir(parents=True, exist_ok=True)
        # Resolve again after creating parents so a pre-existing link/junction
        # cannot redirect publication outside the configured store.
        target = self._path(key)
        source_hash, _, _ = _stable_file_digest(source)
        if target.exists():
            existing = self.describe(key)
            if existing.sha256 != source_hash:
                raise FileExistsError(
                    "artifact target already exists with different content for the idempotency key"
                )
            return existing.model_copy(
                update={"row_count": (metadata or {}).get("rowCount", existing.row_count)}
            )

        temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
        try:
            shutil.copyfile(source, temporary)
            temporary_hash, _, _ = _stable_file_digest(temporary)
            if temporary_hash != source_hash:
                raise RuntimeError("artifact source changed while it was being published")
            with temporary.open("r+b") as stream:
                stream.flush()
                os.fsync(stream.fileno())
            try:
                # A hard-link publish is an atomic create-if-absent operation
                # on the same filesystem.  Unlike Path.replace(), it can never
                # overwrite a winner that committed between the existence
                # check above and this point.
                os.link(temporary, target)
            except FileExistsError:
                winner = self._path(key)
                target_hash, _, _ = _stable_file_digest(winner)
                if target_hash != source_hash:
                    raise FileExistsError(
                        "artifact target was concurrently published with different content "
                        "for the idempotency key"
                    )
        finally:
            temporary.unlink(missing_ok=True)
        descriptor = self.describe(key)
        return descriptor.model_copy(
            update={"row_count": (metadata or {}).get("rowCount")}
        )
