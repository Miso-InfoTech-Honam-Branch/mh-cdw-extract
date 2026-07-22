"""파일 기반 작업 상태와 실행 중 취소 제어를 관리한다."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
import weakref
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, Protocol


TERMINAL_STATES = {"COMPLETED", "FAILED", "SUCCESS", "CANCELED", "CANCELLED"}


class InterruptibleConnection(Protocol):
    """실행 중 질의를 외부 취소 신호로 중단할 수 있는 연결 계약이다."""

    def interrupt(self) -> None: ...


class JobCancelled(RuntimeError):
    """수락된 취소 요청으로 워커 실행을 중단해야 함을 나타낸다."""


@dataclass
class ExportCancellation:
    """한 추출 실행의 취소 이벤트와 현재 DuckDB 연결을 연결한다."""

    job_id: str
    requested: threading.Event = field(default_factory=threading.Event)
    _connection: InterruptibleConnection | None = None
    _lock: threading.Lock = field(default_factory=threading.Lock)
    finished: threading.Event = field(default_factory=threading.Event)

    def attach(self, connection: InterruptibleConnection) -> None:
        with self._lock:
            self._connection = connection
            should_interrupt = self.requested.is_set()
        if should_interrupt:
            try:
                connection.interrupt()
            except Exception:
                # The runner may have detached/closed between registration and interrupt.
                # The event remains authoritative and is checked before terminal commit.
                pass

    def detach(self, connection: InterruptibleConnection) -> None:
        with self._lock:
            if self._connection is connection:
                self._connection = None

    def cancel(self) -> None:
        self.requested.set()
        with self._lock:
            connection = self._connection
        if connection is not None:
            try:
                connection.interrupt()
            except Exception:
                # Closing a DuckDB connection races legitimately with cancellation.
                # The runner still observes requested before committing COMPLETED.
                pass

    def raise_if_requested(self) -> None:
        if self.requested.is_set():
            raise JobCancelled("Job cancellation was requested.")


_job_locks_guard = threading.Lock()
_job_locks: weakref.WeakValueDictionary[str, threading.RLock] = weakref.WeakValueDictionary()
_running_exports_guard = threading.Lock()
_running_exports: dict[str, ExportCancellation] = {}


def _job_lock(job_id: str) -> threading.RLock:
    normalized = normalize_job_id(job_id)
    with _job_locks_guard:
        return _job_locks.setdefault(normalized, threading.RLock())


@contextmanager
def _job_file_lock(data_root: str | Path, job_id: str) -> Iterator[None]:
    """Serialize job read/modify/write across processes on Windows and POSIX."""
    root = job_dir(data_root, job_id)
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".job.lock"
    with lock_path.open("a+b") as lock_file:
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)
        if os.name == "nt":
            import msvcrt

            deadline = time.monotonic() + 60.0
            while True:
                try:
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"timed out locking job manifest: {job_id}")
                    time.sleep(0.01)
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


@contextmanager
def cancellable_export(job_id: str) -> Iterator[ExportCancellation]:
    """작업을 프로세스 내 실행 목록에 등록하고 종료 시 안전하게 해제한다."""

    normalized = normalize_job_id(job_id)
    cancellation = ExportCancellation(normalized)
    with _running_exports_guard:
        if normalized in _running_exports:
            raise RuntimeError(f"extract job is already running: {normalized}")
        _running_exports[normalized] = cancellation
    try:
        yield cancellation
    finally:
        with _running_exports_guard:
            if _running_exports.get(normalized) is cancellation:
                _running_exports.pop(normalized, None)
        cancellation.finished.set()


def _request_running_export_cancel(job_id: str) -> bool:
    normalized = normalize_job_id(job_id)
    with _running_exports_guard:
        cancellation = _running_exports.get(normalized)
    if cancellation is None:
        return False
    cancellation.cancel()
    return True


def _running_export(job_id: str) -> ExportCancellation | None:
    normalized = normalize_job_id(job_id)
    with _running_exports_guard:
        return _running_exports.get(normalized)


def utc_now() -> str:
    """UTC 현재 시각을 ISO 8601 문자열로 반환한다."""

    return datetime.now(timezone.utc).isoformat()


def job_failure_fields(exc: Exception, *, include_error: bool = False) -> dict[str, str]:
    """예외를 파일 작업 저장소의 안정적인 실패 필드로 변환한다."""

    message = str(exc)
    fields = {
        "errorCode": type(exc).__name__,
        "message": message,
    }
    if include_error:
        fields["error"] = message
    return fields


def normalize_job_id(job_id: str) -> str:
    """작업 식별자를 정규 UUID 문자열로 검증·변환한다."""

    try:
        return str(uuid.UUID(str(job_id)))
    except (TypeError, ValueError) as exc:
        raise ValueError("jobId must be a UUID") from exc


def jobs_root(data_root: str | Path) -> Path:
    """파일 기반 작업 저장소의 루트 경로를 반환한다."""

    return Path(data_root) / "jobs"


def job_dir(data_root: str | Path, job_id: str) -> Path:
    """검증된 작업 식별자에 대응하는 저장 디렉터리를 반환한다."""

    return jobs_root(data_root) / normalize_job_id(job_id)


def job_manifest_path(data_root: str | Path, job_id: str) -> Path:
    """작업 상태 JSON 파일의 정규 경로를 반환한다."""

    return job_dir(data_root, job_id) / "job.json"


def _load_job_unlocked(data_root: str | Path, job_id: str) -> dict:
    path = job_manifest_path(data_root, job_id)
    if not path.exists():
        raise FileNotFoundError(f"job not found: {normalize_job_id(job_id)}")
    return json.loads(path.read_text(encoding="utf-8"))


def _merged_state(existing: dict, incoming: dict) -> str | None:
    previous = existing.get("state")
    requested = incoming.get("state")
    if previous in TERMINAL_STATES and requested != previous:
        return previous
    return requested or previous


def _write_job_unlocked(data_root: str | Path, job: dict, existing: dict | None = None) -> dict:
    job_id = normalize_job_id(job["jobId"])
    root = job_dir(data_root, job_id)
    root.mkdir(parents=True, exist_ok=True)
    path = root / "job.json"
    previous = existing if existing is not None else (_load_job_unlocked(data_root, job_id) if path.exists() else {})
    now = utc_now()
    incoming_state = job.get("state")
    conflicting_terminal = (
        previous.get("state") in TERMINAL_STATES
        and incoming_state is not None
        and incoming_state != previous.get("state")
    )
    # 완료·실패·취소 중 먼저 커밋된 종단 상태를 보존한다. 늦게 도착한
    # 워커나 콜백 저장이 이미 확정된 결과를 되돌리지 못하게 하는 경계다.
    if conflicting_terminal:
        return dict(previous)
    accepted_update = job
    current = {**previous, **accepted_update, "jobId": job_id, "updatedAt": now}
    state = _merged_state(previous, accepted_update)
    if state is not None:
        current["state"] = state
    current["createdAt"] = previous.get("createdAt") or job.get("createdAt") or now
    tmp = root / f"job.{os.getpid()}.{threading.get_ident()}.{uuid.uuid4().hex}.tmp"
    try:
        with tmp.open("w", encoding="utf-8") as file:
            json.dump(current, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return current


def save_job(data_root: str | Path, job: dict) -> dict:
    """작업 상태를 프로세스·OS 잠금과 원자 교체로 저장한다."""

    job_id = normalize_job_id(job["jobId"])
    with _job_lock(job_id):
        with _job_file_lock(data_root, job_id):
            return _write_job_unlocked(data_root, {**job, "jobId": job_id})


def create_job(data_root: str | Path, job: dict) -> tuple[dict, bool]:
    """작업을 한 번만 만들고 동일 jobId의 재전달에는 기존 상태를 반환한다."""
    job_id = normalize_job_id(job["jobId"])
    with _job_lock(job_id):
        with _job_file_lock(data_root, job_id):
            path = job_manifest_path(data_root, job_id)
            if path.exists():
                return _load_job_unlocked(data_root, job_id), False
            return _write_job_unlocked(data_root, {**job, "jobId": job_id}, {}), True


def update_job(data_root: str | Path, job_id: str, mutator: Callable[[dict], dict]) -> dict:
    """스레드·OS 파일 잠금 안에서 한 작업을 원자적으로 읽고 변경해 저장한다."""
    normalized = normalize_job_id(job_id)
    with _job_lock(normalized):
        with _job_file_lock(data_root, normalized):
            existing = _load_job_unlocked(data_root, normalized)
            updated = mutator(dict(existing))
            if not isinstance(updated, dict):
                raise TypeError("job mutator must return a dict")
            return _write_job_unlocked(data_root, {**updated, "jobId": normalized}, existing)


def load_job(data_root: str | Path, job_id: str) -> dict:
    """동시 갱신과 직렬화된 상태에서 작업 매니페스트를 읽는다."""

    normalized = normalize_job_id(job_id)
    with _job_lock(normalized):
        with _job_file_lock(data_root, normalized):
            return _load_job_unlocked(data_root, normalized)


def _cancel_response(job: dict, cancel_supported: bool, message: str) -> dict:
    return {
        "jobId": job["jobId"],
        "state": job.get("state"),
        "cancelSupported": cancel_supported,
        "message": message,
    }


def cancel_job(data_root: str | Path, job_id: str) -> dict:
    """추출 작업을 멱등적으로 취소하고 실행 중 연결의 중단 여부를 보고한다."""

    normalized = normalize_job_id(job_id)
    with _job_lock(normalized):
        with _job_file_lock(data_root, normalized):
            job = _load_job_unlocked(data_root, normalized)
            state = str(job.get("state") or "").upper()
            if state in TERMINAL_STATES:
                return _cancel_response(job, False, f"Job already finished with state {state}.")
            if job.get("jobType") != "EXPORT":
                return _cancel_response(job, False, "Cancellation is supported only for extract jobs.")
            if state == "ACCEPTED":
                _request_running_export_cancel(normalized)
                job.update(
                    {
                        "state": "CANCELLED",
                        "cancelSupported": True,
                        "message": "Job cancelled before execution.",
                    }
                )
                saved = _write_job_unlocked(data_root, job, job)
                return _cancel_response(saved, True, saved["message"])
            if state != "RUNNING":
                return _cancel_response(job, False, f"Job state {state or 'UNKNOWN'} cannot be cancelled.")

    cancellation = _running_export(normalized)
    if cancellation is None or not _request_running_export_cancel(normalized):
        return _cancel_response(
            job,
            False,
            "The running process is not registered in this worker, so cancellation cannot be guaranteed.",
        )
    try:
        wait_seconds = max(0.0, float(os.environ.get("EXTRACT_CANCEL_WAIT_SECONDS", "2")))
    except ValueError:
        wait_seconds = 2.0
    cancellation.finished.wait(wait_seconds)
    current = load_job(data_root, normalized)
    if current.get("state") == "CANCELLED":
        return _cancel_response(current, True, "The running extract was cancelled.")
    return _cancel_response(
        current,
        False,
        "Cancellation was signalled, but terminal cancellation has not been confirmed yet.",
    )


def ensure_under_data_root(data_root: str | Path, path: str | Path) -> Path:
    """다운로드 대상이 신뢰된 데이터 루트 아래에 있는지 검증한다."""

    root = Path(data_root).expanduser().resolve()
    resolved = Path(path).expanduser().resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError("job file path is outside DATA_ROOT") from exc
    return resolved


def job_download_file(data_root: str | Path, job_id: str) -> tuple[Path, dict]:
    """완료된 작업의 검증된 다운로드 경로와 상태를 반환한다."""

    job = load_job(data_root, job_id)
    if job.get("state") != "COMPLETED":
        raise ValueError(f"job is not completed: {job.get('state')}")
    file_path = job.get("filePath")
    if not file_path:
        raise FileNotFoundError(f"job has no downloadable file: {normalize_job_id(job_id)}")
    path = ensure_under_data_root(data_root, file_path)
    if not path.exists():
        raise FileNotFoundError(f"job file not found: {path}")
    return path, job
