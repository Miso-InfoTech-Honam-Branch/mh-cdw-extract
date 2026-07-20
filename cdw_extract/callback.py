"""작업 모듈이 공유하는 HTTP callback 전송 경계."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import requests

HttpPost = Callable[..., Any]
Wait = Callable[[float], None]


def callback_options(request: Mapping[str, object], *, legacy_url_key: str | None = None) -> dict:
    """요청의 callback 설정을 정규화한다.

    ``legacy_url_key``는 이전 Boot 요청과의 호환이 필요한 작업에서만 사용한다.
    """
    raw = request.get("callback") or {}
    options = dict(raw) if isinstance(raw, Mapping) else {}
    if legacy_url_key and request.get(legacy_url_key) and not options.get("url"):
        options["url"] = request[legacy_url_key]
    return options


def post_json_callback(
    options: Mapping[str, object] | None,
    payload: Mapping[str, object],
    *,
    operation: str,
    attempts: int = 1,
    backoff_seconds: Sequence[float] = (),
    post: HttpPost = requests.post,
    wait: Wait = time.sleep,
) -> dict | None:
    """JSON callback을 전송하고 일관된 전달 결과를 반환한다.

    작업의 성공 여부와 callback 전달 성공 여부는 별개다. 이 함수는 전송만 담당하며,
    최종 callback 오류를 작업 상태에 기록할지는 각 작업 모듈이 결정한다.
    """
    callback = dict(options or {})
    url = callback.get("url")
    if not url:
        return None
    if attempts < 1:
        raise ValueError("callback attempts must be at least 1")

    headers = callback.get("headers") or {}
    timeout = float(callback.get("timeoutSeconds") or callback.get("timeout") or 10)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = post(url, json=dict(payload), headers=headers, timeout=timeout)
            if 200 <= response.status_code < 300:
                return {"url": url, "statusCode": response.status_code, "attempts": attempt}
            body = response.text[:4096].strip()
            last_error = RuntimeError(
                f"{operation} callback failed status={response.status_code} body={body}"
            )
        except requests.RequestException as exc:
            last_error = exc

        if attempt < attempts:
            delay_index = attempt - 1
            delay = backoff_seconds[delay_index] if delay_index < len(backoff_seconds) else 0
            if delay > 0:
                wait(delay)

    if last_error is None:
        last_error = RuntimeError(f"{operation} callback failed without a response")
    raise last_error

