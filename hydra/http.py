"""Shared HTTP layer for every provider.

One ``requests.Session`` per thread (created lazily), wrapped in an
``HTTPAdapter`` with a tuned ``urllib3.util.retry.Retry``. Idempotent verbs
(GET/HEAD) get transparent retries on 429/5xx through urllib3. Mutating verbs
(POST/PUT/PATCH/DELETE) never auto-retry through urllib3 — instead, the
``request()`` wrapper retries them **once** when the failure is a fresh-
connect failure (a ``urllib3.exceptions.NewConnectionError`` somewhere in
the cause chain: TCP refused / DNS / connect-timeout — i.e. no bytes ever
left the wire). Mid-request resets, read-timeouts, and 5xx responses are
**never** retried for mutations, since the peer may have already processed
the write.

Every outbound call carries a ``(connect, read)`` timeout. Without this, a
hung peer bypasses the retry path entirely and blocks the caller forever.
Override via ``HYDRA_HTTP_TIMEOUT_CONNECT`` / ``HYDRA_HTTP_TIMEOUT_READ``.

Retry telemetry (count by hostname) is exposed via ``pop_retry_stats()`` so
the CLI can surface "Retried N transient errors" in command footers. The
counter only bumps for retries that *actually happened*, not for exhausted
budgets (where ``Retry.increment`` raises ``MaxRetryError``).
"""

from __future__ import annotations

import os
import threading
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import NewConnectionError
from urllib3.util.retry import Retry

# ── Timeouts ────────────────────────────────────────────────────────────

DEFAULT_CONNECT_TIMEOUT = 5.0
DEFAULT_READ_TIMEOUT = 30.0


def _default_timeout() -> Tuple[float, float]:
    """Resolve the (connect, read) timeout, honouring env-var overrides each call.

    Re-reading env on every call lets tests use ``monkeypatch.setenv`` without
    fighting module-import caching.
    """
    try:
        connect = float(os.environ.get("HYDRA_HTTP_TIMEOUT_CONNECT", DEFAULT_CONNECT_TIMEOUT))
    except ValueError:
        connect = DEFAULT_CONNECT_TIMEOUT
    try:
        read = float(os.environ.get("HYDRA_HTTP_TIMEOUT_READ", DEFAULT_READ_TIMEOUT))
    except ValueError:
        read = DEFAULT_READ_TIMEOUT
    return connect, read


DEFAULT_TIMEOUT: Tuple[float, float] = (DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT)


# ── Retry telemetry ─────────────────────────────────────────────────────

_stats_lock = threading.Lock()
_retry_counts: Dict[str, int] = {}


def _host_of(url: str) -> str:
    try:
        return urlparse(url).hostname or "unknown"
    except ValueError:
        return "unknown"


def _bump_retry(url: str) -> None:
    _bump_retry_host(_host_of(url))


def _bump_retry_host(host: str) -> None:
    host = host or "unknown"
    with _stats_lock:
        _retry_counts[host] = _retry_counts.get(host, 0) + 1


def pop_retry_stats() -> Dict[str, int]:
    """Return a snapshot of the retry counters and clear them."""
    with _stats_lock:
        out = dict(_retry_counts)
        _retry_counts.clear()
    return out


def reset_retry_stats() -> None:
    with _stats_lock:
        _retry_counts.clear()


class _CountingRetry(Retry):
    """Retry subclass that records each retry attempt by host.

    The bump happens *after* ``super().increment`` returns — so attempts that
    exhaust the budget (where increment raises ``MaxRetryError``) do not get
    counted as a retry. Counter semantics: "retries actually performed".

    urllib3 passes ``url`` as a path-only (e.g. ``/api/v4/projects``), so we
    extract the host from ``_pool`` (the ``HTTPConnectionPool``) instead.
    """

    def increment(  # type: ignore[override]
        self,
        method=None,
        url=None,
        response=None,
        error=None,
        _pool=None,
        _stacktrace=None,
    ):
        new_retry = super().increment(
            method=method,
            url=url,
            response=response,
            error=error,
            _pool=_pool,
            _stacktrace=_stacktrace,
        )
        host = getattr(_pool, "host", None) or _host_of(url or "")
        _bump_retry_host(host)
        return new_retry


# ── Session ─────────────────────────────────────────────────────────────

_thread_local = threading.local()


def _build_retry() -> Retry:
    return _CountingRetry(
        total=3,
        backoff_factor=0.3,
        status_forcelist=(429, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )


def session() -> requests.Session:
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=_build_retry())
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _thread_local.session = s
    return s


# ── Request wrapper ─────────────────────────────────────────────────────

_MUTATING = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def request(method: str, url: str, **kwargs: Any) -> requests.Response:
    """Issue an HTTP request via the per-thread session.

    - Injects ``timeout=_default_timeout()`` if absent.
    - For mutating verbs, retries **once** on a fresh-connect failure only —
      i.e. the underlying urllib3 cause is ``NewConnectionError`` (TCP refused
      / DNS / connect-timeout before any bytes left the wire). Mid-request
      resets and read-timeouts are **not** retried, since the peer may have
      already processed the write.
    """
    kwargs.setdefault("timeout", _default_timeout())
    upper = method.upper()
    s = session()
    if upper not in _MUTATING:
        return s.request(upper, url, **kwargs)

    try:
        return s.request(upper, url, **kwargs)
    except requests.exceptions.ConnectionError as exc:
        if not _is_fresh_connect_failure(exc):
            raise
        _bump_retry(url)
        return s.request(upper, url, **kwargs)


def _is_fresh_connect_failure(exc: BaseException) -> bool:
    """True iff this ConnectionError happened before any bytes hit the wire.

    Walks the exception chain looking for urllib3's ``NewConnectionError``,
    which is the only cause we know is safe to retry for a mutating verb.
    """
    seen = set()
    cur: Optional[BaseException] = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, NewConnectionError):
            return True
        # requests wraps urllib3 exceptions in .args[0] sometimes, and chains
        # via __cause__ / __context__ in others. Cover both.
        cur = cur.__cause__ or cur.__context__
    # Fallback: requests.exceptions.ConnectionError sometimes stores the
    # urllib3 cause as the first positional arg.
    if exc.args and isinstance(exc.args[0], NewConnectionError):
        return True
    return False


def get(url: str, **kwargs: Any) -> requests.Response:
    return request("GET", url, **kwargs)


def post(url: str, **kwargs: Any) -> requests.Response:
    return request("POST", url, **kwargs)


def delete(url: str, **kwargs: Any) -> requests.Response:
    return request("DELETE", url, **kwargs)


def put(url: str, **kwargs: Any) -> requests.Response:
    return request("PUT", url, **kwargs)
