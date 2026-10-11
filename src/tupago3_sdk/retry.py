"""Bounded retry policy for *read-only* operations.

Create operations must NEVER use this wrapper — the financial invariant requires at most one
gRPC attempt for creates, and :class:`TupagoPaymentClient` / :class:`AsyncTupagoPaymentClient`
invoke create RPCs directly. This module exists only to make read RPCs resilient to transient
transport errors (UNAVAILABLE / DEADLINE_EXCEEDED / CANCELLED / INTERNAL / RESOURCE_EXHAUSTED)
with a hard attempt ceiling and capped backoff.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

import grpc

from tupago3_sdk import errors

T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded retry policy for read operations."""

    max_attempts: int = 3
    base_delay_seconds: float = 0.1
    max_delay_seconds: float = 1.0
    # Operation names starting with "Create" are excluded by the callers; this is a secondary
    # guard so the policy itself refuses to retry creates.
    jitter: float = 0.05

    def _sleep(self, attempt: int) -> None:
        delay = min(self.max_delay_seconds, self.base_delay_seconds * (2 ** (attempt - 1)))
        jitter = self.jitter * attempt
        time.sleep(max(0.0, delay + (jitter if jitter else 0.0)))


def _is_retryable_rpc_error(exc: Exception, operation: str) -> bool:
    code = getattr(exc, "code", None)
    if callable(code) and not isinstance(code, grpc.StatusCode):
        try:
            code = code()
        except Exception:
            return False
    if not isinstance(code, grpc.StatusCode):
        return False
    return errors.is_retryable(code, operation=operation)


def retry_read_sync(
    policy: RetryPolicy,
    operation: str,
    call: Callable[[], T],
) -> T:
    """Invoke ``call`` with bounded retry for read operations.

    Returns the response on success. On a non-retryable error (or after exhausting attempts)
    the typed error is raised via :func:`errors.from_grpc_error`.
    """
    attempts = max(1, policy.max_attempts)
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return call()
        except grpc.RpcError as exc:  # type: ignore[attr-defined]
            last_exc = exc
            if attempt >= attempts or not _is_retryable_rpc_error(exc, operation):
                raise errors.from_grpc_error(exc, operation=operation) from exc
            policy._sleep(attempt)
    assert last_exc is not None
    raise errors.from_grpc_error(last_exc, operation=operation)
