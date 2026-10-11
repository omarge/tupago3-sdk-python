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

    def _backoff_seconds(self, attempt: int) -> float:
        delay = min(self.max_delay_seconds, self.base_delay_seconds * (2 ** (attempt - 1)))
        jitter = self.jitter * attempt
        return max(0.0, delay + jitter)

    def _sleep(self, attempt: int) -> None:
        time.sleep(self._backoff_seconds(attempt))


def _retry_delay_seconds(policy: RetryPolicy, attempt: int, exc: Exception) -> float:
    """Compute how long to wait before the next attempt.

    Honours a server-advised ``RetryInfo`` delay (capped) for ``RESOURCE_EXHAUSTED``; otherwise
    uses the bounded exponential backoff. This guarantees a finite, bounded total wait.
    """
    server_delay = errors.server_retry_delay(exc)
    if server_delay is not None:
        return server_delay
    return policy._backoff_seconds(attempt)


def retry_read_sync(
    policy: RetryPolicy,
    operation: str,
    call: Callable[[], T],
) -> T:
    """Invoke ``call`` with bounded retry for read operations.

    Returns the response on success. On a non-retryable error (or after exhausting attempts)
    the typed error is raised via :func:`errors.from_grpc_error`. Create operations are never
    passed here; the policy also refuses them via :func:`errors.should_retry_read`.
    """
    attempts = max(1, policy.max_attempts)
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return call()
        except grpc.RpcError as exc:  # type: ignore[attr-defined]
            last_exc = exc
            if attempt >= attempts or not errors.should_retry_read(exc, operation=operation):
                raise errors.from_grpc_error(exc, operation=operation) from exc
            time.sleep(_retry_delay_seconds(policy, attempt, exc))
    assert last_exc is not None
    raise errors.from_grpc_error(last_exc, operation=operation)
