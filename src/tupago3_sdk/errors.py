"""Typed exception hierarchy and gRPC status mapping for the TUPAGO3 Python SDK.

Every non-OK gRPC terminal status is mapped to a stable, typed Python exception so callers
can branch deterministically. Financial-critical conditions get dedicated, non-retryable
types:

* :class:`TupagoPendingReconciliationError` — the backend returned the stable
  ``TUPAGO_PENDING_RECONCILIATION`` condition (gRPC ``ABORTED``). The create outcome is
  unknown and MUST NOT be resubmitted; the caller must query status first.
* :class:`TupagoUnknownOutcomeError` — the SDK observed a transport failure after the
  request may have been sent, so the create outcome is indeterminate. Non-retryable and
  carries the operation name, idempotency key and original gRPC code for reconciliation.
"""

from __future__ import annotations

from grpc import StatusCode

try:
    from google.rpc import error_details_pb2, status_pb2

    _HAVE_GRPC_STATUS = True
except Exception:  # pragma: no cover - optional dependency
    error_details_pb2 = None
    status_pb2 = None
    _HAVE_GRPC_STATUS = False

# Stable application code embedded by backend3 (TupagoGrpcService.UNKNOWN_REMOTE_STATE_CODE).
PENDING_RECONCILIATION_CODE = "TUPAGO_PENDING_RECONCILIATION"

# Upper bound (seconds) we will ever wait for a server-advised retry delay. This prevents a
# malicious or buggy backend from inducing an arbitrarily long client sleep.
MAX_SERVER_RETRY_DELAY = 10.0

# gRPC status codes that MAY be retried for *read-only* operations.
#
# * CANCELLED is intentionally excluded: it means the call was cancelled (client deadline /
#   propagation) and retrying it is wrong — it must propagate immediately and never be retried.
# * INTERNAL is intentionally excluded: it signals a server-side fault that is not generically
#   safe to retry (state may be indeterminate); treating it as a generic transient would hide
#   real bugs and risk duplicate side effects.
# * RESOURCE_EXHAUSTED is only retried when the server advertises a safe retry delay via a
#   ``RetryInfo`` (carried in ``grpc-status-details-bin``); otherwise it is non-retryable to
#   avoid amplifying load (retry storm).
READ_RETRYABLE_CODES = frozenset(
    {
        StatusCode.UNAVAILABLE,
        StatusCode.DEADLINE_EXCEEDED,
        StatusCode.RESOURCE_EXHAUSTED,
    }
)


class TupagoError(Exception):
    """Base class for all TUPAGO3 SDK errors."""

    #: Whether the operation may be safely retried by the caller. Subclasses override.
    retryable: bool = False

    def __init__(
        self,
        message: str,
        *,
        grpc_code: str | None = None,
        operation: str | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.grpc_code = grpc_code
        self.operation = operation
        self.idempotency_key = idempotency_key


class TupagoTransportError(TupagoError):
    """Low-level transport / channel failure (before a typed status is known)."""


class InsecureTransportError(TupagoError):
    """Raised when a transport configuration would violate the fail-closed TLS rule."""


class TupagoValidationError(TupagoError):
    """Local request validation failure (e.g. monetary amount)."""


class TupagoNotFoundError(TupagoError):
    """gRPC NOT_FOUND — referenced entity does not exist."""


class TupagoUnauthenticatedError(TupagoError):
    """gRPC UNAUTHENTICATED — missing or invalid bearer token."""


class TupagoPermissionDeniedError(TupagoError):
    """gRPC PERMISSION_DENIED — authenticated but not authorized for the resource."""


class TupagoInvalidArgumentError(TupagoError):
    """gRPC INVALID_ARGUMENT — malformed request."""


class TupagoAlreadyExistsError(TupagoError):
    """gRPC ALREADY_EXISTS — idempotency conflict or policy revision conflict."""


class TupagoFailedPreconditionError(TupagoError):
    """gRPC FAILED_PRECONDITION — amount limit / policy not satisfied."""


class TupagoAbortedError(TupagoError):
    """gRPC ABORTED — operation aborted (non-transient)."""


class TupagoInternalError(TupagoError):
    """gRPC INTERNAL — server-side failure; non-transient by default."""


class TupagoUnavailableError(TupagoError):
    """gRPC UNAVAILABLE — transient server unavailability."""

    retryable = True


class TupagoDeadlineExceededError(TupagoError):
    """gRPC DEADLINE_EXCEEDED — request timed out."""


class TupagoCancelledError(TupagoError):
    """gRPC CANCELLED — call was cancelled."""


class TupagoUnknownError(TupagoError):
    """gRPC status that is not explicitly modelled."""


class TupagoPendingReconciliationError(TupagoAbortedError):
    """Backend returned TUPAGO_PENDING_RECONCILIATION.

    The create outcome is unknown (the dangerous crash window). Non-retryable: resubmitting
    risks a duplicate payment. The caller must query status and reconcile.
    """

    retryable = False


class TupagoUnknownOutcomeError(TupagoError):
    """Create outcome indeterminate after a transport failure.

    Raised by create operations when the SDK cannot establish whether the request was
    received by the backend (e.g. UNAVAILABLE / DEADLINE_EXCEEDED / CANCELLED / ABORTED /
    transport drop after send). ``ALREADY_EXISTS`` is NOT mapped here: a create with a
    conflicting idempotency key is a deterministic :class:`TupagoAlreadyExistsError`. The SDK
    guarantees at most one gRPC attempt, but cannot guarantee exactly-once server execution;
    the caller must reconcile via status queries.
    """

    retryable = False


_STATUS_MAP = {
    StatusCode.NOT_FOUND: TupagoNotFoundError,
    StatusCode.UNAUTHENTICATED: TupagoUnauthenticatedError,
    StatusCode.PERMISSION_DENIED: TupagoPermissionDeniedError,
    StatusCode.INVALID_ARGUMENT: TupagoInvalidArgumentError,
    StatusCode.ALREADY_EXISTS: TupagoAlreadyExistsError,
    StatusCode.FAILED_PRECONDITION: TupagoFailedPreconditionError,
    StatusCode.ABORTED: TupagoAbortedError,
    StatusCode.INTERNAL: TupagoInternalError,
    StatusCode.UNAVAILABLE: TupagoUnavailableError,
    StatusCode.DEADLINE_EXCEEDED: TupagoDeadlineExceededError,
    StatusCode.CANCELLED: TupagoCancelledError,
}


def _is_pending_reconciliation(description: str | None) -> bool:
    return bool(description and PENDING_RECONCILIATION_CODE in description)


def from_grpc_error(
    exc: Exception,
    *,
    operation: str | None = None,
    idempotency_key: str | None = None,
    unknown_outcome_on_create: bool = False,
) -> TupagoError:
    """Map a ``grpc.RpcError`` (or similar) to a typed :class:`TupagoError`.

    ``unknown_outcome_on_create`` should be set by create operations: when the terminal
    status is one of the indeterminate codes, a :class:`TupagoUnknownOutcomeError` is
    returned instead of the raw typed error, preserving the create financial invariant.
    """
    code_attr = getattr(exc, "code", None)
    code = code_attr() if callable(code_attr) else code_attr
    code_name = (
        code.name if isinstance(code, StatusCode) else (str(code) if code is not None else None)
    )
    details_attr = getattr(exc, "details", None)
    details = details_attr() if callable(details_attr) else details_attr

    # Pending reconciliation is carried on ABORTED with a stable description code.
    if (
        isinstance(code, StatusCode)
        and code == StatusCode.ABORTED
        and _is_pending_reconciliation(details)
    ):
        return TupagoPendingReconciliationError(
            details or "Payment pending reconciliation; do not retry; query status first",
            grpc_code=code_name,
            operation=operation,
            idempotency_key=idempotency_key,
        )

    if (
        unknown_outcome_on_create
        and isinstance(code, StatusCode)
        and code
        in (
            StatusCode.UNAVAILABLE,
            StatusCode.DEADLINE_EXCEEDED,
            StatusCode.CANCELLED,
            StatusCode.ABORTED,
        )
    ):
        return TupagoUnknownOutcomeError(
            f"Create outcome indeterminate after {code_name}: {details or 'no detail'}",
            grpc_code=code_name,
            operation=operation,
            idempotency_key=idempotency_key,
        )

    exc_type = (
        _STATUS_MAP.get(code, TupagoUnknownError)
        if isinstance(code, StatusCode)
        else TupagoTransportError
    )
    return exc_type(
        details or str(exc),
        grpc_code=code_name,
        operation=operation,
        idempotency_key=idempotency_key,
    )


def _retry_delay_seconds_from_exc(exc: object) -> float | None:
    """Return the server-advised retry delay (seconds) from ``RetryInfo``, if present.

    The backend may attach a ``google.rpc.RetryInfo`` inside ``grpc-status-details-bin`` on a
    ``RESOURCE_EXHAUSTED`` (or other) status. We only honour an explicit, positive delay and
    cap it at :data:`MAX_SERVER_RETRY_DELAY`. Returns ``None`` when no safe delay is signalled.
    """
    if not _HAVE_GRPC_STATUS or status_pb2 is None or error_details_pb2 is None:
        return None
    try:
        trailing = getattr(exc, "trailing_metadata", None)
        if callable(trailing):
            trailing = trailing()
        if not trailing:
            return None
        raw = None
        for key, value in trailing:
            if key == "grpc-status-details-bin":
                raw = value
                break
        if not raw:
            return None
        status = status_pb2.Status()
        status.ParseFromString(raw)
        for detail in status.details:
            if detail.type_url.endswith("RetryInfo"):
                info = error_details_pb2.RetryInfo()
                detail.Unpack(info)
                delay = info.retry_delay
                total = float(delay.seconds) + float(delay.nanos) / 1e9
                if total > 0:
                    return min(total, MAX_SERVER_RETRY_DELAY)
    except Exception:
        return None
    return None


def server_retry_delay(exc: object) -> float | None:
    """Public accessor for the server-advised retry delay (seconds), or ``None``."""
    return _retry_delay_seconds_from_exc(exc)


def is_retryable(
    code: object,
    *,
    operation: str | None = None,
    exc: object | None = None,
) -> bool:
    """Return whether ``code`` may be retried.

    Create operations are NEVER retryable (``operation`` starts with ``Create``). Read-only
    operations may retry only ``UNAVAILABLE`` / ``DEADLINE_EXCEEDED``, or ``RESOURCE_EXHAUSTED``
    when the server advertises a safe ``RetryInfo`` delay via ``exc``.
    """
    if operation and operation.startswith("Create"):
        return False
    if not isinstance(code, StatusCode):
        if callable(code):
            try:
                code = code()
            except Exception:
                return False
        else:
            return False
    if not isinstance(code, StatusCode):
        return False
    if code in (StatusCode.UNAVAILABLE, StatusCode.DEADLINE_EXCEEDED):
        return True
    if code == StatusCode.RESOURCE_EXHAUSTED:
        # Only retry when the server explicitly signals a safe delay (RetryInfo); blindly
        # retrying RESOURCE_EXHAUSTED would amplify load (retry storm).
        return _retry_delay_seconds_from_exc(exc) is not None
    return False


def should_retry_read(exc: object, *, operation: str | None = None) -> bool:
    """Convenience: resolve the gRPC code from ``exc`` and decide retryability for a read."""
    code_attr = getattr(exc, "code", None)
    code = code_attr() if callable(code_attr) else code_attr
    return is_retryable(code, operation=operation, exc=exc)
