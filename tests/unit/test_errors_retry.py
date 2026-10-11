"""T12: typed gRPC error matrix. T13-T14: read retry bounded / non-retryable."""

from __future__ import annotations

import grpc
import pytest

from tupago3_sdk import (
    ClientConfig,
    LabJwtProvider,
    RetryPolicy,
    TupagoAlreadyExistsError,
    TupagoCancelledError,
    TupagoDeadlineExceededError,
    TupagoError,
    TupagoFailedPreconditionError,
    TupagoInternalError,
    TupagoInvalidArgumentError,
    TupagoNotFoundError,
    TupagoPendingReconciliationError,
    TupagoPermissionDeniedError,
    TupagoUnauthenticatedError,
    TupagoUnavailableError,
    TupagoUnknownOutcomeError,
    from_grpc_error,
    is_retryable,
    retry,
    server_retry_delay,
    should_retry_read,
)


def _rpc_error(code: grpc.StatusCode, details: str = "x", trailing=None) -> grpc.RpcError:
    e = grpc.RpcError("status")
    e.code = lambda: code  # type: ignore[attr-defined]
    e.details = lambda: details  # type: ignore[attr-defined]
    if trailing is not None:
        e.trailing_metadata = lambda: trailing  # type: ignore[attr-defined]
    return e


def _retryinfo_trailing(seconds: int, nanos: int = 0) -> list[tuple[str, bytes]]:
    from google.rpc import error_details_pb2, status_pb2

    st = status_pb2.Status()
    st.code = 8  # RESOURCE_EXHAUSTED
    st.message = "rate limited"
    info = error_details_pb2.RetryInfo()
    info.retry_delay.seconds = seconds
    info.retry_delay.nanos = nanos
    st.details.add().Pack(info)
    return [("grpc-status-details-bin", st.SerializeToString())]


@pytest.mark.parametrize(
    "code,exc_type,retryable",
    [
        (grpc.StatusCode.NOT_FOUND, TupagoNotFoundError, False),
        (grpc.StatusCode.UNAUTHENTICATED, TupagoUnauthenticatedError, False),
        (grpc.StatusCode.PERMISSION_DENIED, TupagoPermissionDeniedError, False),
        (grpc.StatusCode.INVALID_ARGUMENT, TupagoInvalidArgumentError, False),
        (grpc.StatusCode.ALREADY_EXISTS, TupagoAlreadyExistsError, False),
        (grpc.StatusCode.FAILED_PRECONDITION, TupagoFailedPreconditionError, False),
        (grpc.StatusCode.INTERNAL, TupagoInternalError, False),
        (grpc.StatusCode.UNAVAILABLE, TupagoUnavailableError, True),
        (grpc.StatusCode.DEADLINE_EXCEEDED, TupagoDeadlineExceededError, False),
        (grpc.StatusCode.CANCELLED, TupagoCancelledError, False),
    ],
)
def test_T12_typed_grpc_error_matrix(code, exc_type, retryable):
    err = from_grpc_error(_rpc_error(code), operation="GetCliente")
    assert isinstance(err, exc_type)
    assert isinstance(err, TupagoError)
    assert err.retryable is retryable
    assert err.grpc_code == code.name


def test_T12_pending_reconciliation_detected():
    err = from_grpc_error(
        _rpc_error(grpc.StatusCode.ABORTED, "TUPAGO_PENDING_RECONCILIATION; do not retry"),
        operation="CreateQr",
    )
    assert isinstance(err, TupagoPendingReconciliationError)
    assert not err.retryable


def test_T12_unknown_outcome_on_create():
    err = from_grpc_error(
        _rpc_error(grpc.StatusCode.UNAVAILABLE, "boom"),
        operation="CreateQr",
        unknown_outcome_on_create=True,
    )
    assert isinstance(err, TupagoUnknownOutcomeError)
    assert not err.retryable


def test_T12_is_retryable_rules():
    # Read transients.
    assert is_retryable(grpc.StatusCode.UNAVAILABLE, operation="GetCliente") is True
    assert is_retryable(grpc.StatusCode.DEADLINE_EXCEEDED, operation="GetCliente") is True
    # Creates are never retried, even for transient codes.
    assert is_retryable(grpc.StatusCode.UNAVAILABLE, operation="CreateQr") is False
    # Non-transient (typed) codes are never retried.
    assert is_retryable(grpc.StatusCode.NOT_FOUND, operation="GetCliente") is False
    # G3 hardening: CANCELLED and INTERNAL must NOT be treated as generic transients.
    assert is_retryable(grpc.StatusCode.CANCELLED, operation="GetCliente") is False
    assert is_retryable(grpc.StatusCode.INTERNAL, operation="GetCliente") is False
    # G3 hardening: RESOURCE_EXHAUSTED is only retryable when the server advertises RetryInfo.
    assert is_retryable(grpc.StatusCode.RESOURCE_EXHAUSTED, operation="GetCliente") is False
    assert (
        is_retryable(
            grpc.StatusCode.RESOURCE_EXHAUSTED,
            operation="GetCliente",
            exc=_rpc_error(
                code=grpc.StatusCode.RESOURCE_EXHAUSTED, trailing=_retryinfo_trailing(2)
            ),
        )
        is True
    )


def test_T13_read_retry_bounded(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    servicer.unavailable_on_read = True
    servicer.get_cliente_count = 0
    config = ClientConfig(endpoint=f"127.0.0.1:{port}")
    client = __import__("tupago3_sdk").TupagoPaymentClient(
        config,
        token_provider=LabJwtProvider(base64_secret=lab_secret),
        retry_policy=RetryPolicy(max_attempts=3),
    )
    with client:
        with pytest.raises(TupagoUnavailableError):
            client.get_cliente(__import__("tupago3_sdk").models.GetClienteRequest(id=1))
    # Exactly max_attempts invocations (bounded, no endless retry).
    assert servicer.get_cliente_count == 3


def test_T13b_read_retry_then_success():
    calls = {"n": 0}

    def call():
        calls["n"] += 1
        if calls["n"] == 1:
            raise _rpc_error(grpc.StatusCode.UNAVAILABLE, "transient")
        return "ok"

    result = retry.retry_read_sync(
        RetryPolicy(max_attempts=3, base_delay_seconds=0.001), "GetCliente", call
    )
    assert result == "ok"
    assert calls["n"] == 2


def test_T14_read_non_retryable_error(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    servicer.not_found_on_get_cliente = True
    servicer.get_cliente_count = 0
    config = ClientConfig(endpoint=f"127.0.0.1:{port}")
    client = __import__("tupago3_sdk").TupagoPaymentClient(
        config,
        token_provider=LabJwtProvider(base64_secret=lab_secret),
        retry_policy=RetryPolicy(max_attempts=5),
    )
    with client:
        with pytest.raises(TupagoNotFoundError):
            client.get_cliente(__import__("tupago3_sdk").models.GetClienteRequest(id=1))
    # NOT_FOUND is non-retryable: exactly one attempt.
    assert servicer.get_cliente_count == 1


# --- G3 retry hardening (CANCELLED / INTERNAL / RESOURCE_EXHAUSTED) -------------------


def _counted_failing(call_code, trailing=None):
    calls = {"n": 0}

    def call():
        calls["n"] += 1
        raise _rpc_error(call_code, "boom", trailing=trailing)

    return calls, lambda: call()


def test_T_G3_cancelled_not_retried():
    calls, call = _counted_failing(grpc.StatusCode.CANCELLED)
    with pytest.raises(Exception):
        retry.retry_read_sync(
            RetryPolicy(max_attempts=3, base_delay_seconds=0.001), "GetCliente", call
        )
    assert calls["n"] == 1


def test_T_G3_internal_not_retried():
    calls, call = _counted_failing(grpc.StatusCode.INTERNAL)
    with pytest.raises(Exception):
        retry.retry_read_sync(
            RetryPolicy(max_attempts=3, base_delay_seconds=0.001), "GetCliente", call
        )
    assert calls["n"] == 1


def test_T_G3_resource_exhausted_no_retryinfo_no_storm():
    # Without a server RetryInfo, RESOURCE_EXHAUSTED must NOT be retried (no retry storm).
    calls, call = _counted_failing(grpc.StatusCode.RESOURCE_EXHAUSTED)
    with pytest.raises(Exception):
        retry.retry_read_sync(
            RetryPolicy(max_attempts=3, base_delay_seconds=0.001), "GetCliente", call
        )
    assert calls["n"] == 1


def test_T_G3_resource_exhausted_with_retryinfo_retries():
    # With a server RetryInfo delay, RESOURCE_EXHAUSTED IS retried (bounded by max_attempts).
    calls, call = _counted_failing(
        grpc.StatusCode.RESOURCE_EXHAUSTED, trailing=_retryinfo_trailing(0, nanos=1_000_000)
    )
    with pytest.raises(Exception):
        retry.retry_read_sync(
            RetryPolicy(max_attempts=3, base_delay_seconds=0.001), "GetCliente", call
        )
    assert calls["n"] == 3


def test_T_G3_server_retry_delay_capped():
    delay = server_retry_delay(
        _rpc_error(grpc.StatusCode.RESOURCE_EXHAUSTED, trailing=_retryinfo_trailing(999))
    )
    assert delay is not None
    assert delay == 10.0  # capped at MAX_SERVER_RETRY_DELAY


def test_T_G3_should_retry_read_consistency():
    assert (
        should_retry_read(_rpc_error(grpc.StatusCode.UNAVAILABLE), operation="GetCliente") is True
    )
    assert should_retry_read(_rpc_error(grpc.StatusCode.CANCELLED), operation="GetCliente") is False
    assert should_retry_read(_rpc_error(grpc.StatusCode.INTERNAL), operation="GetCliente") is False
    assert (
        should_retry_read(_rpc_error(grpc.StatusCode.RESOURCE_EXHAUSTED), operation="GetCliente")
        is False
    )
    assert (
        should_retry_read(
            _rpc_error(grpc.StatusCode.RESOURCE_EXHAUSTED, trailing=_retryinfo_trailing(1)),
            operation="GetCliente",
        )
        is True
    )
