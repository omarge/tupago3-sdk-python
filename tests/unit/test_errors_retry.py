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
)


def _rpc_error(code: grpc.StatusCode, details: str = "x") -> grpc.RpcError:
    e = grpc.RpcError("status")
    e.code = lambda: code  # type: ignore[attr-defined]
    e.details = lambda: details  # type: ignore[attr-defined]
    return e


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
    assert is_retryable(grpc.StatusCode.UNAVAILABLE, operation="GetCliente") is True
    assert is_retryable(grpc.StatusCode.UNAVAILABLE, operation="CreateQr") is False
    assert is_retryable(grpc.StatusCode.NOT_FOUND, operation="GetCliente") is False


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
