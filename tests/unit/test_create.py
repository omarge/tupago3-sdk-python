"""T15-T20: create exactly-one-attempt, unknown outcome (unavailable/deadline), pending
reconciliation, idempotency key preserved."""

from __future__ import annotations

import pytest

from tupago3_sdk import (
    ClientConfig,
    CreateQrRequest,
    CreateTelefonicoRequest,
    LabJwtProvider,
    TupagoPaymentClient,
    TupagoPendingReconciliationError,
    TupagoUnknownOutcomeError,
)
from tupago3_sdk.models import to_double_amount


def _client(port, lab_secret, deadline=30.0):
    config = ClientConfig(endpoint=f"127.0.0.1:{port}", deadline_seconds=deadline)
    return TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=lab_secret))


def test_T15_create_qr_exactly_one_attempt(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    servicer.unavailable_on_create = True
    client = _client(port, lab_secret)
    with client:
        with pytest.raises(TupagoUnknownOutcomeError):
            client.create_qr(CreateQrRequest(id_cuenta=1, monto=10.0, idempotency_key="k1"))
    assert servicer.create_qr_count == 1


def test_T16_create_telefonico_exactly_one_attempt(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    servicer.unavailable_on_create = True
    client = _client(port, lab_secret)
    with client:
        with pytest.raises(TupagoUnknownOutcomeError):
            client.create_telefonico(
                CreateTelefonicoRequest(key="K", monto=10.0, idempotency_key="k2")
            )
    assert servicer.create_telefonico_count == 1


def test_T17_create_unavailable_unknown_outcome(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    servicer.unavailable_on_create = True
    client = _client(port, lab_secret)
    with client:
        with pytest.raises(TupagoUnknownOutcomeError) as exc:
            client.create_qr(CreateQrRequest(id_cuenta=1, monto=10.0, idempotency_key="k3"))
    assert exc.value.grpc_code == "UNAVAILABLE"
    assert exc.value.operation == "CreateQr"
    assert exc.value.idempotency_key == "k3"
    assert servicer.create_qr_count == 1


def test_T18_create_deadline_unknown_outcome(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    # Server is slow; client deadline is tiny -> client-side DEADLINE_EXCEEDED.
    servicer.slow_create_seconds = 2.0
    client = _client(port, lab_secret, deadline=0.3)
    with client:
        with pytest.raises(TupagoUnknownOutcomeError) as exc:
            client.create_qr(CreateQrRequest(id_cuenta=1, monto=10.0, idempotency_key="k4"))
    assert exc.value.grpc_code in ("DEADLINE_EXCEEDED", "UNAVAILABLE")
    assert servicer.create_qr_count == 1


def test_T19_pending_reconciliation(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    servicer.pending_reconciliation_on_create = True
    client = _client(port, lab_secret)
    with client:
        with pytest.raises(TupagoPendingReconciliationError) as exc:
            client.create_qr(CreateQrRequest(id_cuenta=1, monto=10.0, idempotency_key="k5"))
    assert errors_pending_code(exc.value)
    assert servicer.create_qr_count == 1


def errors_pending_code(err) -> bool:
    return "TUPAGO_PENDING_RECONCILIATION" in (err.message or "")


def test_T20_idempotency_key_preserved(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    key = "stable-idempotency-key-123"
    client = _client(port, lab_secret)
    with client:
        resp = client.create_qr(CreateQrRequest(id_cuenta=1, monto=10.0, idempotency_key=key))
    assert servicer.create_qr_count == 1
    assert servicer.received_qr_keys == [key]
    # Response echoes the key (server uses it as 'key' field in JSON).
    assert key in resp.json


def test_T20b_decimal_amount_preserved():
    # Explicit Decimal -> double conversion is documented; assert round-trip via wire.
    from tupago3_sdk.client import build_create_qr

    req = CreateQrRequest(
        id_cuenta=1, monto=__import__("decimal").Decimal("100.50"), idempotency_key="d1"
    )
    built = build_create_qr(req)
    assert built.monto == 100.50
    assert to_double_amount(__import__("decimal").Decimal("100.50")) == 100.50
