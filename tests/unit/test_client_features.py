"""T21-T26: context manager, async cancellation, parallel calls, no secrets in logs,
monetary validation, sync/async parity."""

from __future__ import annotations

import asyncio
import logging

import pytest

from tupago3_sdk import (
    AsyncTupagoPaymentClient,
    ClientConfig,
    CreateQrRequest,
    GetStatusRequest,
    LabAsyncJwtProvider,
    LabJwtProvider,
    TupagoPaymentClient,
    TupagoValidationError,
    models,
    to_double_amount,
)


# -- T21 ---------------------------------------------------------------------
def test_T21_context_manager_closes_resources(plaintext_server, lab_secret):
    port, _ = plaintext_server
    config = ClientConfig(endpoint=f"127.0.0.1:{port}")
    client = TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=lab_secret))
    with client:
        assert client.get_cliente(models.GetClienteRequest(id=1)).cliente.nombre == "Cliente Demo"
    assert client._closed is True
    # Idempotent close.
    client.close()
    # Channel is closed; further calls must fail rather than hang/leak.
    with pytest.raises(Exception):
        client.get_cliente(models.GetClienteRequest(id=1))


# -- T22 ---------------------------------------------------------------------
async def test_T22_async_cancellation(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    servicer.slow_create_seconds = 1.0
    config = ClientConfig(endpoint=f"127.0.0.1:{port}", deadline_seconds=10.0)
    client = AsyncTupagoPaymentClient(
        config, token_provider=LabAsyncJwtProvider(base64_secret=lab_secret)
    )
    try:
        task = asyncio.create_task(
            client.create_qr(CreateQrRequest(id_cuenta=1, monto=1.0, idempotency_key="c1"))
        )
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        await client.aclose()
    # Cancellation must NOT trigger a resubmit; exactly one attempt recorded.
    assert servicer.create_qr_count == 1


# -- T23 ---------------------------------------------------------------------
async def test_T23_parallel_client_calls(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    config = ClientConfig(endpoint=f"127.0.0.1:{port}")
    client = AsyncTupagoPaymentClient(
        config, token_provider=LabAsyncJwtProvider(base64_secret=lab_secret)
    )
    try:
        tasks = [
            client.get_qr_status(GetStatusRequest(idc=f"idc-{i}", id_cuenta=1)) for i in range(10)
        ]
        results = await asyncio.gather(*tasks)
    finally:
        await client.aclose()
    assert len(results) == 10
    assert all(r.json for r in results)
    assert servicer.get_qr_status_count == 10


# -- T24 ---------------------------------------------------------------------
def test_T24_no_secrets_in_logs(plaintext_server, lab_secret, caplog):
    port, _ = plaintext_server
    config = ClientConfig(endpoint=f"127.0.0.1:{port}")
    provider = LabJwtProvider(base64_secret=lab_secret)
    token = provider.get_token()
    with caplog.at_level(logging.DEBUG):
        client = TupagoPaymentClient(config, token_provider=provider)
        with client:
            client.get_cliente(models.GetClienteRequest(id=1))
    text = caplog.text
    assert lab_secret not in text
    assert token not in text
    assert "Bearer " not in text
    assert "secret" not in text.lower() or "base64_secret" not in text


# -- T25 ---------------------------------------------------------------------
@pytest.mark.parametrize(
    "value,should_raise",
    [
        (float("nan"), True),
        (float("inf"), True),
        (-1.0, True),
        (0.0, True),
        ("-5.5", True),
        (100.0, False),
        (1, False),
        ("123.45", False),
        (__import__("decimal").Decimal("50.25"), False),
    ],
)
def test_T25_monetary_validation(value, should_raise):
    if should_raise:
        with pytest.raises(TupagoValidationError):
            to_double_amount(value)
    else:
        assert to_double_amount(value) >= 0


def test_T25b_create_rejects_invalid_monto_before_send(plaintext_server, lab_secret):
    port, servicer = plaintext_server
    config = ClientConfig(endpoint=f"127.0.0.1:{port}")
    client = TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=lab_secret))
    with client:
        with pytest.raises(TupagoValidationError):
            client.create_qr(
                CreateQrRequest(id_cuenta=1, monto=float("nan"), idempotency_key="bad")
            )
    # Nothing reached the server.
    assert servicer.create_qr_count == 0


# -- T26 ---------------------------------------------------------------------
async def test_T26_sync_async_parity(plaintext_server, lab_secret):
    port, _ = plaintext_server
    config = ClientConfig(endpoint=f"127.0.0.1:{port}")

    sync = TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=lab_secret))
    with sync:
        s_perfil = sync.get_cliente(models.GetClienteRequest(id=7))

    async_client = AsyncTupagoPaymentClient(
        config, token_provider=LabAsyncJwtProvider(base64_secret=lab_secret)
    )
    try:
        a_perfil = await async_client.get_cliente(models.GetClienteRequest(id=7))
    finally:
        await async_client.aclose()

    assert s_perfil.SerializeToString() == a_perfil.SerializeToString()
