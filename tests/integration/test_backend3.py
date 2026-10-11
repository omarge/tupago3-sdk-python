"""Integration against the REAL backend3 gRPC server (read-only).

This test is intentionally isolated from the contract/unit suites. It probes
``127.0.0.1:9090`` (backend3's opt-in gRPC port). If the server is not listening, the test
is SKIPPED and recorded as BLOCKED in the final certification report — a mock is NEVER
substituted for a real integration. When available, it exercises read RPCs with valid /
invalid / missing JWT and NOT_FOUND, and verifies the real Protobuf contract.

No Create call is ever issued against the real backend. No production credentials are used;
the lab Base64 secret is read from backend3's dev profile only when present.
"""

from __future__ import annotations

import os
import socket

import pytest

from tupago3_sdk import (
    AsyncTupagoPaymentClient,
    ClientConfig,
    GetClienteRequest,
    GetStatusRequest,
    LabAsyncJwtProvider,
    LabJwtProvider,
    StaticTokenProvider,
    TupagoNotFoundError,
    TupagoPaymentClient,
    TupagoUnauthenticatedError,
)

BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 9090
BACKEND_DEV_YML = "/home/geo/tupago2/backend3/src/main/resources/config/application-dev.yml"


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _read_lab_secret() -> str | None:
    if not os.path.exists(BACKEND_DEV_YML):
        return None
    with open(BACKEND_DEV_YML) as fh:
        for line in fh:
            if "base64-secret:" in line:
                return line.split("base64-secret:", 1)[1].strip().strip('"')
    return None


@pytest.fixture(scope="module")
def backend_available():
    if not _port_open(BACKEND_HOST, BACKEND_PORT):
        pytest.skip("BACKEND3 NOT AVAILABLE — integration BLOCKED (port 9090 closed)")
    secret = _read_lab_secret()
    if not secret:
        pytest.skip("BACKEND3 available but lab secret not found — BLOCKED")
    return secret


def test_backend3_read_valid_jwt(backend_available):
    secret = backend_available
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=secret))
    with client:
        # Synthetic id; backend may return NOT_FOUND for unknown ids, which still proves the
        # contract and auth path. We accept either a profile or a typed NOT_FOUND.
        try:
            client.get_cliente(GetClienteRequest(id=1))
        except TupagoNotFoundError:
            pass


def test_backend3_read_invalid_jwt(backend_available):
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = TupagoPaymentClient(config, token_provider=StaticTokenProvider("invalid.token.value"))
    with client:
        with pytest.raises(TupagoUnauthenticatedError):
            client.get_cliente(GetClienteRequest(id=1))


def test_backend3_read_missing_jwt(backend_available):
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = TupagoPaymentClient(config, token_provider=None)
    with client:
        with pytest.raises(TupagoUnauthenticatedError):
            client.get_cliente(GetClienteRequest(id=1))


async def test_backend3_async_read(backend_available):
    secret = backend_available
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = AsyncTupagoPaymentClient(
        config, token_provider=LabAsyncJwtProvider(base64_secret=secret)
    )
    try:
        try:
            await client.get_qr_status(GetStatusRequest(idc="synthetic", id_cuenta=1))
        except TupagoNotFoundError:
            pass
    finally:
        await client.aclose()
