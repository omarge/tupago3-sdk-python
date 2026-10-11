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
    GetCatalogoRequest,
    GetClienteRequest,
    GetStatusRequest,
    LabAsyncJwtProvider,
    LabJwtProvider,
    StaticTokenProvider,
    TupagoNotFoundError,
    TupagoPaymentClient,
    TupagoPermissionDeniedError,
    TupagoUnauthenticatedError,
)

BACKEND_HOST = "127.0.0.1"
BACKEND_PORT = 9090
BACKEND_DEV_YML = "/home/geo/tupago2/backend3/src/main/resources/config/application-dev.yml"

# Synthetic owner principals present in the lab dataset. `prodgcp` (jhi_user id 1054) owns
# cliente id=1 (clave_institucion 40030); `testgcp` (id 1053) owns cliente id=2 (40997).
OWNER_PROD = "prodgcp"
OWNER_DEV = "testgcp"


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


def test_backend3_positive_get_cliente(backend_available):
    secret = backend_available
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = TupagoPaymentClient(
        config, token_provider=LabJwtProvider(base64_secret=secret, sub=OWNER_PROD)
    )
    with client:
        perfil = client.get_cliente(GetClienteRequest(id=1))
    # Concrete fields from the synthetic dataset (cliente id=1 owned by principal 'prodgcp').
    # NOT_FOUND is NOT accepted here — this must be a real positive response.
    cliente = perfil.cliente
    assert cliente.id == 1
    assert cliente.nombre == "GESTION CORPORATIVA DE PROYECTOS SA DE C"
    assert cliente.clave_institucion == "40030"
    assert perfil.catalogo_institucion.clave_institucion == "40030"
    assert perfil.catalogo_institucion.nombre_corto == "BAJIO"


def test_backend3_positive_get_catalogo(backend_available):
    secret = backend_available
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=secret))
    with client:
        catalogo = client.get_catalogo(GetCatalogoRequest(clave_institucion="301"))
    # Concrete fields from the synthetic catalogo reference data.
    assert catalogo.id == 72797
    assert catalogo.clave_institucion == "301"
    assert catalogo.nombre_corto == "MEMBER_A"


def test_backend3_ownership_foreign_cliente(backend_available):
    secret = backend_available
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = TupagoPaymentClient(
        config, token_provider=LabJwtProvider(base64_secret=secret, sub=OWNER_PROD)
    )
    with client:
        # 'prodgcp' owns cliente id=1 but NOT id=2 (owned by 'testgcp'). A valid, authenticated
        # owner of one cliente must not be able to read the other: fail-closed ownership.
        with pytest.raises(TupagoNotFoundError):
            client.get_cliente(GetClienteRequest(id=2))


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


def test_backend3_catalogo_valid_jwt(backend_available):
    secret = backend_available
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=secret))
    with client:
        # Institution 301 is the synthetic institution used by the Java integration test.
        try:
            client.get_catalogo(GetCatalogoRequest(clave_institucion="301"))
        except TupagoNotFoundError:
            pass


def test_backend3_qr_status_not_found(backend_available):
    secret = backend_available
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=secret))
    with client:
        with pytest.raises(TupagoNotFoundError):
            client.get_qr_status(GetStatusRequest(idc="does-not-exist-xyz", id_cuenta=1))


def test_backend3_ownership_rejection(backend_available):
    secret = backend_available
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=secret))
    with client:
        # id=1 is owned by principal 'prodgcp' in the synthetic dataset; the SDK's lab principal
        # must NOT be able to read it. Auth succeeds (no UNAUTHENTICATED) but authorization
        # rejects it as NOT_FOUND / PERMISSION_DENIED — proving the backend enforces ownership.
        with pytest.raises((TupagoNotFoundError, TupagoPermissionDeniedError)):
            client.get_cliente(GetClienteRequest(id=1))


def test_backend3_expired_jwt(backend_available):
    secret = backend_available
    config = ClientConfig(endpoint=f"{BACKEND_HOST}:{BACKEND_PORT}")
    client = TupagoPaymentClient(
        config, token_provider=LabJwtProvider(base64_secret=secret, validity_seconds=-10)
    )
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
