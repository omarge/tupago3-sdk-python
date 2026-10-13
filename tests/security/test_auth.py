"""T05-T08: Valid JWT, missing JWT, invalid JWT, token rotation."""

from __future__ import annotations

import pytest

from tupago3_sdk import (
    ClientConfig,
    LabJwtProvider,
    StaticTokenProvider,
    TlsConfig,
    TupagoPaymentClient,
    TupagoUnauthenticatedError,
)


def _client(port, lab_secret, token_provider=None, use_tls=False, ca=None, allow_unauth=False):
    if use_tls:
        config = ClientConfig(
            endpoint=f"127.0.0.1:{port}",
            use_tls=True,
            tls=TlsConfig(ca_cert_path=ca, server_name="127.0.0.1"),
            allow_unauthenticated=allow_unauth,
        )
    else:
        config = ClientConfig(endpoint=f"127.0.0.1:{port}", allow_unauthenticated=allow_unauth)
    return TupagoPaymentClient(config, token_provider=token_provider)


def test_T05_valid_jwt(plaintext_server, lab_secret):
    port, _ = plaintext_server
    client = _client(port, lab_secret, token_provider=LabJwtProvider(base64_secret=lab_secret))
    with client:
        perfil = client.get_cliente(__import__("tupago3_sdk").models.GetClienteRequest(id=1))
        assert perfil.cliente.nombre == "Cliente Demo"


def test_T06_missing_jwt(plaintext_server, lab_secret):
    port, _ = plaintext_server
    client = _client(port, lab_secret, token_provider=None, allow_unauth=True)
    with client:
        with pytest.raises(TupagoUnauthenticatedError):
            client.get_cliente(__import__("tupago3_sdk").models.GetClienteRequest(id=1))


def test_T07_invalid_jwt(plaintext_server, lab_secret):
    port, _ = plaintext_server
    client = _client(port, lab_secret, token_provider=StaticTokenProvider("not-a-jwt"))
    with client:
        with pytest.raises(TupagoUnauthenticatedError):
            client.get_cliente(__import__("tupago3_sdk").models.GetClienteRequest(id=1))


def test_T07b_expired_jwt(plaintext_server, lab_secret):
    port, _ = plaintext_server
    expired = LabJwtProvider(base64_secret=lab_secret, validity_seconds=-10)
    client = _client(port, lab_secret, token_provider=expired)
    with client:
        with pytest.raises(TupagoUnauthenticatedError):
            client.get_cliente(__import__("tupago3_sdk").models.GetClienteRequest(id=1))


def test_T08_token_rotation(plaintext_server, lab_secret):
    port, _ = plaintext_server
    # Dynamic providers are not constant-cached: different claims yield different tokens.
    pa = LabJwtProvider(base64_secret=lab_secret, sub="alice")
    pb_ = LabJwtProvider(base64_secret=lab_secret, sub="bob")
    assert pa.get_token() != pb_.get_token()
    # A freshly generated token validates against the server (rotation honoured).
    provider = LabJwtProvider(base64_secret=lab_secret)
    client = _client(port, lab_secret, token_provider=provider)
    with client:
        assert (
            client.get_cliente(
                __import__("tupago3_sdk").models.GetClienteRequest(id=1)
            ).cliente.nombre
            == "Cliente Demo"
        )
    # Decode and confirm HS512 alg claim.
    import base64
    import json

    header = provider.get_token().split(".")[0]
    claims = json.loads(base64.urlsafe_b64decode(header + "==="))
    assert claims["alg"] == "HS512"
