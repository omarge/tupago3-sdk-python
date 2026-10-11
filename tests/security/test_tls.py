"""T09-T11: Remote TLS required, untrusted CA rejected, server-name mismatch rejected."""

from __future__ import annotations

import pytest

from tupago3_sdk import (
    ClientConfig,
    InsecureTransportError,
    LabJwtProvider,
    TlsConfig,
    TupagoPaymentClient,
)


def _tls_client(port, ca_cert_path, server_name="127.0.0.1", lab_secret="x" * 88):
    config = ClientConfig(
        endpoint=f"127.0.0.1:{port}",
        use_tls=True,
        tls=TlsConfig(ca_cert_path=ca_cert_path, server_name=server_name),
    )
    return TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=lab_secret))


def test_T09_remote_tls_required():
    # A non-loopback endpoint with plaintext must be rejected by the fail-closed rule.
    config = ClientConfig(endpoint="203.0.113.5:9090", use_tls=False)
    with pytest.raises(InsecureTransportError):
        TupagoPaymentClient(config)
    # Loopback plaintext is allowed (fail-open only on loopback).
    loop = ClientConfig(endpoint="127.0.0.1:9090", use_tls=False)
    TupagoPaymentClient(loop)  # construction OK


def test_T09b_tls_verification_cannot_be_disabled():
    # Public API must never allow disabling certificate verification for any endpoint.
    config = ClientConfig(
        endpoint="203.0.113.5:9090",
        use_tls=True,
        tls=TlsConfig(verify=False, ca_cert_path="/tmp/nonexistent.pem"),
    )
    with pytest.raises(InsecureTransportError):
        TupagoPaymentClient(config)


def test_T10_untrusted_ca_rejected(tls_scenario, lab_secret):
    if tls_scenario["kind"] != "untrusted":
        pytest.skip("covered by parametrization")
    port = tls_scenario["port"]
    bundle = tls_scenario["bundle"]
    client = _tls_client(port, bundle.ca_cert_path, lab_secret=lab_secret)
    with client:
        with pytest.raises(Exception):
            client.get_cliente(__import__("tupago3_sdk").models.GetClienteRequest(id=1))


def test_T11_server_name_mismatch_rejected(tls_scenario, lab_secret):
    if tls_scenario["kind"] != "mismatch":
        pytest.skip("covered by parametrization")
    port = tls_scenario["port"]
    bundle = tls_scenario["bundle"]
    # Connect with a CA that trusts the cert, but the SAN does not cover 127.0.0.1.
    client = _tls_client(port, bundle.ca_cert_path, server_name="127.0.0.1", lab_secret=lab_secret)
    with client:
        with pytest.raises(Exception):
            client.get_cliente(__import__("tupago3_sdk").models.GetClienteRequest(id=1))


def test_T11b_trusted_ca_accepted(tls_scenario, lab_secret):
    if tls_scenario["kind"] != "trusted":
        pytest.skip("covered by parametrization")
    port = tls_scenario["port"]
    bundle = tls_scenario["bundle"]
    client = _tls_client(port, bundle.ca_cert_path, server_name="127.0.0.1", lab_secret=lab_secret)
    with client:
        perfil = client.get_cliente(__import__("tupago3_sdk").models.GetClienteRequest(id=1))
        assert perfil.cliente.nombre == "Cliente Demo"


def test_T11c_expired_cert_rejected(tls_scenario, lab_secret):
    if tls_scenario["kind"] != "expired":
        pytest.skip("covered by parametrization")
    port = tls_scenario["port"]
    bundle = tls_scenario["bundle"]
    client = _tls_client(port, bundle.ca_cert_path, server_name="127.0.0.1", lab_secret=lab_secret)
    with client:
        with pytest.raises(Exception):
            client.get_cliente(__import__("tupago3_sdk").models.GetClienteRequest(id=1))
