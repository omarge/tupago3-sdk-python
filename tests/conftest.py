"""Shared pytest fixtures for the TUPAGO3 SDK test-suite."""

from __future__ import annotations

import base64
import os

import pytest

from tests.grpc_test_server import start_server
from tests.tls_certs import generate_bundle
from tupago3_sdk import ClientConfig, LabJwtProvider, TlsConfig


@pytest.fixture
def lab_secret() -> str:
    """A random LAB-ONLY base64 secret (never a production credential)."""
    return base64.b64encode(os.urandom(64)).decode("ascii")


@pytest.fixture
def plaintext_server(lab_secret):
    server, port, servicer = start_server(lab_secret, host="127.0.0.1", port=0)
    yield port, servicer
    server.stop(grace=0.2)


@pytest.fixture(params=["trusted", "untrusted", "expired", "mismatch"])
def tls_scenario(request, tmp_path, lab_secret):
    kind = request.param
    bundle = generate_bundle(str(tmp_path), kind)
    with open(bundle.server_cert_path, "rb") as fh:
        cert = fh.read()
    with open(bundle.server_key_path, "rb") as fh:
        key = fh.read()
    server, port, servicer = start_server(
        lab_secret, host="127.0.0.1", port=0, tls_credentials=(cert, key)
    )
    yield {"kind": kind, "port": port, "bundle": bundle, "servicer": servicer}
    server.stop(grace=0.2)


def sync_client_for(
    port: int, lab_secret: str, *, use_tls: bool = False, ca_cert_path: str | None = None
):
    config = ClientConfig(
        endpoint=f"127.0.0.1:{port}",
        use_tls=use_tls,
        tls=TlsConfig(ca_cert_path=ca_cert_path, server_name="127.0.0.1")
        if use_tls
        else TlsConfig(),
    )
    return __import__("tupago3_sdk").TupagoPaymentClient(
        config, token_provider=LabJwtProvider(base64_secret=lab_secret)
    )
