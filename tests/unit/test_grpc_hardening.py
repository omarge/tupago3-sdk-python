"""A3 gRPC hardening adversarial + conformance tests.

Covers:
* P0-PY-GRPC-01: public constructors reject arbitrary channel injection; SDK-owned
  channels always disable gRPC auto-retries (grpc.enable_retries=0); remote plaintext
  is impossible through the public API.
* P0-PY-GRPC-02: loopback authority is based on explicit local identity, never on
  mutable DNS resolution of an arbitrary hostname.
* P1-PY-GRPC-03: create + ALREADY_EXISTS maps to TupagoAlreadyExistsError (not the
  ambiguous UnknownOutcome); other dispatch-ambiguous codes still fail closed.
* Auth fail-closed: missing token_provider raises at construction unless the explicit
  lab opt-in (ClientConfig.allow_unauthenticated) is set.
"""

from __future__ import annotations

import socket

import pytest
from grpc import StatusCode

from tupago3_sdk import (
    ClientConfig,
    InsecureTransportError,
    TupagoPaymentClient,
    TupagoUnauthenticatedError,
    errors,
    security,
)
from tupago3_sdk.async_client import AsyncTupagoPaymentClient, _aio_channel_options
from tupago3_sdk.config import _is_loopback_host


# --- P0-PY-GRPC-01: channel injection bypass removed -------------------------
def test_public_sync_constructor_rejects_channel_arg():
    with pytest.raises(TypeError):
        TupagoPaymentClient(ClientConfig(), channel=object())  # type: ignore[call-arg]


def test_public_async_constructor_rejects_channel_arg():
    with pytest.raises(TypeError):
        AsyncTupagoPaymentClient(ClientConfig(), channel=object())  # type: ignore[call-arg]


def test_sdk_owned_sync_channel_disables_retries():
    opts = security.channel_options(ClientConfig())
    assert ("grpc.enable_retries", 0) in opts


def test_sdk_owned_async_channel_disables_retries():
    opts = _aio_channel_options(ClientConfig())
    assert ("grpc.enable_retries", 0) in opts


def test_remote_plaintext_impossible_via_public_api():
    # A non-loopback plaintext target must be rejected by the fail-closed rule so a
    # caller can never reach a remote endpoint in cleartext through the public API.
    config = ClientConfig(endpoint="203.0.113.5:9090", use_tls=False)
    with pytest.raises(InsecureTransportError):
        TupagoPaymentClient(config)


# --- P0-PY-GRPC-02: loopback authority without DNS resolution ----------------
def test_loopback_policy_explicit_local_identity():
    assert _is_loopback_host("localhost")
    assert _is_loopback_host("localhost.localdomain")
    assert _is_loopback_host("127.0.0.1")
    assert _is_loopback_host("127.1.2.3")
    assert _is_loopback_host("::1")
    assert _is_loopback_host("[::1]")


def test_loopback_policy_rejects_remote_literal():
    assert not _is_loopback_host("8.8.8.8")
    assert not _is_loopback_host("2001:4860:4860::8888")


def test_loopback_policy_rejects_arbitrary_hostname_even_if_dns_spofs_loopback(monkeypatch):
    def fake_getaddrinfo(*_a, **_k):
        return [(socket.AF_INET, None, None, None, ("127.0.0.1", 0))]

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    # The implementation must NOT consult DNS for plaintext eligibility.
    assert not _is_loopback_host("evil.example.com")


def test_loopback_policy_rejects_unresolvable_hostname(monkeypatch):
    def fake_getaddrinfo(*_a, **_k):
        raise socket.gaierror("no such host")

    monkeypatch.setattr(socket, "getaddrinfo", fake_getaddrinfo)
    assert not _is_loopback_host("nope.invalid")


# --- P1-PY-GRPC-03: ALREADY_EXISTS is deterministic, not ambiguous -----------
class _FakeStatus:
    def __init__(self, code: StatusCode, details: str = "x") -> None:
        self._code = code
        self._details = details

    def code(self) -> StatusCode:
        return self._code

    def details(self) -> str:
        return self._details


def test_create_already_exists_maps_to_alreadyexists():
    err = errors.from_grpc_error(
        _FakeStatus(StatusCode.ALREADY_EXISTS),
        operation="CreateQr",
        idempotency_key="k",
        unknown_outcome_on_create=True,
    )
    assert isinstance(err, errors.TupagoAlreadyExistsError)
    assert not isinstance(err, errors.TupagoUnknownOutcomeError)


@pytest.mark.parametrize(
    "code",
    [
        StatusCode.UNAVAILABLE,
        StatusCode.DEADLINE_EXCEEDED,
        StatusCode.CANCELLED,
        StatusCode.ABORTED,
    ],
)
def test_create_dispatch_ambiguous_codes_map_to_unknown_outcome(code):
    err = errors.from_grpc_error(
        _FakeStatus(code),
        operation="CreateQr",
        idempotency_key="k",
        unknown_outcome_on_create=True,
    )
    assert isinstance(err, errors.TupagoUnknownOutcomeError)


# --- Auth fail-closed --------------------------------------------------------
def test_auth_fail_closed_requires_token_by_default():
    with pytest.raises(TupagoUnauthenticatedError):
        TupagoPaymentClient(ClientConfig(endpoint="127.0.0.1"))


def test_auth_fail_closed_async_requires_token_by_default():
    with pytest.raises(TupagoUnauthenticatedError):
        AsyncTupagoPaymentClient(ClientConfig(endpoint="127.0.0.1"))


def test_auth_lab_opt_in_allows_construction_without_token():
    cfg = ClientConfig(endpoint="127.0.0.1", allow_unauthenticated=True)
    client = TupagoPaymentClient(cfg)
    client.close()

    import asyncio

    async def _go() -> None:
        aclient = AsyncTupagoPaymentClient(cfg)
        await aclient.aclose()

    asyncio.run(_go())
