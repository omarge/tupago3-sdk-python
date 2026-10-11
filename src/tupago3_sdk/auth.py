"""Authentication: dynamic JWT token providers for the TUPAGO3 Python SDK.

Backend3 expects every RPC to carry ``Authorization: Bearer <jwt>`` metadata and validates
the JWT with HS512 against a Base64-encoded secret. The SDK never embeds a token
permanently on the channel; a :class:`TokenProvider` is consulted per-RPC so credentials can
rotate and expire without recreating the channel.

Two provider flavours exist: synchronous (for :class:`TupagoPaymentClient`) and
asynchronous (for :class:`AsyncTupagoPaymentClient`). The async providers expose an
``aget_token`` coroutine; the sync providers expose ``get_token``.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

__all__ = [
    "TokenProvider",
    "AsyncTokenProvider",
    "StaticTokenProvider",
    "StaticAsyncTokenProvider",
    "CallableTokenProvider",
    "CallableAsyncTokenProvider",
    "LabJwtProvider",
    "LabAsyncJwtProvider",
]


class TokenProvider:
    """Sync token provider protocol."""

    def get_token(self) -> str:
        raise NotImplementedError


class AsyncTokenProvider:
    """Async token provider protocol."""

    async def aget_token(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class StaticTokenProvider(TokenProvider):
    """Returns a fixed bearer token. Use only for static lab credentials."""

    token: str

    def get_token(self) -> str:
        return self.token


@dataclass(frozen=True)
class StaticAsyncTokenProvider(TokenProvider, AsyncTokenProvider):
    """Returns a fixed bearer token from both sync and async paths."""

    token: str

    def get_token(self) -> str:
        return self.token

    async def aget_token(self) -> str:
        return self.token


@dataclass(frozen=True)
class CallableTokenProvider(TokenProvider):
    """Wraps a callable that returns the current token (e.g. a vault lookup)."""

    fn: Callable[[], str]

    def get_token(self) -> str:
        return self.fn()


@dataclass(frozen=True)
class CallableAsyncTokenProvider(TokenProvider, AsyncTokenProvider):
    """Wraps an async callable / coroutine function that returns the current token."""

    fn: Callable[[], str | Awaitable[str]]

    def get_token(self) -> str:
        result = self.fn()
        if asyncio.iscoroutine(result):
            raise RuntimeError("CallableAsyncTokenProvider.fn is async; use aget_token")
        return result  # type: ignore[return-value]

    async def aget_token(self) -> str:
        result = self.fn()
        if asyncio.iscoroutine(result):  # type: ignore[arg-type]
            return await result  # type: ignore[no-any-return]
        return result  # type: ignore[return-value]


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _encode_hs512(secret: bytes, payload: bytes) -> str:
    return _b64url(hmac.new(secret, payload, hashlib.sha512).digest())


@dataclass
class LabJwtProvider(TokenProvider):
    """Builds an HS512 JWT on demand from a Base64-encoded lab secret.

    Mirrors backend3's ``SecurityJwtConfiguration``: the secret is Base64-decoded and used as
    the HMAC-SHA512 key. Claims ``sub``, ``userId``, ``auth`` (authorities list), ``iat`` and
    ``exp`` are emitted. The token is signed fresh on each call to honour rotation/expiry.
    """

    base64_secret: str
    sub: str = "tupago-sdk"
    user_id: str = "1"
    authorities: tuple[str, ...] = ("ROLE_USER",)
    validity_seconds: int = 86400

    def _secret(self) -> bytes:
        try:
            return base64.b64decode(self.base64_secret, validate=True)
        except binascii.Error as exc:  # pragma: no cover - defensive
            raise ValueError("base64_secret is not valid Base64") from exc

    def _build(self, now: int | None = None) -> str:
        now = int(now if now is not None else time.time())
        header = {"alg": "HS512", "typ": "JWT"}
        claims = {
            "sub": self.sub,
            "userId": self.user_id,
            "auth": list(self.authorities),
            "iat": now,
            "exp": now + self.validity_seconds,
        }
        signing_input = (
            _b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
            + "."
            + _b64url(json.dumps(claims, separators=(",", ":")).encode("utf-8"))
        ).encode("ascii")
        signature = _encode_hs512(self._secret(), signing_input)
        return signing_input.decode("ascii") + "." + signature

    def get_token(self) -> str:
        return self._build()


@dataclass
class LabAsyncJwtProvider(LabJwtProvider, AsyncTokenProvider):
    """Async variant of :class:`LabJwtProvider`."""

    async def aget_token(self) -> str:
        return self._build()
