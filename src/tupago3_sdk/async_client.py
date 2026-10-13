"""Asynchronous TUPAGO3 payment client (``grpc.aio``).

Mirrors :class:`tupago3_sdk.client.TupagoPaymentClient` with the same six RPCs, fail-closed
TLS, per-RPC bearer JWT and the financial create invariant. Read operations use a bounded
async retry for transient errors; create operations perform exactly one gRPC attempt.
"""

from __future__ import annotations

import asyncio

import grpc
import grpc.aio

from tupago3_sdk import auth, errors, models, security
from tupago3_sdk.client import (
    build_create_qr,
    build_create_telefonico,
    build_get_catalogo,
    build_get_cliente,
    build_get_status,
)
from tupago3_sdk.config import ClientConfig, validate_transport
from tupago3_sdk.generated import payment_pb2 as pb
from tupago3_sdk.generated import payment_pb2_grpc as grpc_pb

BearerProvider = auth.TokenProvider | auth.AsyncTokenProvider


def _aio_channel_options(config: ClientConfig) -> list[tuple[str, object]]:
    return [
        ("grpc.max_send_message_length", config.max_message_bytes),
        ("grpc.max_receive_message_length", config.max_message_bytes),
        ("grpc.enable_retries", 0),
    ]


class AsyncTupagoPaymentClient:
    """Asynchronous client for the TUPAGO3 ``PaymentService``."""

    def __init__(
        self,
        config: ClientConfig | None = None,
        *,
        token_provider: BearerProvider | None = None,
        max_read_attempts: int = 3,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        self._config = config or ClientConfig()
        validate_transport(self._config)
        if token_provider is None and not self._config.allow_unauthenticated:
            raise errors.TupagoUnauthenticatedError(
                "token_provider is required for production clients; enable "
                "ClientConfig.allow_unauthenticated only for isolated lab/test endpoints"
            )
        self._token_provider = token_provider
        self._max_read_attempts = max(1, max_read_attempts)
        self._loop = loop
        target = f"{self._config.host()}:{self._config.port()}"
        if not self._config.use_tls:
            # Plaintext permitted only after validate_transport confirmed loopback.
            self._channel = grpc.aio.insecure_channel(
                target, options=_aio_channel_options(self._config)
            )
        else:
            creds = security.build_channel_credentials(self._config)
            assert creds is not None
            # ``server_name`` is the *explicit* expected server identity. We only override when
            # the caller configures it; otherwise gRPC verifies against the target host (correct,
            # non-bypass behaviour for remote endpoints).
            server_name = self._config.tls.server_name
            secure_kwargs: dict[str, object] = {"options": _aio_channel_options(self._config)}
            if server_name:
                secure_kwargs["server_host_override"] = server_name
            self._channel = grpc.aio.secure_channel(target, creds, **secure_kwargs)
        self._own_channel = True
        self._stub = grpc_pb.PaymentServiceStub(self._channel)
        self._closed = False

    @classmethod
    def _from_channel(
        cls,
        channel: grpc.aio.Channel,
        *,
        config: ClientConfig | None = None,
        token_provider: BearerProvider | None = None,
        max_read_attempts: int = 3,
    ) -> AsyncTupagoPaymentClient:
        """Internal/test-only seam: wrap a pre-built async gRPC channel.

        Underscore-prefixed on purpose. The public constructor always builds the channel
        itself (fail-closed TLS, ``grpc.enable_retries=0``); this exists so tests can attach
        to an in-process gRPC servant without exposing an arbitrary channel-injection bypass
        in the production API.
        """
        self = cls.__new__(cls)
        self._config = config or ClientConfig()
        self._token_provider = token_provider
        self._max_read_attempts = max(1, max_read_attempts)
        self._loop = None
        self._channel = channel
        self._own_channel = False
        self._stub = grpc_pb.PaymentServiceStub(self._channel)
        self._closed = False
        return self

    # -- resource management ------------------------------------------------
    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._own_channel:
            try:
                await self._channel.close()
            except Exception:
                pass

    async def __aenter__(self) -> AsyncTupagoPaymentClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # -- internals ----------------------------------------------------------
    async def _token(self) -> str:
        if self._token_provider is None:
            return ""
        provider = self._token_provider
        if isinstance(provider, auth.AsyncTokenProvider):
            return await provider.aget_token()
        return provider.get_token()

    async def _metadata(self) -> list[tuple[str, str]]:
        token = await self._token()
        if token:
            return [("authorization", f"Bearer {token}")]
        return []

    def _timeout(self) -> float | None:
        return self._config.deadline_seconds or None

    async def _call_read(self, operation: str, method, request):
        if self._closed:
            raise errors.TupagoTransportError("Client is closed; cannot issue read RPCs")
        metadata = await self._metadata()
        timeout = self._timeout()
        last_exc: Exception | None = None
        for attempt in range(1, self._max_read_attempts + 1):
            try:
                return await method(request, metadata=metadata, timeout=timeout)
            except grpc.RpcError as exc:  # type: ignore[attr-defined]
                last_exc = exc
                if attempt >= self._max_read_attempts or not errors.should_retry_read(
                    exc, operation=operation
                ):
                    raise errors.from_grpc_error(exc, operation=operation) from exc
                server_delay = errors.server_retry_delay(exc)
                if server_delay is not None:
                    await asyncio.sleep(server_delay)
                else:
                    await asyncio.sleep(min(1.0, 0.1 * (2 ** (attempt - 1))))
        assert last_exc is not None
        raise errors.from_grpc_error(last_exc, operation=operation)

    async def _call_create(self, operation: str, method, request):
        if self._closed:
            raise errors.TupagoTransportError("Client is closed; cannot issue create RPCs")
        try:
            metadata = await self._metadata()
            return await method(request, metadata=metadata, timeout=self._timeout())
        except grpc.RpcError as exc:  # type: ignore[attr-defined]
            raise errors.from_grpc_error(
                exc,
                operation=operation,
                idempotency_key=getattr(request, "idempotency_key", None),
                unknown_outcome_on_create=True,
            ) from exc

    # -- RPC surface --------------------------------------------------------
    async def get_cliente(
        self, req: models.GetClienteRequest | pb.GetClienteRequest
    ) -> pb.ClientePerfil:
        request = req if isinstance(req, pb.GetClienteRequest) else build_get_cliente(req)
        return await self._call_read("GetCliente", self._stub.GetCliente, request)

    async def get_catalogo(
        self, req: models.GetCatalogoRequest | pb.GetCatalogoRequest
    ) -> pb.CatalogoInstitucion:
        request = req if isinstance(req, pb.GetCatalogoRequest) else build_get_catalogo(req)
        return await self._call_read("GetCatalogo", self._stub.GetCatalogo, request)

    async def get_qr_status(
        self, req: models.GetStatusRequest | pb.GetStatusRequest
    ) -> pb.JsonResponse:
        request = req if isinstance(req, pb.GetStatusRequest) else build_get_status(req)
        return await self._call_read("GetQrStatus", self._stub.GetQrStatus, request)

    async def get_telefonico_status(
        self, req: models.GetStatusRequest | pb.GetStatusRequest
    ) -> pb.JsonResponse:
        request = req if isinstance(req, pb.GetStatusRequest) else build_get_status(req)
        return await self._call_read("GetTelefonicoStatus", self._stub.GetTelefonicoStatus, request)

    async def create_qr(self, req: models.CreateQrRequest | pb.CreateQrRequest) -> pb.JsonResponse:
        request = req if isinstance(req, pb.CreateQrRequest) else build_create_qr(req)
        return await self._call_create("CreateQr", self._stub.CreateQr, request)

    async def create_telefonico(
        self, req: models.CreateTelefonicoRequest | pb.CreateTelefonicoRequest
    ) -> pb.JsonResponse:
        request = (
            req if isinstance(req, pb.CreateTelefonicoRequest) else build_create_telefonico(req)
        )
        return await self._call_create("CreateTelefonico", self._stub.CreateTelefonico, request)
