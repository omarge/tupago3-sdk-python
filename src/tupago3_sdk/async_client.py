"""Asynchronous TUPAGO3 payment client (``grpc.aio``).

Mirrors :class:`tupago3_sdk.client.TupagoPaymentClient` with the same six RPCs, fail-closed
TLS, per-RPC bearer JWT and the financial create invariant. Read operations use a bounded
async retry for transient errors; create operations perform exactly one gRPC attempt. All wire
I/O is delegated to :class:`~tupago3_sdk.transport.AsyncGrpcTransport`.
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
from tupago3_sdk.transport import AsyncGrpcTransport, AsyncPaymentTransport

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
        transport: str = "grpc",
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
        self._transport: AsyncPaymentTransport
        if transport == "rest":
            from tupago3_sdk.rest import AsyncRestTransport

            self._transport = AsyncRestTransport(
                config=self._config,
                token_provider=token_provider,
                max_read_attempts=self._max_read_attempts,
            )
            self._own_channel = True
        elif transport == "graphql":
            from tupago3_sdk.graphql import AsyncGraphQLTransport

            self._transport = AsyncGraphQLTransport(
                config=self._config,
                token_provider=token_provider,
                max_read_attempts=self._max_read_attempts,
            )
            self._own_channel = True
        else:
            target = f"{self._config.host()}:{self._config.port()}"
            if not self._config.use_tls:
                # Plaintext permitted only after validate_transport confirmed loopback.
                channel = grpc.aio.insecure_channel(
                    target, options=_aio_channel_options(self._config)
                )
            else:
                creds = security.build_channel_credentials(self._config)
                assert creds is not None
                server_name = self._config.tls.server_name
                secure_kwargs: dict[str, object] = {"options": _aio_channel_options(self._config)}
                if server_name:
                    secure_kwargs["server_host_override"] = server_name
                channel = grpc.aio.secure_channel(target, creds, **secure_kwargs)
            self._transport = AsyncGrpcTransport(
                channel,
                config=self._config,
                token_provider=token_provider,
                max_read_attempts=self._max_read_attempts,
            )
            self._own_channel = True
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
        self._transport = AsyncGrpcTransport(
            channel,
            config=self._config,
            token_provider=token_provider,
            max_read_attempts=self._max_read_attempts,
        )
        self._own_channel = False
        self._closed = False
        return self

    # -- resource management ------------------------------------------------
    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._own_channel:
            await self._transport.aclose()

    async def __aenter__(self) -> AsyncTupagoPaymentClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # -- RPC surface --------------------------------------------------------
    async def get_cliente(
        self, req: models.GetClienteRequest | pb.GetClienteRequest
    ) -> pb.ClientePerfil:
        request = req if isinstance(req, pb.GetClienteRequest) else build_get_cliente(req)
        return await self._transport.read_async("GetCliente", "GetCliente", request)

    async def get_catalogo(
        self, req: models.GetCatalogoRequest | pb.GetCatalogoRequest
    ) -> pb.CatalogoInstitucion:
        request = req if isinstance(req, pb.GetCatalogoRequest) else build_get_catalogo(req)
        return await self._transport.read_async("GetCatalogo", "GetCatalogo", request)

    async def get_qr_status(
        self, req: models.GetStatusRequest | pb.GetStatusRequest
    ) -> pb.JsonResponse:
        request = req if isinstance(req, pb.GetStatusRequest) else build_get_status(req)
        return await self._transport.read_async("GetQrStatus", "GetQrStatus", request)

    async def get_telefonico_status(
        self, req: models.GetStatusRequest | pb.GetStatusRequest
    ) -> pb.JsonResponse:
        request = req if isinstance(req, pb.GetStatusRequest) else build_get_status(req)
        return await self._transport.read_async(
            "GetTelefonicoStatus", "GetTelefonicoStatus", request
        )

    async def create_qr(self, req: models.CreateQrRequest | pb.CreateQrRequest) -> pb.JsonResponse:
        request = req if isinstance(req, pb.CreateQrRequest) else build_create_qr(req)
        return await self._transport.create_async(
            "CreateQr", "CreateQr", request, idempotency_key=request.idempotency_key
        )

    async def create_telefonico(
        self, req: models.CreateTelefonicoRequest | pb.CreateTelefonicoRequest
    ) -> pb.JsonResponse:
        request = (
            req if isinstance(req, pb.CreateTelefonicoRequest) else build_create_telefonico(req)
        )
        return await self._transport.create_async(
            "CreateTelefonico", "CreateTelefonico", request, idempotency_key=request.idempotency_key
        )
