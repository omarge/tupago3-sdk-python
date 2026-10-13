"""Synchronous TUPAGO3 payment client.

Exposes the six ``PaymentService`` RPCs with an idiomatic Python API, bearer-JWT injected
per-RPC, fail-closed TLS, and the financial create invariant (at most one gRPC attempt, no
automatic retry). Read operations use a bounded retry for transient errors only. All wire I/O
is delegated to a :class:`~tupago3_sdk.transport.GrpcTransport`; the transport interface is
shared with future REST/GraphQL transports, so this public surface is stable.
"""

from __future__ import annotations

import grpc

from tupago3_sdk import auth, errors, models, retry, security
from tupago3_sdk.config import ClientConfig, validate_transport
from tupago3_sdk.generated import payment_pb2 as pb
from tupago3_sdk.transport import GrpcTransport, SyncPaymentTransport

BearerProvider = auth.TokenProvider


def build_get_cliente(req: models.GetClienteRequest) -> pb.GetClienteRequest:
    return pb.GetClienteRequest(id=int(req.id))


def build_get_catalogo(req: models.GetCatalogoRequest) -> pb.GetCatalogoRequest:
    return pb.GetCatalogoRequest(clave_institucion=req.clave_institucion)


def build_get_status(req: models.GetStatusRequest) -> pb.GetStatusRequest:
    return pb.GetStatusRequest(idc=req.idc, id_cuenta=int(req.id_cuenta))


def build_create_qr(req: models.CreateQrRequest) -> pb.CreateQrRequest:
    return pb.CreateQrRequest(
        id_cuenta=int(req.id_cuenta),
        monto=req.double_monto(),
        concepto=req.concepto,
        referencia_numerica=int(req.referencia_numerica),
        nombre_cliente=req.nombre_cliente,
        email=req.email,
        tipo_pago=int(req.tipo_pago),
        vigencia=int(req.vigencia),
        idempotency_key=req.idempotency_key,
    )


def build_create_telefonico(req: models.CreateTelefonicoRequest) -> pb.CreateTelefonicoRequest:
    return pb.CreateTelefonicoRequest(
        key=req.key,
        monto=req.double_monto(),
        concepto=req.concepto,
        referencia_numerica=int(req.referencia_numerica),
        numero_celular=req.numero_celular,
        vigencia=int(req.vigencia),
        idempotency_key=req.idempotency_key,
    )


class TupagoPaymentClient:
    """Synchronous client for the TUPAGO3 ``PaymentService``."""

    def __init__(
        self,
        config: ClientConfig | None = None,
        *,
        token_provider: BearerProvider | None = None,
        retry_policy: retry.RetryPolicy | None = None,
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
        self._retry_policy = retry_policy or retry.RetryPolicy()
        self._transport: SyncPaymentTransport
        if transport == "rest":
            from tupago3_sdk.rest import RestTransport

            self._transport = RestTransport(
                config=self._config, token_provider=token_provider, retry_policy=self._retry_policy
            )
            self._own_channel = True
        else:
            # SDK-authoritative gRPC channel (fail-closed TLS + grpc.enable_retries=0) wrapped by
            # a transport the public API cannot bypass.
            channel = security.create_channel(self._config)
            self._transport = GrpcTransport(
                channel,
                config=self._config,
                token_provider=token_provider,
                retry_policy=self._retry_policy,
            )
            self._own_channel = True
        self._closed = False

    @classmethod
    def _from_channel(
        cls,
        channel: grpc.Channel,
        *,
        config: ClientConfig | None = None,
        token_provider: BearerProvider | None = None,
        retry_policy: retry.RetryPolicy | None = None,
    ) -> TupagoPaymentClient:
        """Internal/test-only seam: wrap a pre-built gRPC channel.

        Underscore-prefixed on purpose. The public constructor always builds the channel
        itself (fail-closed TLS, ``grpc.enable_retries=0``); this exists so tests can attach
        to an in-process gRPC servant without exposing an arbitrary channel-injection bypass
        in the production API.
        """
        self = cls.__new__(cls)
        self._config = config or ClientConfig()
        self._token_provider = token_provider
        self._retry_policy = retry_policy or retry.RetryPolicy()
        self._transport = GrpcTransport(
            channel,
            config=self._config,
            token_provider=token_provider,
            retry_policy=self._retry_policy,
        )
        self._own_channel = False
        self._closed = False
        return self

    # -- resource management ------------------------------------------------
    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._own_channel:
            self._transport.close()

    def __enter__(self) -> TupagoPaymentClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- RPC surface --------------------------------------------------------
    def get_cliente(self, req: models.GetClienteRequest | pb.GetClienteRequest) -> pb.ClientePerfil:
        request = req if isinstance(req, pb.GetClienteRequest) else build_get_cliente(req)
        return self._transport.read("GetCliente", "GetCliente", request)

    def get_catalogo(
        self, req: models.GetCatalogoRequest | pb.GetCatalogoRequest
    ) -> pb.CatalogoInstitucion:
        request = req if isinstance(req, pb.GetCatalogoRequest) else build_get_catalogo(req)
        return self._transport.read("GetCatalogo", "GetCatalogo", request)

    def get_qr_status(self, req: models.GetStatusRequest | pb.GetStatusRequest) -> pb.JsonResponse:
        request = req if isinstance(req, pb.GetStatusRequest) else build_get_status(req)
        return self._transport.read("GetQrStatus", "GetQrStatus", request)

    def get_telefonico_status(
        self, req: models.GetStatusRequest | pb.GetStatusRequest
    ) -> pb.JsonResponse:
        request = req if isinstance(req, pb.GetStatusRequest) else build_get_status(req)
        return self._transport.read("GetTelefonicoStatus", "GetTelefonicoStatus", request)

    def create_qr(self, req: models.CreateQrRequest | pb.CreateQrRequest) -> pb.JsonResponse:
        request = req if isinstance(req, pb.CreateQrRequest) else build_create_qr(req)
        return self._transport.create(
            "CreateQr", "CreateQr", request, idempotency_key=request.idempotency_key
        )

    def create_telefonico(
        self, req: models.CreateTelefonicoRequest | pb.CreateTelefonicoRequest
    ) -> pb.JsonResponse:
        request = (
            req if isinstance(req, pb.CreateTelefonicoRequest) else build_create_telefonico(req)
        )
        return self._transport.create(
            "CreateTelefonico", "CreateTelefonico", request, idempotency_key=request.idempotency_key
        )
