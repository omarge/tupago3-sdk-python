"""Synchronous TUPAGO3 payment client.

Exposes the six ``PaymentService`` RPCs with an idiomatic Python API, bearer-JWT injected
per-RPC, fail-closed TLS, and the financial create invariant (at most one gRPC attempt, no
automatic retry). Read operations use a bounded retry for transient errors only.
"""

from __future__ import annotations

import grpc

from tupago3_sdk import auth, errors, models, retry, security
from tupago3_sdk.config import ClientConfig, validate_transport
from tupago3_sdk.generated import payment_pb2 as pb
from tupago3_sdk.generated import payment_pb2_grpc as grpc_pb

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
        # SDK-authoritative channel: fail-closed TLS + grpc.enable_retries=0. A caller
        # cannot inject an arbitrary channel through the public constructor.
        self._channel = security.create_channel(self._config)
        self._own_channel = True
        self._stub = grpc_pb.PaymentServiceStub(self._channel)
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
        self._channel = channel
        self._own_channel = False
        self._stub = grpc_pb.PaymentServiceStub(self._channel)
        self._closed = False
        return self

    # -- resource management ------------------------------------------------
    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._own_channel:
            try:
                self._channel.close()
            except Exception:
                pass

    def __enter__(self) -> TupagoPaymentClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- internals ----------------------------------------------------------
    def _metadata(self) -> list[tuple[str, str]]:
        metadata: list[tuple[str, str]] = []
        if self._token_provider is not None:
            token = self._token_provider.get_token()
            if token:
                metadata.append(("authorization", f"Bearer {token}"))
        return metadata

    def _timeout(self) -> float | None:
        return self._config.deadline_seconds or None

    def _call_read(self, operation: str, method, request, response_type):
        if self._closed:
            raise errors.TupagoTransportError("Client is closed; cannot issue read RPCs")

        def _invoke() -> object:
            return method(request, metadata=self._metadata(), timeout=self._timeout())

        return retry.retry_read_sync(self._retry_policy, operation, _invoke)

    def _call_create(self, operation: str, method, request):
        if self._closed:
            raise errors.TupagoTransportError("Client is closed; cannot issue create RPCs")
        try:
            return method(request, metadata=self._metadata(), timeout=self._timeout())
        except grpc.RpcError as exc:  # type: ignore[attr-defined]
            raise errors.from_grpc_error(
                exc,
                operation=operation,
                idempotency_key=getattr(request, "idempotency_key", None),
                unknown_outcome_on_create=True,
            ) from exc

    # -- RPC surface --------------------------------------------------------
    def get_cliente(self, req: models.GetClienteRequest | pb.GetClienteRequest) -> pb.ClientePerfil:
        request = req if isinstance(req, pb.GetClienteRequest) else build_get_cliente(req)
        return self._call_read("GetCliente", self._stub.GetCliente, request, pb.ClientePerfil)

    def get_catalogo(
        self, req: models.GetCatalogoRequest | pb.GetCatalogoRequest
    ) -> pb.CatalogoInstitucion:
        request = req if isinstance(req, pb.GetCatalogoRequest) else build_get_catalogo(req)
        return self._call_read(
            "GetCatalogo", self._stub.GetCatalogo, request, pb.CatalogoInstitucion
        )

    def get_qr_status(self, req: models.GetStatusRequest | pb.GetStatusRequest) -> pb.JsonResponse:
        request = req if isinstance(req, pb.GetStatusRequest) else build_get_status(req)
        return self._call_read("GetQrStatus", self._stub.GetQrStatus, request, pb.JsonResponse)

    def get_telefonico_status(
        self, req: models.GetStatusRequest | pb.GetStatusRequest
    ) -> pb.JsonResponse:
        request = req if isinstance(req, pb.GetStatusRequest) else build_get_status(req)
        return self._call_read(
            "GetTelefonicoStatus", self._stub.GetTelefonicoStatus, request, pb.JsonResponse
        )

    def create_qr(self, req: models.CreateQrRequest | pb.CreateQrRequest) -> pb.JsonResponse:
        request = req if isinstance(req, pb.CreateQrRequest) else build_create_qr(req)
        return self._call_create("CreateQr", self._stub.CreateQr, request)

    def create_telefonico(
        self, req: models.CreateTelefonicoRequest | pb.CreateTelefonicoRequest
    ) -> pb.JsonResponse:
        request = (
            req if isinstance(req, pb.CreateTelefonicoRequest) else build_create_telefonico(req)
        )
        return self._call_create("CreateTelefonico", self._stub.CreateTelefonico, request)
