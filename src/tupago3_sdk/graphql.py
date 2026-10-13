"""GraphQL transport for the TUPAGO3 payment RPCs (phase C).

Implements :class:`~tupago3_sdk.transport.SyncPaymentTransport` /
:class:`~tupago3_sdk.transport.AsyncPaymentTransport` over the backend3 GraphQL endpoint
(``/graphql``), discovered from ``src/main/resources/graphql/tupago.graphqls``. All six
capabilities are expressed as GraphQL operations:

  C1 CreateQr          -> mutation createPagoQr(input, idempotencyKey)
  C2 CreateTelefonico  -> mutation createPagoTelefonico(input, idempotencyKey)
  C3 GetQrStatus       -> query pagoQrEstatusUsuario(idc: String!): JSON
  C4 GetTelefonicoStatus -> query pagoTelefonicoEstatusUsuario(idc: String!): JSON
  C5 GetCliente        -> query cliente(id: ID!): ClientePerfil
  C6 GetCatalogo       -> query catalogoInstitucion(claveInstitucion: String!): CatalogoInstitucion

Identifiers are sent as GraphQL ``ID`` (string form) to preserve int64 exactness. Responses
are converted to the SAME protobuf types the gRPC/REST paths return, so the public client
surface is unchanged. Financial-safety rules mirror the REST transport.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from tupago3_sdk import auth, errors, retry
from tupago3_sdk.config import ClientConfig, validate_transport
from tupago3_sdk.generated import payment_pb2 as pb
from tupago3_sdk.rest import _json_to_catalogo, _json_to_cliente_perfil

_GRAPHQL_PATH = "/graphql"


def _map_graphql_error(err: dict) -> errors.TupagoError:
    extensions = err.get("extensions") or {}
    code = (extensions.get("code") or "").upper()
    message = err.get("message", "graphql error")
    if code == "ALREADY_EXISTS":
        return errors.TupagoAlreadyExistsError(message, grpc_code="ALREADY_EXISTS")
    if "PENDING_RECONCILIATION" in code or "PENDING_RECONCILIATION" in message.upper():
        return errors.TupagoPendingReconciliationError(message, grpc_code="ABORTED")
    if code == "UNAUTHENTICATED":
        return errors.TupagoUnauthenticatedError(message, grpc_code="UNAUTHENTICATED")
    if code == "PERMISSION_DENIED":
        return errors.TupagoPermissionDeniedError(message, grpc_code="PERMISSION_DENIED")
    if code == "NOT_FOUND":
        return errors.TupagoNotFoundError(message, grpc_code="NOT_FOUND")
    if code == "INVALID_ARGUMENT":
        return errors.TupagoInvalidArgumentError(message, grpc_code="INVALID_ARGUMENT")
    if code in ("UNAVAILABLE", "RESOURCE_EXHAUSTED"):
        return errors.TupagoUnavailableError(message, grpc_code=code)
    return errors.TupagoInternalError(message, grpc_code="INTERNAL")


class GraphQLTransport:
    """Synchronous GraphQL transport (SDK-authoritative httpx client)."""

    def __init__(self, *, config: ClientConfig, token_provider, retry_policy=None) -> None:
        validate_transport(config)
        self._config = config
        self._token_provider = token_provider
        self._retry_policy = retry_policy or retry.RetryPolicy()
        scheme = "https" if config.use_tls else "http"
        verify: Any = True
        if config.use_tls and config.tls.ca_cert_path:
            verify = config.tls.ca_cert_path
        self._client = httpx.Client(
            base_url=f"{scheme}://{config.endpoint}{_GRAPHQL_PATH}",
            verify=verify,
            follow_redirects=False,
            timeout=config.deadline_seconds or 30.0,
        )

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self._token_provider is not None:
            token = self._token_provider.get_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
        return headers

    def _execute(
        self,
        query: str,
        variables: dict,
        *,
        idempotency_key=None,
        is_create: bool = False,
        max_attempts: int = 1,
    ) -> dict:
        payload = {"query": query, "variables": variables}
        attempts = max(1, max_attempts)
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                resp = self._client.post("", headers=self._headers(), json=payload)
            except httpx.TransportError as exc:
                # Network failure after handing a FINANCIAL create to the transport: fail
                # closed as UnknownOutcome (a false "definitely not sent" is forbidden).
                if is_create:
                    raise errors.TupagoUnknownOutcomeError(
                        f"GraphQL create transport error after dispatch: {exc}",
                        grpc_code="UNKNOWN",
                        idempotency_key=idempotency_key,
                    ) from exc
                last_exc = exc
                if attempt >= attempts:
                    raise
                continue
            if not (200 <= resp.status_code < 300):
                if resp.status_code == 401:
                    raise errors.TupagoUnauthenticatedError(
                        resp.text or "unauthenticated", grpc_code="UNAUTHENTICATED"
                    )
                if resp.status_code == 403:
                    raise errors.TupagoPermissionDeniedError(
                        resp.text or "forbidden", grpc_code="PERMISSION_DENIED"
                    )
                raise errors.TupagoInternalError(
                    f"http {resp.status_code}", grpc_code="INTERNAL", operation=query[:32]
                )
            body = resp.json()
            if body.get("errors"):
                # Partial data + errors fails closed (prefer the first typed error).
                raise _map_graphql_error(body["errors"][0])
            return body.get("data", {})
        assert last_exc is not None
        raise last_exc

    # -- RPC dispatch -------------------------------------------------------
    def read(self, operation: str, method_name: str, request: object):
        max_attempts = getattr(self._retry_policy, "max_attempts", 1)
        if operation == "GetCliente":
            assert isinstance(request, pb.GetClienteRequest)
            q = (
                "query($id: ID!){ cliente(id:$id){ cliente{id nombre claveInstitucion key} "
                "catalogoInstitucion{id claveInstitucion nombreCorto} } }"
            )
            data = self._execute(q, {"id": str(request.id)}, max_attempts=max_attempts)
            return _json_to_cliente_perfil(json.dumps(data["cliente"]))
        if operation == "GetCatalogo":
            assert isinstance(request, pb.GetCatalogoRequest)
            q = (
                "query($c: String!){ catalogoInstitucion(claveInstitucion:$c){ "
                "id claveInstitucion nombreCorto } }"
            )
            data = self._execute(q, {"c": request.clave_institucion}, max_attempts=max_attempts)
            return _json_to_catalogo(json.dumps(data["catalogoInstitucion"]))
        if operation == "GetQrStatus":
            assert isinstance(request, pb.GetStatusRequest)
            q = "query($idc: String!){ pagoQrEstatusUsuario(idc:$idc) }"
            data = self._execute(q, {"idc": request.idc}, max_attempts=max_attempts)
            return pb.JsonResponse(json=json.dumps(data["pagoQrEstatusUsuario"]))
        if operation == "GetTelefonicoStatus":
            assert isinstance(request, pb.GetStatusRequest)
            q = "query($idc: String!){ pagoTelefonicoEstatusUsuario(idc:$idc) }"
            data = self._execute(q, {"idc": request.idc}, max_attempts=max_attempts)
            return pb.JsonResponse(json=json.dumps(data["pagoTelefonicoEstatusUsuario"]))
        raise errors.TupagoError(f"unsupported GraphQL read: {operation}")

    def create(self, operation: str, method_name: str, request: object, *, idempotency_key=None):
        if operation == "CreateQr":
            assert isinstance(request, pb.CreateQrRequest)
            inp = {
                "idCuenta": str(request.id_cuenta),
                "monto": float(request.monto),
                "concepto": request.concepto,
                "referenciaNumerica": int(request.referencia_numerica),
                "nombreCliente": request.nombre_cliente,
                "email": request.email,
                "tipoPago": int(request.tipo_pago),
                "vigencia": int(request.vigencia),
            }
            q = (
                "mutation($input: PagoQrInput!, $key: String){ "
                "createPagoQr(input:$input, idempotencyKey:$key) }"
            )
            data = self._execute(
                q,
                {"input": inp, "key": idempotency_key},
                idempotency_key=idempotency_key,
                is_create=True,
            )
            return pb.JsonResponse(json=json.dumps(data["createPagoQr"]))
        if operation == "CreateTelefonico":
            assert isinstance(request, pb.CreateTelefonicoRequest)
            inp = {
                "key": request.key,
                "monto": float(request.monto),
                "concepto": request.concepto,
                "referenciaNumerica": int(request.referencia_numerica),
                "numeroCelular": request.numero_celular,
                "vigencia": int(request.vigencia),
            }
            q = (
                "mutation($input: PagoTelefonicoInput!, $key: String){ "
                "createPagoTelefonico(input:$input, idempotencyKey:$key) }"
            )
            data = self._execute(
                q,
                {"input": inp, "key": idempotency_key},
                idempotency_key=idempotency_key,
                is_create=True,
            )
            return pb.JsonResponse(json=json.dumps(data["createPagoTelefonico"]))
        raise errors.TupagoError(f"unsupported GraphQL create: {operation}")

    def close(self) -> None:
        self._client.close()


class AsyncGraphQLTransport:
    """Asynchronous GraphQL transport (SDK-authoritative httpx async client)."""

    def __init__(self, *, config: ClientConfig, token_provider, max_read_attempts: int = 3) -> None:
        validate_transport(config)
        self._config = config
        self._token_provider = token_provider
        self._max_read_attempts = max(1, max_read_attempts)
        scheme = "https" if config.use_tls else "http"
        verify: Any = True
        if config.use_tls and config.tls.ca_cert_path:
            verify = config.tls.ca_cert_path
        self._client = httpx.AsyncClient(
            base_url=f"{scheme}://{config.endpoint}{_GRAPHQL_PATH}",
            verify=verify,
            follow_redirects=False,
            timeout=config.deadline_seconds or 30.0,
        )

    async def _token(self) -> str:
        if self._token_provider is None:
            return ""
        provider = self._token_provider
        if isinstance(provider, auth.AsyncTokenProvider):
            return await provider.aget_token()
        return provider.get_token()

    async def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        token = await self._token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def _execute(
        self,
        query: str,
        variables: dict,
        *,
        idempotency_key=None,
        is_create: bool = False,
        max_attempts: int = 1,
    ) -> dict:
        payload = {"query": query, "variables": variables}
        headers = await self._headers()
        attempts = max(1, max_attempts)
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                resp = await self._client.post("", headers=headers, json=payload)
            except httpx.TransportError as exc:
                if is_create:
                    raise errors.TupagoUnknownOutcomeError(
                        f"GraphQL create transport error after dispatch: {exc}",
                        grpc_code="UNKNOWN",
                        idempotency_key=idempotency_key,
                    ) from exc
                last_exc = exc
                if attempt >= attempts:
                    raise
                continue
            if not (200 <= resp.status_code < 300):
                if resp.status_code == 401:
                    raise errors.TupagoUnauthenticatedError(
                        resp.text or "unauthenticated", grpc_code="UNAUTHENTICATED"
                    )
                if resp.status_code == 403:
                    raise errors.TupagoPermissionDeniedError(
                        resp.text or "forbidden", grpc_code="PERMISSION_DENIED"
                    )
                raise errors.TupagoInternalError(
                    f"http {resp.status_code}", grpc_code="INTERNAL", operation=query[:32]
                )
            body = resp.json()
            if body.get("errors"):
                raise _map_graphql_error(body["errors"][0])
            return body.get("data", {})
        assert last_exc is not None
        raise last_exc

    async def read_async(self, operation: str, method_name: str, request: object):
        if operation == "GetCliente":
            assert isinstance(request, pb.GetClienteRequest)
            q = (
                "query($id: ID!){ cliente(id:$id){ cliente{id nombre claveInstitucion key} "
                "catalogoInstitucion{id claveInstitucion nombreCorto} } }"
            )
            data = await self._execute(
                q, {"id": str(request.id)}, max_attempts=self._max_read_attempts
            )
            return _json_to_cliente_perfil(json.dumps(data["cliente"]))
        if operation == "GetCatalogo":
            assert isinstance(request, pb.GetCatalogoRequest)
            q = (
                "query($c: String!){ catalogoInstitucion(claveInstitucion:$c){ "
                "id claveInstitucion nombreCorto } }"
            )
            data = await self._execute(
                q, {"c": request.clave_institucion}, max_attempts=self._max_read_attempts
            )
            return _json_to_catalogo(json.dumps(data["catalogoInstitucion"]))
        if operation == "GetQrStatus":
            assert isinstance(request, pb.GetStatusRequest)
            q = "query($idc: String!){ pagoQrEstatusUsuario(idc:$idc) }"
            data = await self._execute(
                q, {"idc": request.idc}, max_attempts=self._max_read_attempts
            )
            return pb.JsonResponse(json=json.dumps(data["pagoQrEstatusUsuario"]))
        if operation == "GetTelefonicoStatus":
            assert isinstance(request, pb.GetStatusRequest)
            q = "query($idc: String!){ pagoTelefonicoEstatusUsuario(idc:$idc) }"
            data = await self._execute(
                q, {"idc": request.idc}, max_attempts=self._max_read_attempts
            )
            return pb.JsonResponse(json=json.dumps(data["pagoTelefonicoEstatusUsuario"]))
        raise errors.TupagoError(f"unsupported GraphQL read: {operation}")

    async def create_async(
        self, operation: str, method_name: str, request: object, *, idempotency_key=None
    ):
        if operation == "CreateQr":
            assert isinstance(request, pb.CreateQrRequest)
            inp = {
                "idCuenta": str(request.id_cuenta),
                "monto": float(request.monto),
                "concepto": request.concepto,
                "referenciaNumerica": int(request.referencia_numerica),
                "nombreCliente": request.nombre_cliente,
                "email": request.email,
                "tipoPago": int(request.tipo_pago),
                "vigencia": int(request.vigencia),
            }
            q = (
                "mutation($input: PagoQrInput!, $key: String){ "
                "createPagoQr(input:$input, idempotencyKey:$key) }"
            )
            data = await self._execute(
                q,
                {"input": inp, "key": idempotency_key},
                idempotency_key=idempotency_key,
                is_create=True,
            )
            return pb.JsonResponse(json=json.dumps(data["createPagoQr"]))
        if operation == "CreateTelefonico":
            assert isinstance(request, pb.CreateTelefonicoRequest)
            inp = {
                "key": request.key,
                "monto": float(request.monto),
                "concepto": request.concepto,
                "referenciaNumerica": int(request.referencia_numerica),
                "numeroCelular": request.numero_celular,
                "vigencia": int(request.vigencia),
            }
            q = (
                "mutation($input: PagoTelefonicoInput!, $key: String){ "
                "createPagoTelefonico(input:$input, idempotencyKey:$key) }"
            )
            data = await self._execute(
                q,
                {"input": inp, "key": idempotency_key},
                idempotency_key=idempotency_key,
                is_create=True,
            )
            return pb.JsonResponse(json=json.dumps(data["createPagoTelefonico"]))
        raise errors.TupagoError(f"unsupported GraphQL create: {operation}")

    async def aclose(self) -> None:
        await self._client.aclose()
