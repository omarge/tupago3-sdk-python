"""GraphQL transport for the TUPAGO3 payment RPCs (phase C, backend-contract R1).

Implements :class:`~tupago3_sdk.transport.SyncPaymentTransport` /
:class:`~tupago3_sdk.transport.AsyncPaymentTransport` over the backend3 GraphQL endpoint
(``/graphql``), discovered from ``src/main/resources/graphql/tupago.graphqls`` (anchor
``bbf06c02bda06221b2f214075a4bf2bdf3345eb0``). All six capabilities are expressed as GraphQL
operations:

  C1 CreateQr          -> mutation createPagoQr(input, idempotencyKey)
  C2 CreateTelefonico  -> mutation createPagoTelefonico(input, idempotencyKey)
  C3 GetQrStatus       -> query pagoQrEstatusByCuenta(idc: String!, idCuenta: ID!)
  C4 GetTelefonicoStatus -> query pagoTelefonicoEstatusByCuenta(idc: String!, idCuenta: ID!)
  C5 GetCliente        -> query cliente(id: ID!): ClientePerfil
  C6 GetCatalogo       -> query catalogoInstitucion(claveInstitucion: String!): CatalogoInstitucion

Identifiers are sent as GraphQL ``ID`` (string form) to preserve int64 exactness. Responses are
converted to the SAME protobuf types the gRPC/REST paths return, so the public client surface is
unchanged. Financial-safety rules mirror the REST transport (R1):

* Reads retry only transient conditions (UNAVAILABLE / RESOURCE_EXHAUSTED / DEADLINE_EXCEEDED),
  bounded by ``retry_policy.max_attempts``; all other read errors are terminal and typed.
* Creates are SINGLE-ATTEMPT. Any post-dispatch ambiguity — transport error, redirect (3xx),
  non-JSON body, or a non-deterministic GraphQL error (anything but ALREADY_EXISTS) — fails
  closed as :class:`TupagoUnknownOutcomeError`. ALREADY_EXISTS is deterministic (idempotency
  conflict) and surfaces as a typed error.
* C1 CreateQr returns a ``{mensaje, emisor, banco}`` map (JSON scalar); the public JsonResponse
  carries ONLY the serialized ``mensaje`` — matching REST/gRPC which serialize the bare result.
* C2 CreateTelefonico returns the bare DTO (JSON scalar); the public JsonResponse carries the
  serialized DTO directly.
* Blank/empty bearer fails closed pre-dispatch (R1 §19) unless ``allow_unauthenticated=True``.
* A raw httpx exception never escapes the public API (R1 §13).
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

# Transient GraphQL error codes that MAY be retried for read-only operations (R1 §6).
_TRANSIENT_GRAPHQL_CODES = frozenset({"UNAVAILABLE", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED"})


def _is_redirect(status: int) -> bool:
    # A redirect on a financial create means the origin already received the POST:
    # the outcome is ambiguous, never "definitely not sent".
    return 300 <= status < 400


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
    if code in _TRANSIENT_GRAPHQL_CODES:
        return errors.TupagoUnavailableError(message, grpc_code=code)
    return errors.TupagoInternalError(message, grpc_code="INTERNAL")


def _is_transient_graphql_error(err: dict) -> bool:
    extensions = err.get("extensions") or {}
    code = (extensions.get("code") or "").upper()
    return code in _TRANSIENT_GRAPHQL_CODES


def _http_error(status: int, text: str, *, operation: str) -> errors.TupagoError:
    if status == 401:
        return errors.TupagoUnauthenticatedError(
            text or "unauthenticated", grpc_code="UNAUTHENTICATED", operation=operation
        )
    if status == 403:
        return errors.TupagoPermissionDeniedError(
            text or "forbidden", grpc_code="PERMISSION_DENIED", operation=operation
        )
    if status in (502, 503, 504):
        return errors.TupagoUnavailableError(
            text or f"http {status}", grpc_code="UNAVAILABLE", operation=operation
        )
    return errors.TupagoInternalError(
        text or f"http {status}", grpc_code="INTERNAL", operation=operation
    )


def _extract_create_qr_mensaje(value: Any, *, operation: str, idempotency_key=None) -> str:
    """GraphQL CreateQr returns a {mensaje, emisor, banco} map; expose ONLY `mensaje`.

    Matches REST/gRPC which serialize the bare result. A missing/null/malformed mensaje after a
    create is an ambiguous outcome -> fail closed as UnknownOutcome (R1 §6/§17).
    """
    if not isinstance(value, dict):
        raise errors.TupagoUnknownOutcomeError(
            "GraphQL CreateQr response not an object after dispatch",
            grpc_code="UNKNOWN",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    if "mensaje" not in value or value["mensaje"] is None:
        raise errors.TupagoUnknownOutcomeError(
            "GraphQL CreateQr response missing 'mensaje' after dispatch",
            grpc_code="UNKNOWN",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    try:
        return json.dumps(value["mensaje"])
    except (TypeError, ValueError) as exc:
        raise errors.TupagoUnknownOutcomeError(
            f"GraphQL CreateQr 'mensaje' not serializable after dispatch: {exc}",
            grpc_code="UNKNOWN",
            operation=operation,
            idempotency_key=idempotency_key,
        ) from exc


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

    # -- auth (fail-closed blank bearer, R1 §19) --------------------------------
    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        token = self._token_provider.get_token() if self._token_provider is not None else ""
        if not self._config.allow_unauthenticated and not (token and token.strip()):
            raise errors.TupagoUnauthenticatedError(
                "token_provider returned empty/blank bearer; auth required",
                grpc_code="UNAUTHENTICATED",
            )
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
        operation = query[:32]
        attempts = max(1, max_attempts)
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            headers = self._headers()
            try:
                resp = self._client.post("", headers=headers, json=payload)
            except httpx.TransportError as exc:
                # Network failure after handing a FINANCIAL create to the transport: fail
                # closed as UnknownOutcome (a false "definitely not sent" is forbidden).
                if is_create:
                    raise errors.TupagoUnknownOutcomeError(
                        f"GraphQL create transport error after dispatch: {exc}",
                        grpc_code="UNKNOWN",
                        operation=operation,
                        idempotency_key=idempotency_key,
                    ) from exc
                last_exc = errors.TupagoUnavailableError(
                    f"GraphQL read transport error: {exc}",
                    grpc_code="UNAVAILABLE",
                    operation=operation,
                )
                if attempt >= attempts:
                    raise last_exc
                continue
            if _is_redirect(resp.status_code):
                # Reads do not follow redirects; an unexpected 3xx is a server error.
                if is_create:
                    raise errors.TupagoUnknownOutcomeError(
                        f"GraphQL create received redirect {resp.status_code} after dispatch",
                        grpc_code="UNKNOWN",
                        operation=operation,
                        idempotency_key=idempotency_key,
                    )
                raise errors.TupagoInternalError(
                    f"unexpected redirect {resp.status_code} on read",
                    grpc_code="INTERNAL",
                    operation=operation,
                )
            if not (200 <= resp.status_code < 300):
                http_err = _http_error(resp.status_code, resp.text or "", operation=operation)
                # Reads retry only transient (502/503/504) conditions.
                if not is_create and resp.status_code in (502, 503, 504):
                    last_exc = http_err
                    if attempt >= attempts:
                        raise last_exc
                    continue
                raise http_err
            try:
                body = resp.json()
            except Exception as exc:
                if is_create:
                    raise errors.TupagoUnknownOutcomeError(
                        "GraphQL create response not JSON after dispatch",
                        grpc_code="UNKNOWN",
                        operation=operation,
                        idempotency_key=idempotency_key,
                    ) from exc
                raise errors.TupagoInternalError(
                    "malformed GraphQL response", grpc_code="INTERNAL", operation=operation
                ) from exc
            if not isinstance(body, dict):
                if is_create:
                    raise errors.TupagoUnknownOutcomeError(
                        "GraphQL create response not an object after dispatch",
                        grpc_code="UNKNOWN",
                        operation=operation,
                        idempotency_key=idempotency_key,
                    )
                raise errors.TupagoInternalError(
                    "malformed GraphQL response", grpc_code="INTERNAL", operation=operation
                )
            if body.get("errors"):
                err = body["errors"][0]
                mapped = _map_graphql_error(err)
                if is_create:
                    # Only ALREADY_EXISTS is deterministic; everything else is ambiguous.
                    if isinstance(mapped, errors.TupagoAlreadyExistsError):
                        raise mapped
                    raise errors.TupagoUnknownOutcomeError(
                        f"GraphQL create error after dispatch: {mapped.message}",
                        grpc_code="UNKNOWN",
                        operation=operation,
                        idempotency_key=idempotency_key,
                    )
                # Read: retry only transient conditions, bounded by max_attempts.
                if _is_transient_graphql_error(err):
                    last_exc = mapped
                    if attempt >= attempts:
                        raise last_exc
                    continue
                raise mapped
            return body.get("data", {})
        assert last_exc is not None
        raise last_exc

    # -- RPC dispatch ----------------------------------------------------------
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
            q = (
                "query($idc: String!, $idCuenta: ID!){ "
                "pagoQrEstatusByCuenta(idc:$idc, idCuenta:$idCuenta) }"
            )
            data = self._execute(
                q,
                {"idc": request.idc, "idCuenta": str(request.id_cuenta)},
                max_attempts=max_attempts,
            )
            return pb.JsonResponse(json=json.dumps(data["pagoQrEstatusByCuenta"]))
        if operation == "GetTelefonicoStatus":
            assert isinstance(request, pb.GetStatusRequest)
            q = (
                "query($idc: String!, $idCuenta: ID!){ "
                "pagoTelefonicoEstatusByCuenta(idc:$idc, idCuenta:$idCuenta) }"
            )
            data = self._execute(
                q,
                {"idc": request.idc, "idCuenta": str(request.id_cuenta)},
                max_attempts=max_attempts,
            )
            return pb.JsonResponse(json=json.dumps(data["pagoTelefonicoEstatusByCuenta"]))
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
            return pb.JsonResponse(
                json=_extract_create_qr_mensaje(
                    data["createPagoQr"], operation=operation, idempotency_key=idempotency_key
                )
            )
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
            # CreateTelefonico returns the bare DTO; expose it directly (matches REST/gRPC).
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
        if not self._config.allow_unauthenticated and not (token and token.strip()):
            raise errors.TupagoUnauthenticatedError(
                "token_provider returned empty/blank bearer; auth required",
                grpc_code="UNAUTHENTICATED",
            )
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
        operation = query[:32]
        attempts = max(1, max_attempts)
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            headers = await self._headers()
            try:
                resp = await self._client.post("", headers=headers, json=payload)
            except httpx.TransportError as exc:
                if is_create:
                    raise errors.TupagoUnknownOutcomeError(
                        f"GraphQL create transport error after dispatch: {exc}",
                        grpc_code="UNKNOWN",
                        operation=operation,
                        idempotency_key=idempotency_key,
                    ) from exc
                last_exc = errors.TupagoUnavailableError(
                    f"GraphQL read transport error: {exc}",
                    grpc_code="UNAVAILABLE",
                    operation=operation,
                )
                if attempt >= attempts:
                    raise last_exc
                continue
            if _is_redirect(resp.status_code):
                if is_create:
                    raise errors.TupagoUnknownOutcomeError(
                        f"GraphQL create received redirect {resp.status_code} after dispatch",
                        grpc_code="UNKNOWN",
                        operation=operation,
                        idempotency_key=idempotency_key,
                    )
                raise errors.TupagoInternalError(
                    f"unexpected redirect {resp.status_code} on read",
                    grpc_code="INTERNAL",
                    operation=operation,
                )
            if not (200 <= resp.status_code < 300):
                http_err = _http_error(resp.status_code, resp.text or "", operation=operation)
                if not is_create and resp.status_code in (502, 503, 504):
                    last_exc = http_err
                    if attempt >= attempts:
                        raise last_exc
                    continue
                raise http_err
            try:
                body = resp.json()
            except Exception as exc:
                if is_create:
                    raise errors.TupagoUnknownOutcomeError(
                        "GraphQL create response not JSON after dispatch",
                        grpc_code="UNKNOWN",
                        operation=operation,
                        idempotency_key=idempotency_key,
                    ) from exc
                raise errors.TupagoInternalError(
                    "malformed GraphQL response", grpc_code="INTERNAL", operation=operation
                ) from exc
            if not isinstance(body, dict):
                if is_create:
                    raise errors.TupagoUnknownOutcomeError(
                        "GraphQL create response not an object after dispatch",
                        grpc_code="UNKNOWN",
                        operation=operation,
                        idempotency_key=idempotency_key,
                    )
                raise errors.TupagoInternalError(
                    "malformed GraphQL response", grpc_code="INTERNAL", operation=operation
                )
            if body.get("errors"):
                err = body["errors"][0]
                mapped = _map_graphql_error(err)
                if is_create:
                    if isinstance(mapped, errors.TupagoAlreadyExistsError):
                        raise mapped
                    raise errors.TupagoUnknownOutcomeError(
                        f"GraphQL create error after dispatch: {mapped.message}",
                        grpc_code="UNKNOWN",
                        operation=operation,
                        idempotency_key=idempotency_key,
                    )
                if _is_transient_graphql_error(err):
                    last_exc = mapped
                    if attempt >= attempts:
                        raise last_exc
                    continue
                raise mapped
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
            q = (
                "query($idc: String!, $idCuenta: ID!){ "
                "pagoQrEstatusByCuenta(idc:$idc, idCuenta:$idCuenta) }"
            )
            data = await self._execute(
                q,
                {"idc": request.idc, "idCuenta": str(request.id_cuenta)},
                max_attempts=self._max_read_attempts,
            )
            return pb.JsonResponse(json=json.dumps(data["pagoQrEstatusByCuenta"]))
        if operation == "GetTelefonicoStatus":
            assert isinstance(request, pb.GetStatusRequest)
            q = (
                "query($idc: String!, $idCuenta: ID!){ "
                "pagoTelefonicoEstatusByCuenta(idc:$idc, idCuenta:$idCuenta) }"
            )
            data = await self._execute(
                q,
                {"idc": request.idc, "idCuenta": str(request.id_cuenta)},
                max_attempts=self._max_read_attempts,
            )
            return pb.JsonResponse(json=json.dumps(data["pagoTelefonicoEstatusByCuenta"]))
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
            return pb.JsonResponse(
                json=_extract_create_qr_mensaje(
                    data["createPagoQr"], operation=operation, idempotency_key=idempotency_key
                )
            )
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
