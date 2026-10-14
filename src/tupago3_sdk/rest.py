"""REST transport for the TUPAGO3 payment RPCs (phase B, backend-contract R1).

Implements :class:`~tupago3_sdk.transport.SyncPaymentTransport` /
:class:`~tupago3_sdk.transport.AsyncPaymentTransport` over HTTP using ``httpx``. The six
capabilities map to the backend3 REST routes under ``/api`` — confirmed READ-ONLY against
backend3 at anchor ``bbf06c02bda06221b2f214075a4bf2bdf3345eb0``:

  C5 GetCliente         -> GET  /api/clientes/{id}            (returns ClienteDTO, NOT wrapped)
                         then GET /api/catalogo-institucions/clave/{claveInstitucion}
                         (only if cliente.claveInstitucion present) -> compose ClientePerfil
  C6 GetCatalogo       -> GET  /api/catalogo-institucions/clave/{claveInstitucion}
  C1 CreateQr          -> POST /api/payments/msjpagoqrusuario   (Idempotency-Key header)
                         body is {mensaje, emisor, banco}; the public JsonResponse carries
                         ONLY the serialized `mensaje` (matches gRPC/GraphQL which serialize
                         the bare result object).
  C2 CreateTelefonico  -> POST /api/payments/msjpagotelefonicousuario (Idempotency-Key header)
                         body is the bare MsjPagoTelefonicoResponseDTO; the public JsonResponse
                         carries the serialized DTO directly.
  C3 GetQrStatus       -> POST /api/payments/estatusmsjqr           (by-cuenta operation)
  C4 GetTelefonicoStatus -> POST /api/payments/estatusmsjcobrotelefonico (by-cuenta operation)

Responses are converted to the SAME protobuf types the gRPC path returns
(``ClientePerfil``, ``CatalogoInstitucion``, ``JsonResponse``) so the public client surface is
unchanged. Malformed SUCCESS responses fail closed (typed error / UnknownOutcome) — no silent
defaults. Reads normalize transport exhaustion to a public TupagoError. Creates are
single-attempt; any post-dispatch ambiguity (transport error, redirect, truncated/malformed
body) fails closed as TupagoUnknownOutcomeError.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from tupago3_sdk import auth, errors, retry
from tupago3_sdk.config import ClientConfig, validate_transport
from tupago3_sdk.generated import payment_pb2 as pb

_BASE_PATH = "/api"


def _is_success(status: int) -> bool:
    return 200 <= status < 300


def _is_redirect(status: int) -> bool:
    # A redirect on a financial create means the origin already received the POST:
    # the outcome is ambiguous, never "definitely not sent".
    return 300 <= status < 400


# --------------------------------------------------------------------------- #
# Fail-closed response validation (no silent defaults — gatekeeper R1 §12)
# --------------------------------------------------------------------------- #
def _loads(body: str, *, operation: str):
    try:
        data = json.loads(body or "")
    except json.JSONDecodeError as exc:
        raise errors.TupagoInternalError(
            f"malformed {operation} response: invalid JSON",
            grpc_code="INTERNAL",
            operation=operation,
        ) from exc
    if not isinstance(data, dict):
        raise errors.TupagoInternalError(
            f"malformed {operation} response: expected JSON object",
            grpc_code="INTERNAL",
            operation=operation,
        )
    return data


def _require(data: dict, key: str, *, operation: str):
    if key not in data or data[key] is None:
        raise errors.TupagoInternalError(
            f"malformed {operation} response: missing required field '{key}'",
            grpc_code="INTERNAL",
            operation=operation,
        )
    return data[key]


def _json_to_catalogo(body: str, *, operation: str = "GetCatalogo") -> pb.CatalogoInstitucion:
    data = _loads(body, operation=operation)
    # Required by contract: id, claveInstitucion, nombreCorto (R1 §12).
    _require(data, "id", operation=operation)
    _require(data, "claveInstitucion", operation=operation)
    _require(data, "nombreCorto", operation=operation)
    return pb.CatalogoInstitucion(
        id=int(data["id"]),
        clave_institucion=str(data["claveInstitucion"]),
        nombre_corto=str(data["nombreCorto"]),
    )


def _parse_cliente_dto(body: str, *, operation: str) -> pb.Cliente:
    data = _loads(body, operation=operation)
    # Required by contract: id, nombre, claveInstitucion (R1 §12).
    _require(data, "id", operation=operation)
    _require(data, "nombre", operation=operation)
    _require(data, "claveInstitucion", operation=operation)
    return pb.Cliente(
        id=int(data["id"]),
        nombre=str(data["nombre"]),
        clave_institucion=str(data["claveInstitucion"]),
        key=str(data.get("key") or ""),
    )


def _compose_cliente_perfil(
    cliente_body: str, catalogo_body: str | None, *, operation: str = "GetCliente"
) -> pb.ClientePerfil:
    cliente = _parse_cliente_dto(cliente_body, operation=operation)
    catalogo = pb.CatalogoInstitucion()
    if catalogo_body is not None:
        catalogo = _json_to_catalogo(catalogo_body, operation="GetCatalogo")
    return pb.ClientePerfil(cliente=cliente, catalogo_institucion=catalogo)


def _extract_create_qr_mensaje(body: str, *, operation: str, idempotency_key=None) -> str:
    """REST CreateQr returns {mensaje, emisor, banco}; expose ONLY `mensaje`.

    gRPC/GraphQL serialize the bare result object, so the public JsonResponse.json must
    equal the serialized `mensaje`. A missing/null/malformed mensaje after a create is an
    ambiguous outcome -> fail closed as UnknownOutcome (R1 §6/§17).
    """
    try:
        data = _loads(body, operation=operation)
    except errors.TupagoError as exc:
        raise errors.TupagoUnknownOutcomeError(
            f"REST CreateQr response malformed after dispatch: {exc}",
            grpc_code="UNKNOWN",
            operation=operation,
            idempotency_key=idempotency_key,
        ) from exc
    if not isinstance(data, dict) or "mensaje" not in data or data["mensaje"] is None:
        raise errors.TupagoUnknownOutcomeError(
            "REST CreateQr response missing 'mensaje' after dispatch",
            grpc_code="UNKNOWN",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    try:
        return json.dumps(data["mensaje"])
    except (TypeError, ValueError) as exc:
        raise errors.TupagoUnknownOutcomeError(
            f"REST CreateQr 'mensaje' not serializable after dispatch: {exc}",
            grpc_code="UNKNOWN",
            operation=operation,
            idempotency_key=idempotency_key,
        ) from exc


# --------------------------------------------------------------------------- #
# Status -> public TupagoError mapping
# --------------------------------------------------------------------------- #
def _status_to_error(
    status: int, body: str, *, operation: str, idempotency_key=None
) -> errors.TupagoError:
    if status == 401:
        return errors.TupagoUnauthenticatedError(
            body or "unauthenticated",
            grpc_code="UNAUTHENTICATED",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    if status == 403:
        return errors.TupagoPermissionDeniedError(
            body or "permission denied",
            grpc_code="PERMISSION_DENIED",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    if status == 404:
        return errors.TupagoNotFoundError(
            body or "not found",
            grpc_code="NOT_FOUND",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    if status == 409:
        return errors.TupagoAlreadyExistsError(
            body or "conflict",
            grpc_code="ALREADY_EXISTS",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    if status == 400:
        return errors.TupagoInvalidArgumentError(
            body or "invalid argument",
            grpc_code="INVALID_ARGUMENT",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    if status in (502, 503, 504):
        return errors.TupagoUnavailableError(
            body or "upstream unavailable",
            grpc_code="UNAVAILABLE",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    return errors.TupagoInternalError(
        body or f"http {status}",
        grpc_code="INTERNAL",
        operation=operation,
        idempotency_key=idempotency_key,
    )


def _assert_read_success(resp: httpx.Response, *, operation: str, idempotency_key=None) -> None:
    if _is_redirect(resp.status_code):
        # Reads do not follow redirects; an unexpected 3xx is a deterministic server error.
        raise errors.TupagoInternalError(
            f"unexpected redirect {resp.status_code} on read",
            grpc_code="INTERNAL",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    if not _is_success(resp.status_code):
        raise _status_to_error(
            resp.status_code,
            resp.text or "",
            operation=operation,
            idempotency_key=idempotency_key,
        )


def _normalize_read_transport_error(exc: Exception, *, operation: str) -> errors.TupagoError:
    # R1 §13: never let a raw httpx exception escape as the public API.
    return errors.TupagoUnavailableError(
        f"REST read transport error: {exc}",
        grpc_code="UNAVAILABLE",
        operation=operation,
    )


class RestTransport:
    """Synchronous REST transport (SDK-authoritative httpx client)."""

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
            base_url=f"{scheme}://{config.endpoint}{_BASE_PATH}",
            verify=verify,
            follow_redirects=False,
            timeout=config.deadline_seconds or 30.0,
        )

    # -- auth (fail-closed blank bearer, R1 §19) -----------------------------
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

    def _request_read(
        self, method: str, path: str, *, json_body=None, operation: str
    ) -> httpx.Response:
        def _invoke() -> httpx.Response:
            return self._client.request(method, path, headers=self._headers(), json=json_body)

        attempts = max(1, self._retry_policy.max_attempts)
        last: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                resp = _invoke()
                if resp.status_code in (502, 503, 504):
                    raise errors.TupagoUnavailableError(f"http {resp.status_code}")
                return resp
            except (httpx.TransportError, errors.TupagoUnavailableError) as exc:
                last = exc
                if attempt >= attempts:
                    raise _normalize_read_transport_error(exc, operation=operation) from exc
        assert last is not None
        raise _normalize_read_transport_error(last, operation=operation)

    # -- RPC dispatch (reads) ------------------------------------------------
    def read(self, operation: str, method_name: str, request: object):
        if operation == "GetCliente":
            assert isinstance(request, pb.GetClienteRequest)
            resp = self._request_read("GET", f"/clientes/{int(request.id)}", operation=operation)
            _assert_read_success(resp, operation=operation)
            cliente_body = resp.text or ""
            clave = _loads(cliente_body, operation=operation).get("claveInstitucion")
            catalogo_body: str | None = None
            if clave:
                cres = self._request_read(
                    "GET", f"/catalogo-institucions/clave/{clave}", operation="GetCatalogo"
                )
                _assert_read_success(cres, operation="GetCatalogo")
                catalogo_body = cres.text or ""
            return _compose_cliente_perfil(cliente_body, catalogo_body)
        if operation == "GetCatalogo":
            assert isinstance(request, pb.GetCatalogoRequest)
            resp = self._request_read(
                "GET",
                f"/catalogo-institucions/clave/{request.clave_institucion}",
                operation=operation,
            )
            _assert_read_success(resp, operation=operation)
            return _json_to_catalogo(resp.text or "", operation=operation)
        if operation == "GetQrStatus":
            assert isinstance(request, pb.GetStatusRequest)
            resp = self._request_read(
                "POST",
                "/payments/estatusmsjqr",
                json_body={"idc": request.idc, "idCuenta": int(request.id_cuenta)},
                operation=operation,
            )
            _assert_read_success(resp, operation=operation)
            return pb.JsonResponse(json=resp.text or "")
        if operation == "GetTelefonicoStatus":
            assert isinstance(request, pb.GetStatusRequest)
            resp = self._request_read(
                "POST",
                "/payments/estatusmsjcobrotelefonico",
                json_body={"idCuenta": int(request.id_cuenta), "idMsjdeCobro": request.idc},
                operation=operation,
            )
            _assert_read_success(resp, operation=operation)
            return pb.JsonResponse(json=resp.text or "")
        raise errors.TupagoError(f"unsupported REST read: {operation}")

    # -- RPC dispatch (creates) ----------------------------------------------
    def _do_create(self, path: str, payload: dict, *, operation: str, idempotency_key=None) -> str:
        headers = self._headers()
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        try:
            resp = self._client.request("POST", path, headers=headers, json=payload)
        except httpx.TransportError as exc:
            raise errors.TupagoUnknownOutcomeError(
                f"REST create transport error after dispatch: {exc}",
                grpc_code="UNKNOWN",
                operation=operation,
                idempotency_key=idempotency_key,
            ) from exc
        # A redirect after a financial create means the origin already received the POST:
        # the outcome is ambiguous (R1 §16).
        if _is_redirect(resp.status_code):
            raise errors.TupagoUnknownOutcomeError(
                f"REST create received redirect {resp.status_code} after dispatch",
                grpc_code="UNKNOWN",
                operation=operation,
                idempotency_key=idempotency_key,
            )
        # Authoritative accepted-but-unresolved create: explicit PendingReconciliation,
        # never generic success and never blind retry.
        if resp.status_code == 202:
            raise errors.TupagoPendingReconciliationError(
                resp.text or "Payment pending reconciliation; do not retry; query status first",
                grpc_code="ABORTED",
                operation=operation,
                idempotency_key=idempotency_key,
            )
        # Authoritative accepted-but-unresolved create: explicit PendingReconciliation,
        # never generic success and never blind retry.
        if resp.status_code == 202:
            raise errors.TupagoPendingReconciliationError(
                resp.text or "Payment pending reconciliation; do not retry; query status first",
                grpc_code="ABORTED",
                operation=operation,
                idempotency_key=idempotency_key,
            )
        # CREATE: 502/503/504 after dispatch = UnknownOutcome (not retryable Unavailable).
        if resp.status_code in (502, 503, 504):
            raise errors.TupagoUnknownOutcomeError(
                f"REST create received HTTP {resp.status_code} after dispatch",
                grpc_code="UNKNOWN",
                operation=operation,
                idempotency_key=idempotency_key,
            )
        if not _is_success(resp.status_code):
            raise _status_to_error(
                resp.status_code,
                resp.text or "",
                operation=operation,
                idempotency_key=idempotency_key,
            )
        # Truncated/malformed create success == ambiguous outcome (R1 §17). Return the raw
        # body; the create methods parse it fail-closed as UnknownOutcome.
        return resp.text or ""

    def create(self, operation: str, method_name: str, request: object, *, idempotency_key=None):
        if operation == "CreateQr":
            body = self._do_create(
                "/payments/msjpagoqrusuario",
                _create_qr_payload(request),
                operation=operation,
                idempotency_key=idempotency_key,
            )
            return pb.JsonResponse(
                json=_extract_create_qr_mensaje(
                    body, operation=operation, idempotency_key=idempotency_key
                )
            )
        if operation == "CreateTelefonico":
            body = self._do_create(
                "/payments/msjpagotelefonicousuario",
                _create_telefonico_payload(request),
                operation=operation,
                idempotency_key=idempotency_key,
            )
            # CreateTelefonico returns the bare DTO; expose it directly (matches gRPC/GraphQL).
            # A malformed success body is an ambiguous outcome -> UnknownOutcome (R1 §17).
            try:
                data = _loads(body, operation=operation)
            except errors.TupagoError as exc:
                raise errors.TupagoUnknownOutcomeError(
                    f"REST CreateTelefonico response malformed after dispatch: {exc}",
                    grpc_code="UNKNOWN",
                    operation=operation,
                    idempotency_key=idempotency_key,
                ) from exc
            return pb.JsonResponse(json=json.dumps(data))
        raise errors.TupagoError(f"unsupported REST create: {operation}")

    def close(self) -> None:
        self._client.close()


class AsyncRestTransport:
    """Asynchronous REST transport (SDK-authoritative httpx async client)."""

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
            base_url=f"{scheme}://{config.endpoint}{_BASE_PATH}",
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

    async def _request_read(self, method: str, path: str, *, json_body=None, operation: str):
        headers = await self._headers()
        last: Exception | None = None
        for attempt in range(1, self._max_read_attempts + 1):
            try:
                resp = await self._client.request(method, path, headers=headers, json=json_body)
                if resp.status_code in (502, 503, 504):
                    raise errors.TupagoUnavailableError(f"http {resp.status_code}")
                return resp
            except (httpx.TransportError, errors.TupagoUnavailableError) as exc:
                last = exc
                if attempt >= self._max_read_attempts:
                    raise _normalize_read_transport_error(exc, operation=operation) from exc
        assert last is not None
        raise _normalize_read_transport_error(last, operation=operation)

    async def read_async(self, operation: str, method_name: str, request: object):
        if operation == "GetCliente":
            assert isinstance(request, pb.GetClienteRequest)
            resp = await self._request_read(
                "GET", f"/clientes/{int(request.id)}", operation=operation
            )
            _assert_read_success(resp, operation=operation)
            cliente_body = resp.text or ""
            clave = _loads(cliente_body, operation=operation).get("claveInstitucion")
            catalogo_body: str | None = None
            if clave:
                cres = await self._request_read(
                    "GET", f"/catalogo-institucions/clave/{clave}", operation="GetCatalogo"
                )
                _assert_read_success(cres, operation="GetCatalogo")
                catalogo_body = cres.text or ""
            return _compose_cliente_perfil(cliente_body, catalogo_body)
        if operation == "GetCatalogo":
            assert isinstance(request, pb.GetCatalogoRequest)
            resp = await self._request_read(
                "GET",
                f"/catalogo-institucions/clave/{request.clave_institucion}",
                operation=operation,
            )
            _assert_read_success(resp, operation=operation)
            return _json_to_catalogo(resp.text or "", operation=operation)
        if operation == "GetQrStatus":
            assert isinstance(request, pb.GetStatusRequest)
            resp = await self._request_read(
                "POST",
                "/payments/estatusmsjqr",
                json_body={"idc": request.idc, "idCuenta": int(request.id_cuenta)},
                operation=operation,
            )
            _assert_read_success(resp, operation=operation)
            return pb.JsonResponse(json=resp.text or "")
        if operation == "GetTelefonicoStatus":
            assert isinstance(request, pb.GetStatusRequest)
            resp = await self._request_read(
                "POST",
                "/payments/estatusmsjcobrotelefonico",
                json_body={"idCuenta": int(request.id_cuenta), "idMsjdeCobro": request.idc},
                operation=operation,
            )
            _assert_read_success(resp, operation=operation)
            return pb.JsonResponse(json=resp.text or "")
        raise errors.TupagoError(f"unsupported REST read: {operation}")

    async def _do_create(
        self, path: str, payload: dict, *, operation: str, idempotency_key=None
    ) -> str:
        headers = await self._headers()
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        try:
            resp = await self._client.request("POST", path, headers=headers, json=payload)
        except httpx.TransportError as exc:
            raise errors.TupagoUnknownOutcomeError(
                f"REST create transport error after dispatch: {exc}",
                grpc_code="UNKNOWN",
                operation=operation,
                idempotency_key=idempotency_key,
            ) from exc
        if _is_redirect(resp.status_code):
            raise errors.TupagoUnknownOutcomeError(
                f"REST create received redirect {resp.status_code} after dispatch",
                grpc_code="UNKNOWN",
                operation=operation,
                idempotency_key=idempotency_key,
            )
        if not _is_success(resp.status_code):
            raise _status_to_error(
                resp.status_code,
                resp.text or "",
                operation=operation,
                idempotency_key=idempotency_key,
            )
        # Truncated/malformed create success == ambiguous outcome (R1 §17). Return raw body.
        return resp.text or ""

    async def create_async(
        self, operation: str, method_name: str, request: object, *, idempotency_key=None
    ):
        if operation == "CreateQr":
            body = await self._do_create(
                "/payments/msjpagoqrusuario",
                _create_qr_payload(request),
                operation=operation,
                idempotency_key=idempotency_key,
            )
            return pb.JsonResponse(
                json=_extract_create_qr_mensaje(
                    body, operation=operation, idempotency_key=idempotency_key
                )
            )
        if operation == "CreateTelefonico":
            body = await self._do_create(
                "/payments/msjpagotelefonicousuario",
                _create_telefonico_payload(request),
                operation=operation,
                idempotency_key=idempotency_key,
            )
            # CreateTelefonico returns the bare DTO; expose it directly (matches gRPC/GraphQL).
            # A malformed success body is an ambiguous outcome -> UnknownOutcome (R1 §17).
            try:
                data = _loads(body, operation=operation)
            except errors.TupagoError as exc:
                raise errors.TupagoUnknownOutcomeError(
                    f"REST CreateTelefonico response malformed after dispatch: {exc}",
                    grpc_code="UNKNOWN",
                    operation=operation,
                    idempotency_key=idempotency_key,
                ) from exc
            return pb.JsonResponse(json=json.dumps(data))
        raise errors.TupagoError(f"unsupported REST create: {operation}")

    async def aclose(self) -> None:
        await self._client.aclose()


# --- payload mapping ---------------------------------------------------------
def _create_qr_payload(request) -> dict[str, Any]:
    if isinstance(request, pb.CreateQrRequest):
        return {
            "idCuenta": int(request.id_cuenta),
            "monto": float(request.monto),
            "concepto": request.concepto,
            "referenciaNumerica": int(request.referencia_numerica),
            "nombreCliente": request.nombre_cliente,
            "email": request.email,
            "tipoPago": int(request.tipo_pago),
            "vigencia": int(request.vigencia),
        }
    return {
        "idCuenta": int(request.id_cuenta),
        "monto": float(request.double_monto()),
        "concepto": request.concepto,
        "referenciaNumerica": int(request.referencia_numerica),
        "nombreCliente": request.nombre_cliente,
        "email": request.email,
        "tipoPago": int(request.tipo_pago),
        "vigencia": int(request.vigencia),
    }


def _create_telefonico_payload(request) -> dict[str, Any]:
    if isinstance(request, pb.CreateTelefonicoRequest):
        return {
            "key": request.key,
            "monto": float(request.monto),
            "concepto": request.concepto,
            "referenciaNumerica": int(request.referencia_numerica),
            "numeroCelular": request.numero_celular,
            "vigencia": int(request.vigencia),
        }
    return {
        "key": request.key,
        "monto": float(request.double_monto()),
        "concepto": request.concepto,
        "referenciaNumerica": int(request.referencia_numerica),
        "numeroCelular": request.numero_celular,
        "vigencia": int(request.vigencia),
    }


# --- composed-perfil parsers (GraphQL shape: {cliente, catalogoInstitucion}) ---
# Kept for the GraphQL transport, which receives an already-composed ClientePerfil from
# the backend GraphQL `cliente` resolver. Fail-closed: required fields enforced (R1 §12).
def _json_to_cliente_perfil(body: str, *, operation: str = "GetCliente") -> pb.ClientePerfil:
    data = _loads(body, operation=operation)
    if "cliente" not in data or data["cliente"] is None:
        raise errors.TupagoInternalError(
            f"malformed {operation} response: missing 'cliente'",
            grpc_code="INTERNAL",
            operation=operation,
        )
    catalogo = data.get("catalogoInstitucion") or {}
    cliente = data["cliente"]
    _require(cliente, "id", operation=operation)
    _require(cliente, "nombre", operation=operation)
    _require(cliente, "claveInstitucion", operation=operation)
    return pb.ClientePerfil(
        cliente=pb.Cliente(
            id=int(cliente["id"]),
            nombre=str(cliente["nombre"]),
            clave_institucion=str(cliente["claveInstitucion"]),
            key=str(cliente.get("key") or ""),
        ),
        catalogo_institucion=(
            _json_to_catalogo(json.dumps(catalogo), operation="GetCatalogo")
            if catalogo
            else pb.CatalogoInstitucion()
        ),
    )
