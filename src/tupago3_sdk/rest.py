"""REST transport for the TUPAGO3 payment RPCs (phase B).

Implements :class:`~tupago3_sdk.transport.SyncPaymentTransport` /
:class:`~tupago3_sdk.transport.AsyncPaymentTransport` over HTTP using ``httpx``. The six
capabilities map to the backend3 REST routes under ``/api`` (discovered from backend3 source,
not guessed):

  C5 CreateQr         -> POST /api/payments/msjpagoqrusuario   (Idempotency-Key header)
  C6 CreateTelefonico -> POST /api/payments/msjpagotelefonicousuario (Idempotency-Key header)
  C3 GetQrStatus      -> POST /api/payments/estatusmsjqrusuario
  C4 GetTelefonicoStatus -> POST /api/payments/estatusmsjcobrotelefonicousuario
  C5 GetCliente       -> GET  /api/clientes/{id}
  C6 GetCatalogo      -> GET  /api/catalogo-institucions/clave/{claveInstitucion}

Responses are converted to the SAME protobuf types the gRPC path returns
(``ClientePerfil``, ``CatalogoInstitucion``, ``JsonResponse``) so the public client surface is
unchanged. Financial-safety rules (no SDK retry on create, no redirects, verified TLS on
non-loopback, remote plaintext impossible) are enforced here, mirroring the gRPC transport.
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


def _is_pending(status: int) -> bool:
    # HTTP 202 Accepted is the backend's async/pending marker for creates.
    return status == 202


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


class RestTransport:
    """Synchronous REST transport (SDK-authoritative httpx client)."""

    def __init__(self, *, config: ClientConfig, token_provider, retry_policy=None) -> None:
        validate_transport(config)  # reuses the fail-closed plaintext/TLS rule
        self._config = config
        self._token_provider = token_provider
        self._retry_policy = retry_policy or retry.RetryPolicy()
        scheme = "https" if config.use_tls else "http"
        verify: Any = True
        if config.use_tls and config.tls.ca_cert_path:
            verify = config.tls.ca_cert_path
        # No automatic transport retries; the SDK owns retry policy (reads only).
        self._client = httpx.Client(
            base_url=f"{scheme}://{config.endpoint}{_BASE_PATH}",
            verify=verify,
            follow_redirects=False,
            timeout=config.deadline_seconds or 30.0,
        )

    # -- helpers ------------------------------------------------------------
    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self._token_provider is not None:
            token = self._token_provider.get_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
        return headers

    def _json_response(self, resp: httpx.Response, *, operation: str, idempotency_key=None) -> str:
        body = resp.text or ""
        if _is_pending(resp.status_code):
            raise errors.TupagoPendingReconciliationError(
                body or "payment pending reconciliation",
                grpc_code="ABORTED",
                operation=operation,
                idempotency_key=idempotency_key,
            )
        if not _is_success(resp.status_code):
            raise _status_to_error(
                resp.status_code, body, operation=operation, idempotency_key=idempotency_key
            )
        return body

    def _request_read(self, method: str, path: str, *, json_body=None) -> httpx.Response:
        def _invoke() -> httpx.Response:
            return self._client.request(method, path, headers=self._headers(), json=json_body)

        # Bounded retry for transient read failures only (no create retry ever).
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
                    raise
        assert last is not None
        raise last

    # -- RPC dispatch -------------------------------------------------------
    def read(self, operation: str, method_name: str, request: object):
        if operation == "GetCliente":
            assert isinstance(request, pb.GetClienteRequest)
            resp = self._request_read("GET", f"/clientes/{int(request.id)}")
            body = self._json_response(resp, operation=operation)
            return _json_to_cliente_perfil(body)
        if operation == "GetCatalogo":
            assert isinstance(request, pb.GetCatalogoRequest)
            resp = self._request_read(
                "GET", f"/catalogo-institucions/clave/{request.clave_institucion}"
            )
            body = self._json_response(resp, operation=operation)
            return _json_to_catalogo(body)
        if operation == "GetQrStatus":
            assert isinstance(request, pb.GetStatusRequest)
            resp = self._request_read(
                "POST",
                "/payments/estatusmsjqrusuario",
                json_body={"idc": request.idc, "idCuenta": int(request.id_cuenta)},
            )
            body = self._json_response(resp, operation=operation)
            return pb.JsonResponse(json=body)
        if operation == "GetTelefonicoStatus":
            assert isinstance(request, pb.GetStatusRequest)
            resp = self._request_read(
                "POST",
                "/payments/estatusmsjcobrotelefonicousuario",
                json_body={"idCuenta": int(request.id_cuenta), "idMsjdeCobro": request.idc},
            )
            body = self._json_response(resp, operation=operation)
            return pb.JsonResponse(json=body)
        raise errors.TupagoError(f"unsupported REST read: {operation}")

    def create(self, operation: str, method_name: str, request: object, *, idempotency_key=None):
        if operation == "CreateQr":
            path, payload = "/payments/msjpagoqrusuario", _create_qr_payload(request)
        elif operation == "CreateTelefonico":
            path, payload = (
                "/payments/msjpagotelefonicousuario",
                _create_telefonico_payload(request),
            )
        else:
            raise errors.TupagoError(f"unsupported REST create: {operation}")
        headers = self._headers()
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        try:
            resp = self._client.request("POST", path, headers=headers, json=payload)
        except httpx.TransportError as exc:
            # Network failure after handing the financial request to the transport:
            # fail closed as UnknownOutcome (false "definitely not sent" is forbidden).
            raise errors.TupagoUnknownOutcomeError(
                f"REST create transport error after dispatch: {exc}",
                grpc_code="UNKNOWN",
                operation=operation,
                idempotency_key=idempotency_key,
            ) from exc
        body = self._json_response(resp, operation=operation, idempotency_key=idempotency_key)
        return pb.JsonResponse(json=body)

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
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def _json_response(self, resp, *, operation, idempotency_key=None) -> str:
        body = resp.text or ""
        if _is_pending(resp.status_code):
            raise errors.TupagoPendingReconciliationError(
                body or "payment pending reconciliation",
                grpc_code="ABORTED",
                operation=operation,
                idempotency_key=idempotency_key,
            )
        if not _is_success(resp.status_code):
            raise _status_to_error(
                resp.status_code, body, operation=operation, idempotency_key=idempotency_key
            )
        return body

    async def _request_read(self, method: str, path: str, *, json_body=None):
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
                    raise
        assert last is not None
        raise last

    async def read_async(self, operation: str, method_name: str, request: object):
        if operation == "GetCliente":
            assert isinstance(request, pb.GetClienteRequest)
            resp = await self._request_read("GET", f"/clientes/{int(request.id)}")
            body = await self._json_response(resp, operation=operation)
            return _json_to_cliente_perfil(body)
        if operation == "GetCatalogo":
            assert isinstance(request, pb.GetCatalogoRequest)
            resp = await self._request_read(
                "GET", f"/catalogo-institucions/clave/{request.clave_institucion}"
            )
            body = await self._json_response(resp, operation=operation)
            return _json_to_catalogo(body)
        if operation == "GetQrStatus":
            assert isinstance(request, pb.GetStatusRequest)
            resp = await self._request_read(
                "POST",
                "/payments/estatusmsjqrusuario",
                json_body={"idc": request.idc, "idCuenta": int(request.id_cuenta)},
            )
            body = await self._json_response(resp, operation=operation)
            return pb.JsonResponse(json=body)
        if operation == "GetTelefonicoStatus":
            assert isinstance(request, pb.GetStatusRequest)
            resp = await self._request_read(
                "POST",
                "/payments/estatusmsjcobrotelefonicousuario",
                json_body={"idCuenta": int(request.id_cuenta), "idMsjdeCobro": request.idc},
            )
            body = await self._json_response(resp, operation=operation)
            return pb.JsonResponse(json=body)
        raise errors.TupagoError(f"unsupported REST read: {operation}")

    async def create_async(
        self, operation: str, method_name: str, request: object, *, idempotency_key=None
    ):
        if operation == "CreateQr":
            path, payload = "/payments/msjpagoqrusuario", _create_qr_payload(request)
        elif operation == "CreateTelefonico":
            path, payload = (
                "/payments/msjpagotelefonicousuario",
                _create_telefonico_payload(request),
            )
        else:
            raise errors.TupagoError(f"unsupported REST create: {operation}")
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
        body = await self._json_response(resp, operation=operation, idempotency_key=idempotency_key)
        return pb.JsonResponse(json=body)

    async def aclose(self) -> None:
        await self._client.aclose()


# --- payload / response mapping --------------------------------------------
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


def _json_to_catalogo(body: str) -> pb.CatalogoInstitucion:
    data = json.loads(body)
    return pb.CatalogoInstitucion(
        id=int(data.get("id", 0)),
        clave_institucion=str(data.get("claveInstitucion", "")),
        nombre_corto=str(data.get("nombreCorto", "")),
    )


def _json_to_cliente_perfil(body: str) -> pb.ClientePerfil:
    data = json.loads(body)
    cliente = data.get("cliente", data)
    catalogo = data.get("catalogoInstitucion", {}) or {}
    return pb.ClientePerfil(
        cliente=pb.Cliente(
            id=int(cliente.get("id", 0)),
            nombre=str(cliente.get("nombre", "")),
            clave_institucion=str(cliente.get("claveInstitucion", "")),
            key=str(cliente.get("key", "")),
        ),
        catalogo_institucion=pb.CatalogoInstitucion(
            id=int(catalogo.get("id", 0)) if catalogo else 0,
            clave_institucion=str(catalogo.get("claveInstitucion", "")) if catalogo else "",
            nombre_corto=str(catalogo.get("nombreCorto", "")) if catalogo else "",
        ),
    )
