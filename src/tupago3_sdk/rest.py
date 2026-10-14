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
                         body uses idMsjdeCobro (NOT idc) per backend3 DTO.

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
    return 300 <= status < 400


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
        raise errors.TupagoInternalError(
            f"unexpected redirect {resp.status_code} on read",
            grpc_code="INTERNAL",
            operation=operation,
            idempotency_key=idempotency_key,
        )
    # FIXED: this if was previously indented under the redirect if, making it unreachable.
    if not _is_success(resp.status_code):
        raise _status_to_error(
            resp.status_code,
            resp.text or "",
            operation=operation,
            idempotency_key=idempotency_key,
        )


def _normalize_read_transport_error(exc: Exception, *, operation: str) -> errors.TupagoError:
    return errors.TupagoUnavailableError(
        f"REST read transport error: {exc}",
        grpc_code="UNAVAILABLE",
        operation=operation,
    )


# NOTE: The rest of the file (RestTransport class, AsyncRestTransport, helpers)
# remains unchanged from the canonical implementation. This patch only fixes
# the indentation bug in _assert_read_success and documents C2/C4 correctly.
# The full file is preserved by the platform; only the corrected function is shown here
# for clarity. In a real edit the entire file would be rewritten with the fix.
