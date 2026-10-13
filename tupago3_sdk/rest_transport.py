"""REST transport for TUPAGO3 Python SDK.

Routes verified against backend3 anchor bbf06c02:
- C1: POST /api/payments/msjpagoqrusuario + Idempotency-Key (unwraps 'mensaje')
- C2: POST /api/payments/msjpagotelefonico + Idempotency-Key
- C3: POST /api/payments/estatusmsjqr (ByCuenta)
- C4: POST /api/payments/estatusmsjcobrotelefonico (ByCuenta)
Creates are single-attempt / no-blind-retry.
"""

from __future__ import annotations

import json
from typing import Any, Optional

import httpx

from .errors import TupagoConfigError, TupagoError
from .models import (
    CatalogoInstitucion,
    Cliente,
    ClientePerfil,
    CreateQrRequest,
    CreateTelefonicoRequest,
    GetCatalogoRequest,
    GetClienteRequest,
    GetStatusRequest,
    JsonResponse,
)


class RestPaymentTransport:
    """HTTP/REST implementation of the six canonical capabilities."""

    def __init__(self, base_url: str, token_provider: Optional[Any] = None, timeout: float = 30.0):
        self._base = base_url.rstrip("/")
        self._token_provider = token_provider
        self._client = httpx.Client(timeout=timeout, follow_redirects=False)

    def _headers(self, idempotency_key: Optional[str] = None) -> dict:
        headers = {"Content-Type": "application/json"}
        if self._token_provider is not None:
            token = self._token_provider.get_token()
            if not token or not str(token).strip():
                raise TupagoConfigError("token provider returned a blank token")
            headers["Authorization"] = f"Bearer {token}"
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        return headers

    def _post(self, path: str, body: dict, idempotency_key: Optional[str] = None) -> dict:
        resp = self._client.post(
            f"{self._base}{path}",
            json=body,
            headers=self._headers(idempotency_key),
        )
        if resp.status_code >= 400:
            raise TupagoError(f"REST {resp.status_code}: {resp.text}")
        return resp.json()

    def get_cliente(self, request: GetClienteRequest) -> ClientePerfil:
        resp = self._client.get(
            f"{self._base}/api/clientes/{request.id}",
            headers=self._headers(),
        )
        resp.raise_for_status()
        data = resp.json()
        return ClientePerfil(
            cliente=Cliente(
                id=data.get("id", request.id),
                nombre=data.get("nombre", ""),
                clave_institucion=data.get("claveInstitucion", ""),
                key=data.get("key", ""),
            )
        )

    def get_catalogo(self, request: GetCatalogoRequest) -> CatalogoInstitucion:
        resp = self._client.get(
            f"{self._base}/api/catalogo-institucions/clave/{request.clave_institucion}",
            headers=self._headers(),
        )
        resp.raise_for_status()
        data = resp.json()
        return CatalogoInstitucion(
            id=data.get("id", 0),
            clave_institucion=data.get("claveInstitucion", request.clave_institucion),
            nombre_corto=data.get("nombreCorto", ""),
        )

    def get_qr_status(self, request: GetStatusRequest) -> JsonResponse:
        body = {"idc": request.idc, "idCuenta": request.id_cuenta}
        data = self._post("/api/payments/estatusmsjqr", body)
        return JsonResponse(json=json.dumps(data))

    def get_telefonico_status(self, request: GetStatusRequest) -> JsonResponse:
        body = {"idc": request.idc, "idCuenta": request.id_cuenta}
        data = self._post("/api/payments/estatusmsjcobrotelefonico", body)
        return JsonResponse(json=json.dumps(data))

    def create_qr(self, request: CreateQrRequest) -> JsonResponse:
        body = {
            "idCuenta": request.id_cuenta,
            "monto": request.monto,
            "concepto": request.concepto,
            "referenciaNumerica": request.referencia_numerica,
            "nombreCliente": request.nombre_cliente,
            "email": request.email,
            "tipoPago": request.tipo_pago,
            "vigencia": request.vigencia,
        }
        data = self._post("/api/payments/msjpagoqrusuario", body, request.idempotency_key)
        mensaje = data.get("mensaje", data)
        return JsonResponse(json=json.dumps(mensaje))

    def create_telefonico(self, request: CreateTelefonicoRequest) -> JsonResponse:
        body = {
            "key": request.key,
            "monto": request.monto,
            "concepto": request.concepto,
            "referenciaNumerica": request.referencia_numerica,
            "numeroCelular": request.numero_celular,
            "vigencia": request.vigencia,
        }
        data = self._post("/api/payments/msjpagotelefonico", body, request.idempotency_key)
        return JsonResponse(json=json.dumps(data))

    def close(self) -> None:
        self._client.close()
