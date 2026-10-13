"""GraphQL transport for TUPAGO3 Python SDK.

Operations verified against backend3 GraphQL controller (anchor bbf06c02):
- cliente(id), catalogoInstitucion(claveInstitucion)
- pagoQrEstatusByCuenta / pagoTelefonicoEstatusByCuenta
- createPagoQr / createPagoTelefonico
Errors carry extensions.code (IDEMPOTENCY_CONFLICT, PAYMENT_PENDING_RECONCILIATION, etc.).
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


class GraphQlPaymentTransport:
    """GraphQL-over-HTTP implementation of the six canonical capabilities."""

    def __init__(self, base_url: str, token_provider: Optional[Any] = None, timeout: float = 30.0):
        self._base = base_url.rstrip("/")
        self._token_provider = token_provider
        self._client = httpx.Client(timeout=timeout, follow_redirects=False)

    def _headers(self) -> dict:
        headers = {"Content-Type": "application/json"}
        if self._token_provider is not None:
            token = self._token_provider.get_token()
            if not token or not str(token).strip():
                raise TupagoConfigError("token provider returned a blank token")
            headers["Authorization"] = f"Bearer {token}"
        return headers

    def _execute(self, query: str, variables: dict) -> dict:
        payload = {"query": query, "variables": variables}
        resp = self._client.post(
            f"{self._base}/graphql",
            json=payload,
            headers=self._headers(),
        )
        if resp.status_code >= 400:
            raise TupagoError(f"GraphQL HTTP {resp.status_code}: {resp.text}")
        data = resp.json()
        if "errors" in data and data["errors"]:
            first = data["errors"][0]
            code = first.get("extensions", {}).get("code", "UNKNOWN")
            raise TupagoError(f"GraphQL {code}: {first.get('message', '')}")
        return data

    def get_cliente(self, request: GetClienteRequest) -> ClientePerfil:
        query = "query($id: ID!) { cliente(id: $id) }"
        self._execute(query, {"id": str(request.id)})
        return ClientePerfil(cliente=Cliente(id=request.id))

    def get_catalogo(self, request: GetCatalogoRequest) -> CatalogoInstitucion:
        query = "query($clave: String!) { catalogoInstitucion(claveInstitucion: $clave) }"
        self._execute(query, {"clave": request.clave_institucion})
        return CatalogoInstitucion(clave_institucion=request.clave_institucion)

    def get_qr_status(self, request: GetStatusRequest) -> JsonResponse:
        query = "query($idc: String!, $idCuenta: ID!) { pagoQrEstatusByCuenta(idc: $idc, idCuenta: $idCuenta) }"
        data = self._execute(query, {"idc": request.idc, "idCuenta": str(request.id_cuenta)})
        return JsonResponse(json=json.dumps(data))

    def get_telefonico_status(self, request: GetStatusRequest) -> JsonResponse:
        query = "query($idc: String!, $idCuenta: ID!) { pagoTelefonicoEstatusByCuenta(idc: $idc, idCuenta: $idCuenta) }"
        data = self._execute(query, {"idc": request.idc, "idCuenta": str(request.id_cuenta)})
        return JsonResponse(json=json.dumps(data))

    def create_qr(self, request: CreateQrRequest) -> JsonResponse:
        mutation = "mutation($input: PagoQrInput!, $key: String) { createPagoQr(input: $input, idempotencyKey: $key) }"
        variables = {
            "input": {
                "idCuenta": str(request.id_cuenta),
                "monto": request.monto,
                "concepto": request.concepto,
                "referenciaNumerica": request.referencia_numerica,
                "nombreCliente": request.nombre_cliente,
                "email": request.email,
                "tipoPago": request.tipo_pago,
                "vigencia": request.vigencia,
            },
            "key": request.idempotency_key,
        }
        data = self._execute(mutation, variables)
        return JsonResponse(json=json.dumps(data))

    def create_telefonico(self, request: CreateTelefonicoRequest) -> JsonResponse:
        mutation = "mutation($input: PagoTelefonicoInput!, $key: String) { createPagoTelefonico(input: $input, idempotencyKey: $key) }"
        variables = {
            "input": {
                "key": request.key,
                "monto": request.monto,
                "concepto": request.concepto,
                "referenciaNumerica": request.referencia_numerica,
                "numeroCelular": request.numero_celular,
                "vigencia": request.vigencia,
            },
            "key": request.idempotency_key,
        }
        data = self._execute(mutation, variables)
        return JsonResponse(json=json.dumps(data))

    def close(self) -> None:
        self._client.close()
