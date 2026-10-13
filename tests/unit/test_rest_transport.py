"""Phase B: REST transport tests against an in-process mock of backend3 REST routes.

The mock mirrors the routes discovered in backend3:
  GET  /api/clientes/{id}
  GET  /api/catalogo-institucions/clave/{clave}
  POST /api/payments/msjpagoqrusuario        (CreateQr, Idempotency-Key header)
  POST /api/payments/msjpagotelefonicousuario (CreateTelefonico)
  POST /api/payments/estatusmsjqrusuario      (GetQrStatus)
  POST /api/payments/estatusmsjcobrotelefonicousuario (GetTelefonicoStatus)
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from tupago3_sdk import (
    AsyncTupagoPaymentClient,
    ClientConfig,
    InsecureTransportError,
    TupagoPaymentClient,
    TupagoUnauthenticatedError,
    TupagoUnknownOutcomeError,
    models,
)


async def _aclient(port, token=None, allow_unauth=True):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=allow_unauth)
    return AsyncTupagoPaymentClient(cfg, token_provider=token, transport="rest")


async def test_rest_async_get_cliente(rest_server):
    c = await _aclient(rest_server)
    perfil = await c.get_cliente(models.GetClienteRequest(id=1))
    assert perfil.cliente.nombre == "Cliente Demo"
    await c.aclose()


async def test_rest_async_create_qr(rest_server):
    c = await _aclient(rest_server)
    resp = await c.create_qr(
        models.CreateQrRequest(
            id_cuenta=1,
            monto=10.5,
            concepto="c",
            referencia_numerica=2,
            nombre_cliente="n",
            email="e",
            tipo_pago=1,
            vigencia=30,
            idempotency_key="IDEK",
        )
    )
    assert resp.json
    await c.aclose()


async def test_rest_async_remote_plaintext_impossible():
    cfg = ClientConfig(endpoint="203.0.113.5:9090", use_tls=False)
    with pytest.raises(InsecureTransportError):
        AsyncTupagoPaymentClient(cfg, token_provider=object(), transport="rest")


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # silence
        pass

    def _send(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/api/clientes/"):
            self._send(
                200,
                {
                    "cliente": {
                        "id": 1,
                        "nombre": "Cliente Demo",
                        "claveInstitucion": "TUP01",
                        "key": "k1",
                    },
                    "catalogoInstitucion": {
                        "id": 7,
                        "claveInstitucion": "TUP01",
                        "nombreCorto": "Demo",
                    },
                },
            )
        elif self.path.startswith("/api/catalogo-institucions/clave/"):
            self._send(200, {"id": 7, "claveInstitucion": "TUP01", "nombreCorto": "Demo"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)
        if self.path == "/api/payments/msjpagoqrusuario":
            assert self.headers.get("Idempotency-Key"), "Idempotency-Key header required"
            self._send(
                200,
                {"TYP": 1, "CRY": "x", "v": {"DEV": "d"}, "ic": {"IDC": "i", "SER": 1, "ENC": "e"}},
            )
        elif self.path == "/api/payments/msjpagotelefonicousuario":
            self._send(200, {"detalleResultadoJson": json.dumps({"ok": True}), "edoPet": 1})
        elif self.path == "/api/payments/estatusmsjqrusuario":
            self._send(200, [{"id": "t1", "cc": "cc", "mt": 10.0, "e": 1}])
        elif self.path == "/api/payments/estatusmsjcobrotelefonicousuario":
            self._send(
                200, {"detalleResultado": {"id": "t2", "cc": "cc", "e": 1, "mt": 9.0}, "edoPet": 1}
            )
        else:
            self._send(404, {"error": "not found"})


@pytest.fixture
def rest_server():
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"127.0.0.1:{port}"
    server.shutdown()


def _client(port, token=None, allow_unauth=True):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=allow_unauth)
    return TupagoPaymentClient(cfg, token_provider=token, transport="rest")


def test_rest_get_cliente(rest_server):
    c = _client(rest_server)
    perfil = c.get_cliente(models.GetClienteRequest(id=1))
    assert perfil.cliente.nombre == "Cliente Demo"
    assert perfil.catalogo_institucion.clave_institucion == "TUP01"
    c.close()


def test_rest_get_catalogo(rest_server):
    c = _client(rest_server)
    cat = c.get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
    assert cat.clave_institucion == "TUP01"
    assert cat.nombre_corto == "Demo"
    c.close()


def test_rest_create_qr(rest_server):
    c = _client(rest_server)
    resp = c.create_qr(
        models.CreateQrRequest(
            id_cuenta=1,
            monto=10.5,
            concepto="c",
            referencia_numerica=2,
            nombre_cliente="n",
            email="e",
            tipo_pago=1,
            vigencia=30,
            idempotency_key="IDEK",
        )
    )
    assert isinstance(resp, object)
    assert resp.json  # JsonResponse.json populated
    c.close()


def test_rest_create_telefonico(rest_server):
    c = _client(rest_server)
    resp = c.create_telefonico(
        models.CreateTelefonicoRequest(
            key="k",
            monto=5.0,
            concepto="c",
            referencia_numerica=2,
            numero_celular="5512345678",
            vigencia=30,
            idempotency_key="IDEK2",
        )
    )
    assert resp.json
    c.close()


def test_rest_qr_status(rest_server):
    c = _client(rest_server)
    resp = c.get_qr_status(models.GetStatusRequest(idc="t1", id_cuenta=1))
    assert resp.json
    c.close()


def test_rest_telefonico_status(rest_server):
    c = _client(rest_server)
    resp = c.get_telefonico_status(models.GetStatusRequest(idc="t2", id_cuenta=1))
    assert resp.json
    c.close()


def test_rest_auth_fail_closed_by_default():
    cfg = ClientConfig(endpoint="127.0.0.1:1", use_tls=False)  # no allow_unauthenticated
    with pytest.raises(TupagoUnauthenticatedError):
        TupagoPaymentClient(cfg, transport="rest")


def test_rest_remote_plaintext_impossible():
    cfg = ClientConfig(endpoint="203.0.113.5:9090", use_tls=False)
    with pytest.raises(InsecureTransportError):
        TupagoPaymentClient(cfg, token_provider=object(), transport="rest")


def test_rest_create_network_ambiguous_is_unknown_outcome():
    # Point at a closed port: the transport error after dispatch must fail closed.
    cfg = ClientConfig(endpoint="127.0.0.1:1", use_tls=False, allow_unauthenticated=True)
    c = TupagoPaymentClient(cfg, transport="rest")
    with pytest.raises(TupagoUnknownOutcomeError):
        c.create_qr(
            models.CreateQrRequest(
                id_cuenta=1,
                monto=1.0,
                concepto="c",
                referencia_numerica=2,
                nombre_cliente="n",
                email="e",
                tipo_pago=1,
                vigencia=30,
                idempotency_key="X",
            )
        )
    c.close()
