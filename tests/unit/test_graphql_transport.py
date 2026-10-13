"""Phase C: GraphQL transport tests against an in-process mock of backend3 /graphql.

The mock mirrors the operations discovered in backend3 tupago.graphqls:
  query cliente(id: ID!)                         -> GetCliente
  query catalogoInstitucion(claveInstitucion)    -> GetCatalogo
  query pagoQrEstatusUsuario(idc: String!)       -> GetQrStatus
  query pagoTelefonicoEstatusUsuario(idc: String!) -> GetTelefonicoStatus
  mutation createPagoQr(input, idempotencyKey)   -> CreateQr
  mutation createPagoTelefonico(input, idempotencyKey) -> CreateTelefonico
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
    TupagoAlreadyExistsError,
    TupagoPaymentClient,
    TupagoUnauthenticatedError,
    TupagoUnknownOutcomeError,
    models,
)

MAX_INT64 = 9223372036854775807


async def _aclient(port, token=None, allow_unauth=True):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=allow_unauth)
    return AsyncTupagoPaymentClient(cfg, token_provider=token, transport="graphql")


def _client(port, token=None, allow_unauth=True):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=allow_unauth)
    return TupagoPaymentClient(cfg, token_provider=token, transport="graphql")


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

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        body = json.loads(raw.decode() or "{}")
        query = body.get("query", "")
        variables = body.get("variables", {})
        # Route by the GraphQL operation shape, not a bare substring: the cliente query
        # also contains "catalogoInstitucion", so the more specific patterns are checked first.
        if "createPagoQr" in query:
            assert variables.get("key"), "idempotencyKey required on createPagoQr"
            assert variables["input"]["idCuenta"] == str(MAX_INT64), "int64 must be exact string"
            self._send(200, {"data": {"createPagoQr": {"idc": "iqr"}}})
        elif "createPagoTelefonico" in query:
            assert variables.get("key"), "idempotencyKey required on createPagoTelefonico"
            self._send(200, {"data": {"createPagoTelefonico": {"idc": "itelef"}}})
        elif "pagoQrEstatusUsuario" in query:
            self._send(200, {"data": {"pagoQrEstatusUsuario": {"id": "t1", "estado": 1}}})
        elif "pagoTelefonicoEstatusUsuario" in query:
            self._send(200, {"data": {"pagoTelefonicoEstatusUsuario": {"id": "t2", "estado": 1}}})
        elif "cliente(" in query:
            self._send(
                200,
                {
                    "data": {
                        "cliente": {
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
                        }
                    }
                },
            )
        elif "catalogoInstitucion(claveInstitucion" in query:
            self._send(
                200,
                {
                    "data": {
                        "catalogoInstitucion": {
                            "id": 7,
                            "claveInstitucion": "TUP01",
                            "nombreCorto": "Demo",
                        }
                    }
                },
            )
        else:
            self._send(200, {"data": {}})


@pytest.fixture
def graphql_server():
    server = HTTPServer(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"127.0.0.1:{port}"
    server.shutdown()


# -- synchronous ----------------------------------------------------------------
def test_graphql_get_cliente(graphql_server):
    c = _client(graphql_server)
    perfil = c.get_cliente(models.GetClienteRequest(id=1))
    assert perfil.cliente.nombre == "Cliente Demo"
    assert perfil.catalogo_institucion.clave_institucion == "TUP01"
    c.close()


def test_graphql_get_catalogo(graphql_server):
    c = _client(graphql_server)
    cat = c.get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
    assert cat.clave_institucion == "TUP01"
    assert cat.nombre_corto == "Demo"
    c.close()


def test_graphql_qr_status(graphql_server):
    c = _client(graphql_server)
    resp = c.get_qr_status(models.GetStatusRequest(idc="t1", id_cuenta=1))
    assert resp.json
    c.close()


def test_graphql_telefonico_status(graphql_server):
    c = _client(graphql_server)
    resp = c.get_telefonico_status(models.GetStatusRequest(idc="t2", id_cuenta=1))
    assert resp.json
    c.close()


def test_graphql_create_qr_int64_exact(graphql_server):
    c = _client(graphql_server)
    resp = c.create_qr(
        models.CreateQrRequest(
            id_cuenta=MAX_INT64,
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
    c.close()


def test_graphql_create_telefonico(graphql_server):
    c = _client(graphql_server)
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


def test_graphql_auth_fail_closed_by_default():
    cfg = ClientConfig(endpoint="127.0.0.1:1", use_tls=False)
    with pytest.raises(TupagoUnauthenticatedError):
        TupagoPaymentClient(cfg, transport="graphql")


def test_graphql_remote_plaintext_impossible():
    cfg = ClientConfig(endpoint="203.0.113.5:9090", use_tls=False)
    with pytest.raises(InsecureTransportError):
        TupagoPaymentClient(cfg, token_provider=object(), transport="graphql")


def test_graphql_create_network_ambiguous_is_unknown_outcome():
    cfg = ClientConfig(endpoint="127.0.0.1:1", use_tls=False, allow_unauthenticated=True)
    c = TupagoPaymentClient(cfg, transport="graphql")
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


# -- asynchronous ---------------------------------------------------------------
async def test_graphql_async_get_cliente(graphql_server):
    c = await _aclient(graphql_server)
    perfil = await c.get_cliente(models.GetClienteRequest(id=1))
    assert perfil.cliente.nombre == "Cliente Demo"
    await c.aclose()


async def test_graphql_async_create_qr(graphql_server):
    c = await _aclient(graphql_server)
    resp = await c.create_qr(
        models.CreateQrRequest(
            id_cuenta=MAX_INT64,
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


# -- error mapping --------------------------------------------------------------
class _ErrorHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, payload):
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)
        self._send({"errors": [{"message": "dup", "extensions": {"code": "ALREADY_EXISTS"}}]})


@pytest.fixture
def graphql_error_server():
    server = HTTPServer(("127.0.0.1", 0), _ErrorHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"127.0.0.1:{port}"
    server.shutdown()


def test_graphql_already_exists_mapped(graphql_error_server):
    c = _client(graphql_error_server)
    with pytest.raises(TupagoAlreadyExistsError):
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
