"""Phase D / R1: cross-transport conformance (gRPC x REST x GraphQL).

All three transports implement the SAME transport-neutral protocol and return the SAME public
protobuf types (ClientePerfil / CatalogoInstitucion / JsonResponse). This module drives an
in-process gRPC PaymentService fake servant AND in-process REST and GraphQL mock servers, then
asserts that identical inputs produce identical public outputs across every transport.

The mocks mirror the REAL backend3 contract (anchor ``bbf06c02bda06221b2f214075a4bf2bdf3345eb0``):
the REST/GraphQL CreateQr returns ``{mensaje, emisor, banco}`` and the public ``JsonResponse``
carries ONLY the serialized ``mensaje``; CreateTelefonico returns the bare DTO; status uses the
by-cuenta routes/queries. The gRPC fake is defined to match. These tests FAIL on the pre-R1
baseline ``871d845`` and PASS only after the R1 fix.
"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer

import grpc
import pytest

from tupago3_sdk import ClientConfig, TupagoPaymentClient, models
from tupago3_sdk.generated import payment_pb2 as pb
from tupago3_sdk.generated.payment_pb2_grpc import (
    PaymentServiceServicer,
    add_PaymentServiceServicer_to_server,
)

# Canonical responses shared by all three transports.
CLIENTE = pb.ClientePerfil(
    cliente=pb.Cliente(id=1, nombre="Cliente Demo", clave_institucion="TUP01", key="k1"),
    catalogo_institucion=pb.CatalogoInstitucion(
        id=7, clave_institucion="TUP01", nombre_corto="Demo"
    ),
)
CATALOGO = pb.CatalogoInstitucion(id=7, clave_institucion="TUP01", nombre_corto="Demo")

# C1 CreateQr: public JsonResponse carries ONLY the serialized mensaje.
QR_MENSAJE = {"idc": "iqr-1", "estado": "PENDIENTE", "folio": "F1"}
QR_BODY = {"mensaje": QR_MENSAJE, "emisor": "BANCO_TUPAGO", "banco": "BBVA"}
CREATE_QR_JSON = json.dumps(QR_MENSAJE)

# C2 CreateTelefonico: public JsonResponse carries the serialized bare DTO.
TELEF_DTO = {"id": 5, "key": "k", "monto": 5.0, "estado": "PENDIENTE"}
CREATE_TELEF_JSON = json.dumps(TELEF_DTO)

# C3 / C4 status payloads.
QR_STATUS = {"estado": "PENDIENTE", "idc": "t1", "monto": 10.0}
TELEF_STATUS = {"estado": "PENDIENTE", "idc": "t2", "monto": 9.0}
STATUS_JSON = json.dumps(QR_STATUS)
TELEF_STATUS_JSON = json.dumps(TELEF_STATUS)


# --------------------------------------------------------------------------- #
# gRPC fake servant
# --------------------------------------------------------------------------- #
class _FakeServicer(PaymentServiceServicer):
    def GetCliente(self, request, context):
        return CLIENTE

    def GetCatalogo(self, request, context):
        return CATALOGO

    def GetQrStatus(self, request, context):
        return pb.JsonResponse(json=STATUS_JSON)

    def GetTelefonicoStatus(self, request, context):
        return pb.JsonResponse(json=TELEF_STATUS_JSON)

    def CreateQr(self, request, context):
        # Matches REST/GraphQL: public JsonResponse carries only the mensaje.
        return pb.JsonResponse(json=CREATE_QR_JSON)

    def CreateTelefonico(self, request, context):
        return pb.JsonResponse(json=CREATE_TELEF_JSON)


@pytest.fixture
def grpc_server():
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    add_PaymentServiceServicer_to_server(_FakeServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield f"127.0.0.1:{port}"
    server.stop(None)


def _grpc_client(port):
    ch = grpc.insecure_channel(port)
    return TupagoPaymentClient._from_channel(ch)


# --------------------------------------------------------------------------- #
# REST mock (mirrors backend3 REST routes)
# --------------------------------------------------------------------------- #
class _RestServer(HTTPServer):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.calls: list[tuple[str, str]] = []


class _RestHandler(BaseHTTPRequestHandler):
    server: _RestServer

    def log_message(self, *a):
        pass

    def _send(self, status, payload=None, raw=None):
        if raw is not None:
            body = raw.encode() if isinstance(raw, str) else raw
        else:
            body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_GET(self):
        self.server.calls.append(("GET", self.path))
        if self.path.startswith("/api/clientes/"):
            self._send(
                200,
                {
                    "id": 1,
                    "nombre": "Cliente Demo",
                    "claveInstitucion": "TUP01",
                    "key": "k1",
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
        self.server.calls.append(("POST", self.path))
        if self.path == "/api/payments/msjpagoqrusuario":
            assert self.headers.get("Idempotency-Key")
            self._send(200, QR_BODY)
        elif self.path == "/api/payments/msjpagotelefonicousuario":
            assert self.headers.get("Idempotency-Key")
            self._send(200, TELEF_DTO)
        elif self.path == "/api/payments/estatusmsjqr":
            self._send(200, QR_STATUS)
        elif self.path == "/api/payments/estatusmsjcobrotelefonico":
            self._send(200, TELEF_STATUS)
        else:
            self._send(404, {"error": "not found"})


@pytest.fixture
def rest_server():
    server = _RestServer(("127.0.0.1", 0), _RestHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{port}", server
    server.shutdown()


def _rest_client(port):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=True)
    return TupagoPaymentClient(cfg, transport="rest")


# --------------------------------------------------------------------------- #
# GraphQL mock (mirrors backend3 tupago.graphqls)
# --------------------------------------------------------------------------- #
class _GraphqlServer(HTTPServer):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.queries: list[str] = []


class _GraphqlHandler(BaseHTTPRequestHandler):
    server: _GraphqlServer

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
        raw = self.rfile.read(length) if length else b"{}"
        body = json.loads(raw.decode() or "{}")
        query = body.get("query", "")
        self.server.queries.append(query)
        if "createPagoQr" in query:
            self._send({"data": {"createPagoQr": QR_BODY}})
        elif "createPagoTelefonico" in query:
            self._send({"data": {"createPagoTelefonico": TELEF_DTO}})
        elif "pagoQrEstatusByCuenta" in query:
            self._send({"data": {"pagoQrEstatusByCuenta": QR_STATUS}})
        elif "pagoTelefonicoEstatusByCuenta" in query:
            self._send({"data": {"pagoTelefonicoEstatusByCuenta": TELEF_STATUS}})
        elif "cliente(" in query:
            self._send(
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
                }
            )
        elif "catalogoInstitucion(claveInstitucion" in query:
            self._send(
                {
                    "data": {
                        "catalogoInstitucion": {
                            "id": 7,
                            "claveInstitucion": "TUP01",
                            "nombreCorto": "Demo",
                        }
                    }
                }
            )
        else:
            self._send({"data": {}})


@pytest.fixture
def graphql_server():
    server = _GraphqlServer(("127.0.0.1", 0), _GraphqlHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{port}", server
    server.shutdown()


def _graphql_client(port):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=True)
    return TupagoPaymentClient(cfg, transport="graphql")


# --------------------------------------------------------------------------- #
# Conformance assertions
# --------------------------------------------------------------------------- #
def _json_equal(a: str, b: str) -> bool:
    return json.loads(a) == json.loads(b)


def test_cross_transport_get_cliente(grpc_server, rest_server, graphql_server):
    rest_port, rserver = rest_server
    graphql_port, gserver = graphql_server
    want = CLIENTE.SerializeToString()
    got = [
        _grpc_client(grpc_server).get_cliente(models.GetClienteRequest(id=1)).SerializeToString(),
        _rest_client(rest_port).get_cliente(models.GetClienteRequest(id=1)).SerializeToString(),
        _graphql_client(graphql_port)
        .get_cliente(models.GetClienteRequest(id=1))
        .SerializeToString(),
    ]
    assert got == [want, want, want]
    # REST must have used the canonical routes (PY-BACKEND-01).
    rpaths = [p for _, p in rserver.calls]
    assert rpaths[0].startswith("/api/clientes/")
    assert any(p.startswith("/api/catalogo-institucions/clave/") for p in rpaths)
    # GraphQL must have used the ByCuenta-free cliente query.
    assert any("cliente(" in q for q in gserver.queries)


def test_cross_transport_get_catalogo(grpc_server, rest_server, graphql_server):
    rest_port, rserver = rest_server
    graphql_port, gserver = graphql_server
    want = CATALOGO.SerializeToString()
    got = [
        _grpc_client(grpc_server)
        .get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
        .SerializeToString(),
        _rest_client(rest_port)
        .get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
        .SerializeToString(),
        _graphql_client(graphql_port)
        .get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
        .SerializeToString(),
    ]
    assert got == [want, want, want]


def test_cross_transport_get_qr_status(grpc_server, rest_server, graphql_server):
    rest_port, rserver = rest_server
    graphql_port, gserver = graphql_server
    grpc_c = _grpc_client(grpc_server).get_qr_status(models.GetStatusRequest(idc="t1", id_cuenta=1))
    rest_c = _rest_client(rest_port).get_qr_status(models.GetStatusRequest(idc="t1", id_cuenta=1))
    gql_c = _graphql_client(graphql_port).get_qr_status(
        models.GetStatusRequest(idc="t1", id_cuenta=1)
    )
    assert _json_equal(grpc_c.json, rest_c.json) and _json_equal(grpc_c.json, gql_c.json)
    # PY-BACKEND-03: canonical by-cuenta route.
    assert any(p == "/api/payments/estatusmsjqr" for _, p in rserver.calls)
    assert any("pagoQrEstatusByCuenta" in q for q in gserver.queries)


def test_cross_transport_get_telefonico_status(grpc_server, rest_server, graphql_server):
    rest_port, rserver = rest_server
    graphql_port, gserver = graphql_server
    grpc_c = _grpc_client(grpc_server).get_telefonico_status(
        models.GetStatusRequest(idc="t2", id_cuenta=1)
    )
    rest_c = _rest_client(rest_port).get_telefonico_status(
        models.GetStatusRequest(idc="t2", id_cuenta=1)
    )
    gql_c = _graphql_client(graphql_port).get_telefonico_status(
        models.GetStatusRequest(idc="t2", id_cuenta=1)
    )
    assert _json_equal(grpc_c.json, rest_c.json) and _json_equal(grpc_c.json, gql_c.json)
    # PY-BACKEND-04: canonical by-cuenta route.
    assert any(p == "/api/payments/estatusmsjcobrotelefonico" for _, p in rserver.calls)
    assert any("pagoTelefonicoEstatusByCuenta" in q for q in gserver.queries)


def test_cross_transport_create_qr(grpc_server, rest_server, graphql_server):
    rest_port, rserver = rest_server
    graphql_port, gserver = graphql_server
    req = models.CreateQrRequest(
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
    grpc_c = _grpc_client(grpc_server).create_qr(req)
    rest_c = _rest_client(rest_port).create_qr(req)
    gql_c = _graphql_client(graphql_port).create_qr(req)
    # All three carry ONLY the serialized mensaje (PY-BACKEND-02).
    assert _json_equal(grpc_c.json, rest_c.json) and _json_equal(grpc_c.json, gql_c.json)
    assert json.loads(gql_c.json) == QR_MENSAJE


def test_cross_transport_create_telefonico(grpc_server, rest_server, graphql_server):
    rest_port, rserver = rest_server
    graphql_port, gserver = graphql_server
    req = models.CreateTelefonicoRequest(
        key="k",
        monto=5.0,
        concepto="c",
        referencia_numerica=2,
        numero_celular="5512345678",
        vigencia=30,
        idempotency_key="IDEK2",
    )
    grpc_c = _grpc_client(grpc_server).create_telefonico(req)
    rest_c = _rest_client(rest_port).create_telefonico(req)
    gql_c = _graphql_client(graphql_port).create_telefonico(req)
    # All three carry the serialized bare DTO (PY-BACKEND-02).
    assert _json_equal(grpc_c.json, rest_c.json) and _json_equal(grpc_c.json, gql_c.json)
    assert json.loads(gql_c.json) == TELEF_DTO
