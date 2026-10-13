"""Phase D: cross-transport conformance (gRPC x REST x GraphQL).

All three transports implement the SAME transport-neutral protocol and return the
SAME public protobuf types (ClientePerfil / CatalogoInstitucion / JsonResponse).
This module drives an in-process gRPC PaymentService fake servant AND in-process
REST and GraphQL mock servers, then asserts that identical inputs produce
identical public outputs across every transport — proving behavioral equivalence
of gRPC, REST and GraphQL at the client boundary.

The gRPC fake, REST mock and GraphQL mock return payloads that deserialize to the
SAME protobuf objects, so the public ClientePerfil/CatalogoInstitucion are compared
by serialized bytes and JsonResponse.json by parsed JSON (whitespace-insensitive).
"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer

import grpc
import pytest

from tupago3_sdk import (
    ClientConfig,
    TupagoPaymentClient,
    models,
)
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
CREATE_JSON = json.dumps({"idc": "iqr", "estado": 1})
STATUS_JSON = json.dumps({"estado": "PENDIENTE", "idc": "t1"})
TELEF_STATUS_JSON = json.dumps({"estado": "PENDIENTE", "idc": "t2"})


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
        return pb.JsonResponse(json=CREATE_JSON)

    def CreateTelefonico(self, request, context):
        return pb.JsonResponse(json=CREATE_JSON)


@pytest.fixture
def grpc_server():
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    add_PaymentServiceServicer_to_server(_FakeServicer(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    yield f"127.0.0.1:{port}"
    server.stop(None)


def _grpc_client(port):
    # Loopback insecure channel is permitted by validate_transport; _from_channel is the
    # test-only seam that attaches to a pre-built channel (no public channel injection).
    ch = grpc.insecure_channel(port)
    return TupagoPaymentClient._from_channel(ch)


# --------------------------------------------------------------------------- #
# REST mock (mirrors backend3 REST routes)
# --------------------------------------------------------------------------- #
class _RestHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
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
            assert self.headers.get("Idempotency-Key")
            self._send(200, json.loads(CREATE_JSON))
        elif self.path == "/api/payments/msjpagotelefonicousuario":
            assert self.headers.get("Idempotency-Key")
            self._send(200, json.loads(CREATE_JSON))
        elif self.path == "/api/payments/estatusmsjqrusuario":
            self._send(200, json.loads(STATUS_JSON))
        elif self.path == "/api/payments/estatusmsjcobrotelefonicousuario":
            self._send(200, json.loads(TELEF_STATUS_JSON))
        else:
            self._send(404, {"error": "not found"})


@pytest.fixture
def rest_server():
    server = HTTPServer(("127.0.0.1", 0), _RestHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{port}"
    server.shutdown()


def _rest_client(port):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=True)
    return TupagoPaymentClient(cfg, transport="rest")


# --------------------------------------------------------------------------- #
# GraphQL mock (mirrors backend3 tupago.graphqls)
# --------------------------------------------------------------------------- #
class _GraphqlHandler(BaseHTTPRequestHandler):
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
        variables = body.get("variables", {})
        if "createPagoQr" in query:
            assert variables.get("key")
            self._send({"data": {"createPagoQr": json.loads(CREATE_JSON)}})
        elif "createPagoTelefonico" in query:
            assert variables.get("key")
            self._send({"data": {"createPagoTelefonico": json.loads(CREATE_JSON)}})
        elif "pagoQrEstatusUsuario" in query:
            self._send({"data": {"pagoQrEstatusUsuario": json.loads(STATUS_JSON)}})
        elif "pagoTelefonicoEstatusUsuario" in query:
            self._send({"data": {"pagoTelefonicoEstatusUsuario": json.loads(TELEF_STATUS_JSON)}})
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
    server = HTTPServer(("127.0.0.1", 0), _GraphqlHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{port}"
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
    want = CLIENTE.SerializeToString()
    got = [
        _grpc_client(grpc_server).get_cliente(models.GetClienteRequest(id=1)).SerializeToString(),
        _rest_client(rest_server).get_cliente(models.GetClienteRequest(id=1)).SerializeToString(),
        _graphql_client(graphql_server)
        .get_cliente(models.GetClienteRequest(id=1))
        .SerializeToString(),
    ]
    assert got == [want, want, want]


def test_cross_transport_get_catalogo(grpc_server, rest_server, graphql_server):
    want = CATALOGO.SerializeToString()
    got = [
        _grpc_client(grpc_server)
        .get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
        .SerializeToString(),
        _rest_client(rest_server)
        .get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
        .SerializeToString(),
        _graphql_client(graphql_server)
        .get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
        .SerializeToString(),
    ]
    assert got == [want, want, want]


def test_cross_transport_get_qr_status(grpc_server, rest_server, graphql_server):
    grpc_c = _grpc_client(grpc_server).get_qr_status(models.GetStatusRequest(idc="t1", id_cuenta=1))
    rest_c = _rest_client(rest_server).get_qr_status(models.GetStatusRequest(idc="t1", id_cuenta=1))
    gql_c = _graphql_client(graphql_server).get_qr_status(
        models.GetStatusRequest(idc="t1", id_cuenta=1)
    )
    assert _json_equal(grpc_c.json, rest_c.json) and _json_equal(grpc_c.json, gql_c.json)


def test_cross_transport_get_telefonico_status(grpc_server, rest_server, graphql_server):
    grpc_c = _grpc_client(grpc_server).get_telefonico_status(
        models.GetStatusRequest(idc="t2", id_cuenta=1)
    )
    rest_c = _rest_client(rest_server).get_telefonico_status(
        models.GetStatusRequest(idc="t2", id_cuenta=1)
    )
    gql_c = _graphql_client(graphql_server).get_telefonico_status(
        models.GetStatusRequest(idc="t2", id_cuenta=1)
    )
    assert _json_equal(grpc_c.json, rest_c.json) and _json_equal(grpc_c.json, gql_c.json)


def test_cross_transport_create_qr(grpc_server, rest_server, graphql_server):
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
    rest_c = _rest_client(rest_server).create_qr(req)
    gql_c = _graphql_client(graphql_server).create_qr(req)
    assert _json_equal(grpc_c.json, rest_c.json) and _json_equal(grpc_c.json, gql_c.json)


def test_cross_transport_create_telefonico(grpc_server, rest_server, graphql_server):
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
    rest_c = _rest_client(rest_server).create_telefonico(req)
    gql_c = _graphql_client(graphql_server).create_telefonico(req)
    assert _json_equal(grpc_c.json, rest_c.json) and _json_equal(grpc_c.json, gql_c.json)
