"""Phase C / R1: GraphQL transport tests against an in-process mock of backend3 /graphql.

The mock mirrors the operations in backend3 ``tupago.graphqls`` (anchor
``bbf06c02bda06221b2f214075a4bf2bdf3345eb0``):

  query cliente(id: ID!)                              -> GetCliente (ClientePerfil)
  query catalogoInstitucion(claveInstitucion)         -> GetCatalogo
  query pagoQrEstatusByCuenta(idc, idCuenta: ID!)     -> C3 (by-cuenta)
  query pagoTelefonicoEstatusByCuenta(idc, idCuenta)   -> C4 (by-cuenta)
  mutation createPagoQr(input, idempotencyKey)        -> {mensaje, emisor, banco}
  mutation createPagoTelefonico(input, idempotencyKey) -> bare DTO

These tests assert the BACKEND-FAITHFUL contract and FAIL on the pre-R1 baseline
``871d845`` (which used ``pagoQrEstatusUsuario``/``pagoTelefonicoEstatusUsuario`` queries and
returned the whole create body), passing only after the R1 fix.
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
    TupagoUnavailableError,
    TupagoUnknownOutcomeError,
    models,
)

MAX_INT64 = 9223372036854775807

QR_MENSAJE = {"idc": "iqr-1", "estado": "PENDIENTE", "folio": "F1"}
QR_BODY = {"mensaje": QR_MENSAJE, "emisor": "BANCO_TUPAGO", "banco": "BBVA"}
TELEF_DTO = {"id": 5, "key": "k", "monto": 5.0, "estado": "PENDIENTE"}
QR_STATUS = {"estado": "PENDIENTE", "idc": "t1", "monto": 10.0}
TELEF_STATUS = {"estado": "PENDIENTE", "idc": "t2", "monto": 9.0}


def _client(port, token=None, allow_unauth=True):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=allow_unauth)
    return TupagoPaymentClient(cfg, token_provider=token, transport="graphql")


async def _aclient(port, token=None, allow_unauth=True):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=allow_unauth)
    return AsyncTupagoPaymentClient(cfg, token_provider=token, transport="graphql")


class _Server(HTTPServer):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.queries: list[tuple[str, dict]] = []


class _Handler(BaseHTTPRequestHandler):
    server: _Server

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
        self.server.queries.append((query, variables))
        # Route by the specific GraphQL operation shape.
        if "createPagoQr" in query:
            assert variables.get("key"), "idempotencyKey required on createPagoQr"
            assert variables["input"]["idCuenta"] == str(MAX_INT64), "int64 must be exact string"
            self._send({"data": {"createPagoQr": QR_BODY}})
        elif "createPagoTelefonico" in query:
            assert variables.get("key"), "idempotencyKey required on createPagoTelefonico"
            self._send({"data": {"createPagoTelefonico": TELEF_DTO}})
        elif "pagoQrEstatusByCuenta" in query:
            assert "idCuenta" in variables and "idc" in variables
            self._send({"data": {"pagoQrEstatusByCuenta": QR_STATUS}})
        elif "pagoTelefonicoEstatusByCuenta" in query:
            assert "idCuenta" in variables and "idc" in variables
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
    server = _Server(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"127.0.0.1:{port}", server
    server.shutdown()


# -- PY-BACKEND-05: status uses the ByCuenta queries -----------------------------------
def test_graphql_qr_status_uses_by_cuenta(graphql_server):
    port, server = graphql_server
    c = _client(port)
    resp = c.get_qr_status(models.GetStatusRequest(idc="t1", id_cuenta=1))
    assert json.loads(resp.json) == QR_STATUS
    assert any("pagoQrEstatusByCuenta" in q for q, _ in server.queries)
    assert server.queries[-1][1]["idCuenta"] == "1"  # ID string form
    c.close()


def test_graphql_telefonico_status_uses_by_cuenta(graphql_server):
    port, server = graphql_server
    c = _client(port)
    resp = c.get_telefonico_status(models.GetStatusRequest(idc="t2", id_cuenta=1))
    assert json.loads(resp.json) == TELEF_STATUS
    assert any("pagoTelefonicoEstatusByCuenta" in q for q, _ in server.queries)
    c.close()


# -- PY-BACKEND-02: CreateQr returns ONLY mensaje; CreateTelefonico returns bare DTO -------
def test_graphql_create_qr_returns_only_mensaje(graphql_server):
    port, _ = graphql_server
    c = _client(port)
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
    assert resp.json == json.dumps(QR_MENSAJE)
    c.close()


def test_graphql_create_telefonico_returns_bare_dto(graphql_server):
    port, _ = graphql_server
    c = _client(port)
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
    assert resp.json == json.dumps(TELEF_DTO)
    c.close()


# -- read / cliente / catalogo --------------------------------------------------------
def test_graphql_get_cliente(graphql_server):
    port, _ = graphql_server
    c = _client(port)
    perfil = c.get_cliente(models.GetClienteRequest(id=1))
    assert perfil.cliente.nombre == "Cliente Demo"
    assert perfil.catalogo_institucion.clave_institucion == "TUP01"
    c.close()


def test_graphql_get_catalogo(graphql_server):
    port, _ = graphql_server
    c = _client(port)
    cat = c.get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
    assert cat.clave_institucion == "TUP01"
    assert cat.nombre_corto == "Demo"
    c.close()


async def test_graphql_async_get_cliente(graphql_server):
    port, _ = graphql_server
    c = await _aclient(port)
    perfil = await c.get_cliente(models.GetClienteRequest(id=1))
    assert perfil.cliente.nombre == "Cliente Demo"
    await c.aclose()


# -- R1.6: transient read errors retry, then raise TupagoUnavailableError --------------
class _TransientHandler(BaseHTTPRequestHandler):
    server: _Server

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
        self.server.queries.append(("query", {}))
        self._send({"errors": [{"message": "down", "extensions": {"code": "UNAVAILABLE"}}]})


@pytest.fixture
def graphql_transient_server():
    server = _Server(("127.0.0.1", 0), _TransientHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"127.0.0.1:{port}", server
    server.shutdown()


def test_graphql_read_transient_unavailable_retries_then_raises(graphql_transient_server):
    port, server = graphql_transient_server
    c = _client(port)
    with pytest.raises(TupagoUnavailableError):
        c.get_cliente(models.GetClienteRequest(id=1))
    # RetryPolicy.max_attempts defaults to 3 -> exactly 3 attempts, no more.
    assert len(server.queries) == 3
    c.close()


# -- R1.17 / ambiguity: create errors are deterministic only for ALREADY_EXISTS --------
class _ErrorHandler(BaseHTTPRequestHandler):
    server: _Server

    def __init__(self, request, client_address, server, code="UNAVAILABLE"):
        self._code = code
        super().__init__(request, client_address, server)

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
        self._send({"errors": [{"message": "err", "extensions": {"code": self._code}}]})


def _error_server(code):
    server = _Server(("127.0.0.1", 0), lambda *a, **k: _ErrorHandler(*a, code=code, **k))
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"127.0.0.1:{port}"


def test_graphql_create_already_exists_is_deterministic():
    server, port = _error_server("ALREADY_EXISTS")
    try:
        c = _client(port)
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
    finally:
        server.shutdown()


def test_graphql_create_transient_error_is_unknown_outcome():
    # A transient GraphQL error on create is ambiguous (the mutation may have executed).
    server, port = _error_server("UNAVAILABLE")
    try:
        c = _client(port)
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
    finally:
        server.shutdown()


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


# -- R1.19: blank bearer fails closed pre-dispatch ------------------------------------
def test_graphql_auth_fail_closed_by_default():
    cfg = ClientConfig(endpoint="127.0.0.1:1", use_tls=False)
    with pytest.raises(TupagoUnauthenticatedError):
        TupagoPaymentClient(cfg, transport="graphql")


def test_graphql_remote_plaintext_impossible():
    cfg = ClientConfig(endpoint="203.0.113.5:9090", use_tls=False)
    with pytest.raises(InsecureTransportError):
        TupagoPaymentClient(cfg, token_provider=object(), transport="graphql")


# -- R1.7 / R1.8 / R1.17: create ambiguity fails closed as UnknownOutcome --------------
class _CreateAdversarialHandler(BaseHTTPRequestHandler):
    server: _Server

    def __init__(self, request, client_address, server, mode="redirect"):
        self._mode = mode
        super().__init__(request, client_address, server)

    def log_message(self, *a):
        pass

    def _send(self, status, payload=None, raw=None):
        if raw is not None:
            body = raw.encode()
        else:
            body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def do_POST(self):
        self.server.queries.append(("create", self._mode))
        if self._mode == "redirect":
            self.send_response(302)
            self.send_header("Location", "/graphql")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif self._mode == "malformed":
            self._send(200, raw="not json at all")
        elif self._mode == "err503":
            self._send(503, {"error": "unavailable"})
        else:
            self._send(200, {"data": {"createPagoQr": QR_BODY}})


def _create_adversarial_server(mode):
    server = _Server(
        ("127.0.0.1", 0),
        lambda *a, **k: _CreateAdversarialHandler(*a, mode=mode, **k),
    )
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"127.0.0.1:{port}"


def test_graphql_create_redirect_is_unknown_outcome():
    server, port = _create_adversarial_server("redirect")
    try:
        c = _client(port)
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
    finally:
        server.shutdown()


def test_graphql_create_malformed_is_unknown_outcome():
    server, port = _create_adversarial_server("malformed")
    try:
        c = _client(port)
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
    finally:
        server.shutdown()


def test_graphql_create_single_attempt_no_retry():
    server, port = _create_adversarial_server("err503")
    try:
        c = _client(port)
        # A 503 create is a typed error, but it must NOT be retried (single attempt).
        with pytest.raises(TupagoUnavailableError):
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
        assert len([q for q in server.queries if q[0] == "create"]) == 1
        c.close()
    finally:
        server.shutdown()
