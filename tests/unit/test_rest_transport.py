"""Phase B / R1: REST transport tests against an in-process mock of backend3 REST routes.

The mock mirrors the REAL backend3 routes confirmed read-only at anchor
``bbf06c02bda06221b2f214075a4bf2bdf3345eb0``:

  GET  /api/clientes/{id}                         -> ClienteDTO (NOT wrapped perfil)
  GET  /api/catalogo-institucions/clave/{clave}   -> CatalogoInstitucionDTO
  POST /api/payments/msjpagoqrusuario             -> {mensaje, emisor, banco}  (CreateQr)
  POST /api/payments/msjpagotelefonicousuario     -> bare DTO                 (CreateTelefonico)
  POST /api/payments/estatusmsjqr                 -> status JSON (C3, by-cuenta)
  POST /api/payments/estatusmsjcobrotelefonico    -> status JSON (C4, by-cuenta)

These tests intentionally assert the BACKEND-FAITHFUL contract (composition, mensaje-only
CreateQr, bare-DTO CreateTelefonico, by-cuenta status routes, fail-closed ambiguity). They
FAIL on the pre-R1 baseline ``871d845`` (which used ``@/`` routes, whole-body CreateQr, and
``pagoQrEstatusUsuario``-style status routes) and PASS only after the R1 fix.
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
    TupagoPendingReconciliationError,
    TupagoUnauthenticatedError,
    TupagoUnavailableError,
    TupagoUnknownOutcomeError,
    models,
)


class _Server(HTTPServer):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.calls: list[tuple[str, str]] = []
        self.create_post_count = 0


QR_MENSAJE = {"idc": "iqr-1", "estado": "PENDIENTE", "folio": "F1"}
QR_BODY = {"mensaje": QR_MENSAJE, "emisor": "BANCO_TUPAGO", "banco": "BBVA"}
TELEF_DTO = {"id": 5, "key": "k", "monto": 5.0, "estado": "PENDIENTE"}
QR_STATUS = {"estado": "PENDIENTE", "idc": "t1", "monto": 10.0}
TELEF_STATUS = {"estado": "PENDIENTE", "idc": "t2", "monto": 9.0}


class _Handler(BaseHTTPRequestHandler):
    server: _Server

    def log_message(self, *a):  # silence
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
            cid = self.path.rsplit("/", 1)[-1]
            if cid == "2":
                # claveInstitucion absent -> SDK must NOT call the catalogo route.
                self._send(
                    200,
                    {"id": 2, "nombre": "Sin Clave", "claveInstitucion": "", "key": "k2"},
                )
            else:
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
            clave = self.path.rsplit("/", 1)[-1]
            assert clave == "TUP01", f"unexpected catalogo clave: {clave}"
            self._send(200, {"id": 7, "claveInstitucion": "TUP01", "nombreCorto": "Demo"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        body = json.loads(raw.decode() or "{}")
        self.server.calls.append(("POST", self.path))
        if self.path == "/api/payments/msjpagoqrusuario":
            self.server.create_post_count += 1
            assert self.headers.get("Idempotency-Key"), "Idempotency-Key header required"
            self._send(200, QR_BODY)
        elif self.path == "/api/payments/msjpagotelefonicousuario":
            self.server.create_post_count += 1
            assert self.headers.get("Idempotency-Key"), "Idempotency-Key header required"
            self._send(200, TELEF_DTO)
        elif self.path == "/api/payments/estatusmsjqr":
            assert "idCuenta" in body and "idc" in body, f"by-cuenta body required: {body}"
            self._send(200, QR_STATUS)
        elif self.path == "/api/payments/estatusmsjcobrotelefonico":
            assert "idCuenta" in body and "idMsjdeCobro" in body, f"by-cuenta body required: {body}"
            self._send(200, TELEF_STATUS)
        else:
            self._send(404, {"error": "not found"})


@pytest.fixture
def rest_server():
    server = _Server(("127.0.0.1", 0), _Handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"127.0.0.1:{port}", server
    server.shutdown()


def _client(port, token=None, allow_unauth=True):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=allow_unauth)
    return TupagoPaymentClient(cfg, token_provider=token, transport="rest")


async def _aclient(port, token=None, allow_unauth=True):
    cfg = ClientConfig(endpoint=port, use_tls=False, allow_unauthenticated=allow_unauth)
    return AsyncTupagoPaymentClient(cfg, token_provider=token, transport="rest")


# -- PY-BACKEND-01: GetCliente composes perfil from ClienteDTO + catalogo --------------
def test_rest_get_cliente_composes_perfil(rest_server):
    port, server = rest_server
    c = _client(port)
    perfil = c.get_cliente(models.GetClienteRequest(id=1))
    assert perfil.cliente.nombre == "Cliente Demo"
    assert perfil.cliente.clave_institucion == "TUP01"
    # The composition MUST hit both routes (PY-BACKEND-01: cliente first, then catalogo).
    paths = [p for _, p in server.calls]
    assert paths[0].startswith("/api/clientes/")
    assert any(p.startswith("/api/catalogo-institucions/clave/") for p in paths)
    c.close()


def test_rest_get_cliente_no_catalogo_when_clave_absent(rest_server):
    port, server = rest_server
    c = _client(port)
    perfil = c.get_cliente(models.GetClienteRequest(id=2))
    assert perfil.cliente.nombre == "Sin Clave"
    # No catalogo route must be called when claveInstitucion is empty.
    assert not any(p.startswith("/api/catalogo-institucions/clave/") for _, p in server.calls)
    assert perfil.catalogo_institucion.clave_institucion == ""
    c.close()


async def test_rest_async_get_cliente_composes_perfil(rest_server):
    port, server = rest_server
    c = await _aclient(port)
    perfil = await c.get_cliente(models.GetClienteRequest(id=1))
    assert perfil.cliente.nombre == "Cliente Demo"
    assert perfil.catalogo_institucion.clave_institucion == "TUP01"
    await c.aclose()


def test_rest_get_catalogo(rest_server):
    port, _ = rest_server
    c = _client(port)
    cat = c.get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
    assert cat.clave_institucion == "TUP01"
    assert cat.nombre_corto == "Demo"
    c.close()


# -- PY-BACKEND-02: CreateQr returns ONLY mensaje; CreateTelefonico returns bare DTO -------
def test_rest_create_qr_returns_only_mensaje(rest_server):
    port, _ = rest_server
    c = _client(port)
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
    # Public JsonResponse.json carries ONLY the serialized mensaje (not the wrapper).
    assert resp.json == json.dumps(QR_MENSAJE)
    c.close()


def test_rest_create_telefonico_returns_bare_dto(rest_server):
    port, _ = rest_server
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


# -- PY-BACKEND-03 / 04: status uses the by-cuenta routes -------------------------------
def test_rest_qr_status_uses_by_cuenta_route(rest_server):
    port, server = rest_server
    c = _client(port)
    resp = c.get_qr_status(models.GetStatusRequest(idc="t1", id_cuenta=1))
    assert json.loads(resp.json) == QR_STATUS
    assert any(p == "/api/payments/estatusmsjqr" for _, p in server.calls)
    c.close()


def test_rest_telefonico_status_uses_by_cuenta_route(rest_server):
    port, server = rest_server
    c = _client(port)
    resp = c.get_telefonico_status(models.GetStatusRequest(idc="t2", id_cuenta=1))
    assert json.loads(resp.json) == TELEF_STATUS
    assert any(p == "/api/payments/estatusmsjcobrotelefonico" for _, p in server.calls)
    c.close()


# -- R1.5: read transport error normalizes to TupagoUnavailableError --------------------
def test_rest_read_transport_error_is_unavailable():
    cfg = ClientConfig(endpoint="127.0.0.1:1", use_tls=False, allow_unauthenticated=True)
    c = TupagoPaymentClient(cfg, transport="rest")
    with pytest.raises(TupagoUnavailableError):
        c.get_cliente(models.GetClienteRequest(id=1))
    c.close()


# -- R1.7: financial create is single-attempt (no blind retry) --------------------------
def test_rest_create_qr_single_attempt_no_retry(rest_server):
    port, server = rest_server
    c = _client(port)
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
    assert resp.json == json.dumps(QR_MENSAJE)
    # Exactly one POST must have reached the create endpoint — never a blind retry.
    assert server.create_post_count == 1
    c.close()


class _PendingHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        self.send_response(202)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"type":"payment-pending-reconciliation"}')


def test_rest_create_202_is_pending_reconciliation():
    server = _Server(("127.0.0.1", 0), _PendingHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        c = _client(f"127.0.0.1:{port}")
        with pytest.raises(TupagoPendingReconciliationError):
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
                    idempotency_key="PENDING",
                )
            )
        c.close()
        assert server.create_post_count == 1
    finally:
        server.shutdown()


@pytest.mark.asyncio
async def test_rest_async_create_202_is_pending_reconciliation():
    server = _Server(("127.0.0.1", 0), _PendingHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        c = await _aclient(f"127.0.0.1:{port}")
        with pytest.raises(TupagoPendingReconciliationError):
            await c.create_qr(
                models.CreateQrRequest(
                    id_cuenta=1,
                    monto=1.0,
                    concepto="c",
                    referencia_numerica=2,
                    nombre_cliente="n",
                    email="e",
                    tipo_pago=1,
                    vigencia=30,
                    idempotency_key="PENDING-ASYNC",
                )
            )
        await c.close()
        assert server.create_post_count == 1
    finally:
        server.shutdown()


# -- R1.17 / R1.8: create ambiguity fails closed as UnknownOutcome ----------------------
def test_rest_create_network_ambiguous_is_unknown_outcome():
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


def test_rest_create_redirect_is_unknown_outcome():
    server = _Server(("127.0.0.1", 0), _RedirectHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        c = _client(f"127.0.0.1:{port}")
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


def test_rest_create_malformed_is_unknown_outcome():
    server = _Server(("127.0.0.1", 0), _MalformedHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        c = _client(f"127.0.0.1:{port}")
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


# -- R1.19: blank bearer fails closed pre-dispatch --------------------------------------
def test_rest_auth_fail_closed_by_default():
    cfg = ClientConfig(endpoint="127.0.0.1:1", use_tls=False)  # no allow_unauthenticated
    with pytest.raises(TupagoUnauthenticatedError):
        TupagoPaymentClient(cfg, transport="rest")


def test_rest_remote_plaintext_impossible():
    cfg = ClientConfig(endpoint="203.0.113.5:9090", use_tls=False)
    with pytest.raises(InsecureTransportError):
        TupagoPaymentClient(cfg, token_provider=object(), transport="rest")


class _RedirectHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        self.send_response(302)
        self.send_header("Location", "/api/payments/msjpagoqrusuario")
        self.send_header("Content-Length", "0")
        self.end_headers()


class _MalformedHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        # 200 OK but body is not valid JSON -> ambiguous create outcome.
        body = b"not json at all"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
