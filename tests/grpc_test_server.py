"""Faithful in-memory gRPC test server for the TUPAGO3 PaymentService.

This server is used EXCLUSIVELY by the SDK test-suite. It mirrors backend3's
`TupagoGrpcService` semantics for the behaviours that the SDK must handle: JWT bearer
validation (HS512, matching backend3's lab secret), the stable
`TUPAGO_PENDING_RECONCILIATION` ABORTED condition, idempotency conflict (ALREADY_EXISTS),
missing-result (NOT_FOUND), and configurable transport faults (UNAVAILABLE / DEADLINE /
slow response to simulate lost response). It also counts create invocations so the SDK's
"exactly one attempt" financial invariant can be verified.

It is NOT a backend3 replacement and never contacts Banxico/PSP.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import threading
import time
from concurrent import futures

import grpc

from tupago3_sdk.generated import payment_pb2 as pb
from tupago3_sdk.generated import payment_pb2_grpc as grpc_pb


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def verify_hs512(secret: bytes, token: str) -> bool:
    """Verify an HS512 JWT signature (mirrors backend3 JwtServerInterceptor)."""
    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
    except ValueError:
        return False
    signing_input = (header_b64 + "." + payload_b64).encode("ascii")
    expected_b64 = (
        base64.urlsafe_b64encode(hmac.new(secret, signing_input, hashlib.sha512).digest())
        .rstrip(b"=")
        .decode("ascii")
    )
    return hmac.compare_digest(expected_b64, sig_b64)


class TestPaymentServicer:
    """Mirrors backend3 PaymentService behaviour without an ImplBase (new codegen style)."""

    def __init__(self, lab_secret: str) -> None:
        self._secret = base64.b64decode(lab_secret, validate=True)
        self.create_qr_count = 0
        self.create_telefonico_count = 0
        self.received_qr_keys: list[str] = []
        self.received_telefonico_keys: list[str] = []
        self.get_cliente_count = 0
        self.get_qr_status_count = 0

        # Fault injection controls (per create method).
        self.unavailable_on_create = False
        self.deadline_on_create = False
        self.pending_reconciliation_on_create = False
        self.already_exists_on_create = False
        self.slow_create_seconds = 0.0

        # Read fault injection.
        self.unavailable_on_read = False
        self.not_found_on_get_cliente = False

        self.lock = threading.Lock()

    # -- auth ---------------------------------------------------------------
    def _check_auth(self, context: grpc.ServicerContext) -> None:
        md = dict(context.invocation_metadata() or [])
        header = md.get("authorization")
        if not header or not header.startswith("Bearer "):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing bearer token")
            return
        token = header[7:]
        if not verify_hs512(self._secret, token):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid token")
        # Validate expiry minimally to mirror backend rejection of expired tokens.
        try:
            _, payload_b64, _ = token.split(".")
            payload = json.loads(_b64url_decode(payload_b64))
            if "exp" in payload and payload["exp"] < int(time.time()):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid token")
        except Exception:
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid token")

    # -- helpers ------------------------------------------------------------
    def _json(self, payload: dict) -> pb.JsonResponse:
        return pb.JsonResponse(json=json.dumps(payload))

    def _maybe_fault_create(self, context: grpc.ServicerContext, kind: str) -> None:
        if self.pending_reconciliation_on_create:
            context.abort(
                grpc.StatusCode.ABORTED,
                "TUPAGO_PENDING_RECONCILIATION; payment pending reconciliation; do not retry; "
                "query status first",
            )
        if self.already_exists_on_create:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Idempotency conflict")
        if self.unavailable_on_create:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Simulated unavailable")
        if self.deadline_on_create:
            time.sleep(self.slow_create_seconds or 5.0)
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "Simulated deadline")

    # -- RPCs ---------------------------------------------------------------
    def GetCliente(self, request, context):  # noqa: N802
        self._check_auth(context)
        with self.lock:
            self.get_cliente_count += 1
        if self.unavailable_on_read:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Simulated unavailable")
        if self.not_found_on_get_cliente:
            context.abort(
                grpc.StatusCode.NOT_FOUND, "No result available; upstream returned no value"
            )
        perfil = pb.ClientePerfil(
            cliente=pb.Cliente(
                id=request.id, nombre="Cliente Demo", clave_institucion="012", key="KEY1"
            ),
            catalogo_institucion=pb.CatalogoInstitucion(
                id=1, clave_institucion="012", nombre_corto="Banco Demo"
            ),
        )
        return perfil

    def GetCatalogo(self, request, context):  # noqa: N802
        self._check_auth(context)
        if self.unavailable_on_read:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Simulated unavailable")
        return pb.CatalogoInstitucion(
            id=1, clave_institucion=request.clave_institucion, nombre_corto="Banco Demo"
        )

    def GetQrStatus(self, request, context):  # noqa: N802
        self._check_auth(context)
        with self.lock:
            self.get_qr_status_count += 1
        if self.unavailable_on_read:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Simulated unavailable")
        return self._json({"status": "PENDING", "idc": request.idc})

    def GetTelefonicoStatus(self, request, context):  # noqa: N802
        self._check_auth(context)
        if self.unavailable_on_read:
            context.abort(grpc.StatusCode.UNAVAILABLE, "Simulated unavailable")
        return self._json({"status": "PENDING", "idc": request.idc})

    def CreateQr(self, request, context):  # noqa: N802
        self._check_auth(context)
        with self.lock:
            self.create_qr_count += 1
            self.received_qr_keys.append(request.idempotency_key)
        if self.slow_create_seconds:
            time.sleep(self.slow_create_seconds)
        self._maybe_fault_create(context, "qr")
        return self._json({"id": "qr-1", "monto": request.monto, "key": request.idempotency_key})

    def CreateTelefonico(self, request, context):  # noqa: N802
        self._check_auth(context)
        with self.lock:
            self.create_telefonico_count += 1
            self.received_telefonico_keys.append(request.idempotency_key)
        if self.slow_create_seconds:
            time.sleep(self.slow_create_seconds)
        self._maybe_fault_create(context, "telefonico")
        return self._json({"id": "tel-1", "monto": request.monto, "key": request.idempotency_key})


def start_server(
    lab_secret: str,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    tls_credentials: tuple[bytes, bytes] | None = None,
    max_workers: int = 10,
) -> tuple[grpc.Server, int, TestPaymentServicer]:
    """Start a test server; returns (server, bound_port, servicer)."""
    servicer = TestPaymentServicer(lab_secret)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    grpc_pb.add_PaymentServiceServicer_to_server(servicer, server)
    if tls_credentials is not None:
        cert_chain, private_key = tls_credentials
        creds = grpc.ssl_server_credentials(
            [(private_key, cert_chain)], root_certificates=None, require_client_auth=False
        )
        port = server.add_secure_port(f"{host}:{port}", creds)
    else:
        port = server.add_insecure_port(f"{host}:{port}")
    server.start()
    return server, port, servicer
