"""T01-T04: Protobuf contract, wire equivalence, int64 lossless, JsonResponse passthrough."""

from __future__ import annotations

import grpc

from tupago3_sdk import TupagoPaymentClient, models
from tupago3_sdk.client import (
    build_create_qr,
    build_get_cliente,
    build_get_status,
)
from tupago3_sdk.generated import payment_pb2 as pb
from tupago3_sdk.generated import payment_pb2_grpc as grpc_pb


def test_T01_six_rpc_contract():
    svc = pb.DESCRIPTOR.services_by_name["PaymentService"]
    methods = {m.name for m in svc.methods}
    assert methods == {
        "GetCliente",
        "GetCatalogo",
        "GetQrStatus",
        "GetTelefonicoStatus",
        "CreateQr",
        "CreateTelefonico",
    }
    # Client exposes all six.
    client_methods = {
        "get_cliente",
        "get_catalogo",
        "get_qr_status",
        "get_telefonico_status",
        "create_qr",
        "create_telefonico",
    }
    assert client_methods.issubset(set(dir(TupagoPaymentClient)))
    # Stub generated methods present (new codegen exposes them via __getattr__ on instances).
    stub_methods = {m.name for m in svc.methods}
    sample = grpc_pb.PaymentServiceStub(grpc.insecure_channel("127.0.0.1:1"))
    for name in stub_methods:
        assert hasattr(sample, name) and callable(getattr(sample, name)), name


def test_T02_protobuf_wire_equivalence():
    req = models.CreateQrRequest(
        id_cuenta=42,
        monto=150.75,
        concepto="Pago",
        referencia_numerica=7,
        nombre_cliente="A",
        email="a@b.c",
        tipo_pago=1,
        vigencia=120,
        idempotency_key="k-xyz",
    )
    built = build_create_qr(req)
    # Direct proto construction must match field-for-field.
    direct = pb.CreateQrRequest(
        id_cuenta=42,
        monto=150.75,
        concepto="Pago",
        referencia_numerica=7,
        nombre_cliente="A",
        email="a@b.c",
        tipo_pago=1,
        vigencia=120,
        idempotency_key="k-xyz",
    )
    assert built.SerializeToString() == direct.SerializeToString()
    # Field numbers preserved.
    assert built.DESCRIPTOR.fields_by_name["monto"].number == 2
    assert built.DESCRIPTOR.fields_by_name["idempotency_key"].number == 9
    # Package preserved.
    assert pb.DESCRIPTOR.package == "tupago.api.v1"


def test_T03_int64_lossless():
    big = 9_223_372_036_854_775_807  # int64 max
    req = build_get_cliente(models.GetClienteRequest(id=big))
    assert req.id == big
    reparsed = pb.GetClienteRequest.FromString(req.SerializeToString())
    assert reparsed.id == big

    status = pb.GetStatusRequest(idc="abc", id_cuenta=big)
    assert pb.GetStatusRequest.FromString(status.SerializeToString()).id_cuenta == big


def test_T04_json_response_passthrough():
    payload = '{"status":"PENDING","idc":"X1"}'
    resp = pb.JsonResponse(json=payload)
    wire = resp.SerializeToString()
    back = pb.JsonResponse.FromString(wire)
    assert back.json == payload
    # Builder preserves raw JSON for status/create responses.
    direct = build_get_status(models.GetStatusRequest(idc="X1", id_cuenta=1))
    assert direct.idc == "X1"
