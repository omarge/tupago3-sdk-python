"""Example: minimal TUPAGO3 SDK usage (synchronous).

Uses a LAB-ONLY base64 secret. Never embed production credentials.
"""

from tupago3_sdk import (
    ClientConfig,
    CreateQrRequest,
    GetClienteRequest,
    LabJwtProvider,
    TupagoPaymentClient,
)

# LAB-ONLY secret (never a production credential). Supply the backend3 dev base64-secret
# here for a real lab run; do NOT embed production secrets.
LAB_BASE64_SECRET = "<LAB_BASE64_SECRET>"


def main() -> None:
    config = ClientConfig(endpoint="127.0.0.1:9090")
    client = TupagoPaymentClient(
        config, token_provider=LabJwtProvider(base64_secret=LAB_BASE64_SECRET)
    )
    with client:
        perfil = client.get_cliente(GetClienteRequest(id=1))
        print("Cliente:", perfil.cliente.nombre)
        resp = client.create_qr(
            CreateQrRequest(
                id_cuenta=1, monto=150.00, concepto="Ejemplo", idempotency_key="example-1"
            )
        )
        print("CreateQr:", resp.json)


if __name__ == "__main__":
    main()
