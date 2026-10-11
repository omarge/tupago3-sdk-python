"""Example: minimal TUPAGO3 SDK usage (asynchronous)."""

import asyncio

from tupago3_sdk import (
    AsyncTupagoPaymentClient,
    ClientConfig,
    CreateTelefonicoRequest,
    GetQrStatusRequest,
    LabAsyncJwtProvider,
)

# LAB-ONLY secret (never a production credential). Supply the backend3 dev base64-secret
# here for a real lab run; do NOT embed production secrets.
LAB_BASE64_SECRET = "<LAB_BASE64_SECRET>"


async def main() -> None:
    config = ClientConfig(endpoint="127.0.0.1:9090")
    client = AsyncTupagoPaymentClient(
        config, token_provider=LabAsyncJwtProvider(base64_secret=LAB_BASE64_SECRET)
    )
    async with client:
        status = await client.get_qr_status(GetQrStatusRequest(idc="synthetic", id_cuenta=1))
        print("QrStatus:", status.json)
        resp = await client.create_telefonico(
            CreateTelefonicoRequest(key="KEY1", monto=99.50, idempotency_key="example-tel-1")
        )
        print("CreateTelefonico:", resp.json)


if __name__ == "__main__":
    asyncio.run(main())
