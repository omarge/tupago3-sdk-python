# TUPAGO3 SDK for Python

Secure, idiomatic Python client SDK for the **TUPAGO3** `PaymentService` gRPC API.

The SDK provides synchronous (`TupagoPaymentClient`) and asynchronous
(`AsyncTupagoPaymentClient`) clients that wrap the six `PaymentService` RPCs defined in
`backend3`'s authoritative Protobuf contract:

| RPC | Description |
| --- | --- |
| `GetCliente` | Fetch an owned client profile. |
| `GetCatalogo` | Fetch an institution catalog entry. |
| `GetQrStatus` | Query QR payment status (raw Banxico JSON). |
| `GetTelefonicoStatus` | Query telephonic payment status (raw Banxico JSON). |
| `CreateQr` | Create a QR payment. |
| `CreateTelefonico` | Create a telephonic payment. |

## Financial safety invariants

* **No automatic retries on create.** `CreateQr` and `CreateTelefonico` perform at most one
  gRPC attempt. The SDK cannot guarantee exactly-once server execution, but it guarantees it
  never resubmits automatically. Transport failures with an indeterminate result are surfaced
  as `TupagoUnknownOutcomeError` (non-retryable) so the caller reconciles via status queries.
* **Idempotency key is preserved verbatim.** The caller owns the `idempotency_key`; the SDK
  never regenerates or alters it.
* **Fail-closed TLS.** TLS with identity verification is mandatory for any non-loopback
  endpoint. Plaintext is permitted only on a real loopback address.
* **Per-RPC bearer JWT.** Credentials are resolved dynamically per call via a `TokenProvider`;
  the token is never embedded permanently on the channel.

## Quick start

```python
from tupago3_sdk import TupagoPaymentClient, ClientConfig, LabJwtProvider, GetClienteRequest

config = ClientConfig(endpoint="127.0.0.1:9090")
client = TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=LAB_SECRET))
with client:
    perfil = client.get_cliente(GetClienteRequest(id=1))
    print(perfil.cliente.nombre)
```

Async:

```python
from tupago3_sdk import AsyncTupagoPaymentClient, LabAsyncJwtProvider, CreateQrRequest

async with AsyncTupagoPaymentClient(config, token_provider=LabAsyncJwtProvider(base64_secret=LAB_SECRET)) as client:
    resp = await client.create_qr(CreateQrRequest(id_cuenta=1, monto=150.00, idempotency_key="k-1"))
    print(resp.json)
```

## Error handling

All failures raise a typed `TupagoError` subclass. Create operations raise
`TupagoUnknownOutcomeError` (indeterminate) or `TupagoPendingReconciliationError`
(`TUPAGO_PENDING_RECONCILIATION`, do-not-retry) when applicable.

## License

Proprietary. Internal controlled distribution only.
