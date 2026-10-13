# TUPAGO3 SDK para Python

Cliente Python idiomático y seguro para el `PaymentService` de **TUPAGO3**.

Proporciona clientes síncrono (`TupagoPaymentClient`) y asíncrono (`AsyncTupagoPaymentClient`) que cubren las seis operaciones canónicas.

| Operación | Descripción |
| --- | --- |
| `GetCliente` | Obtiene el perfil de un cliente propio. |
| `GetCatalogo` | Obtiene una entrada del catálogo de instituciones. |
| `GetQrStatus` | Consulta el estatus de un pago QR (JSON Banxico). |
| `GetTelefonicoStatus` | Consulta el estatus de un pago telefónico (JSON Banxico). |
| `CreateQr` | Crea un pago QR. |
| `CreateTelefonico` | Crea un pago telefónico. |

## Invariantes financieras

* **Sin reintentos automáticos en creates.** `CreateQr` y `CreateTelefonico` realizan como máximo un intento (single-attempt / no-blind-retry). El SDK no garantiza exactly-once en el servidor, pero garantiza que nunca reenvía automáticamente. Fallos de transporte con resultado indeterminado se reportan como `TupagoUnknownOutcomeError` (no reintentable) para que el caller reconcilie.
* **Idempotency key preservada verbatim.** El caller es dueño de la clave; el SDK nunca la regenera ni altera.
* **TLS fail-closed.** TLS con verificación de identidad es obligatorio para cualquier endpoint no-loopback. Plaintext solo se permite en loopback real.
* **JWT Bearer por llamada.** Las credenciales se resuelven dinámicamente vía `TokenProvider`; el token nunca se embebe permanentemente en el canal. Token en blanco falla antes del dispatch.

## Transportes

| Transporte | Módulo | Estado |
|------------|--------|--------|
| gRPC | (default) | Completo |
| REST | `tupago3_sdk.rest_transport` | Implementado (rutas verificadas) |
| GraphQL | `tupago3_sdk.graphql_transport` | Implementado (ByCuenta + mutations) |

## Inicio rápido

```python
from tupago3_sdk import TupagoPaymentClient, ClientConfig, LabJwtProvider, GetClienteRequest

config = ClientConfig(endpoint="127.0.0.1:9090")
client = TupagoPaymentClient(config, token_provider=LabJwtProvider(base64_secret=LAB_SECRET))
with client:
    perfil = client.get_cliente(GetClienteRequest(id=1))
    print(perfil.cliente.nombre)
```

## Licencia

Propietaria. Distribución interna controlada únicamente.
