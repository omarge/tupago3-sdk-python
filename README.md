# TUPAGO3 SDK para Python

Cliente Python idiomático y seguro para el `PaymentService` de **TUPAGO3**.

Proporciona clientes síncrono (`TupagoPaymentClient`) y asíncrono (`AsyncTupagoPaymentClient`) que cubren las seis operaciones canónicas sobre **gRPC**, **REST** y **GraphQL**.

| Operación | Descripción |
| --- | --- |
| `GetCliente` | Obtiene el perfil de un cliente propio. |
| `GetCatalogo` | Obtiene una entrada del catálogo de instituciones. |
| `GetQrStatus` | Consulta el estatus de un pago QR (JSON Banxico). |
| `GetTelefonicoStatus` | Consulta el estatus de un pago telefónico (JSON Banxico). |
| `CreateQr` | Crea un pago QR. |
| `CreateTelefonico` | Crea un pago telefónico. |

## C2 CreateTelefonico — endpoint canónico

REST: `POST /api/payments/msjpagotelefonicousuario`

Usa `resolveSingleOwnedCliente()` — misma autoridad que gRPC/GraphQL.

**NO** usar `/msjpagotelefonico` (usa `requireOwnedClienteByKey`, autoridad diferente).

## ByCuenta en consultas de estatus

El backend3 ofrece dos formas de consultar el estatus de un pago:

| Variante | Cuándo usarla | Qué envía el SDK |
|----------|-----------------|-------------------|
| **ByCuenta** | El caller conoce la cuenta | `id_cuenta` explícito en el body |
| **Usuario** | El backend resuelve la cuenta del JWT | Solo `idc` |

**Política del SDK:** si el request incluye `id_cuenta` > 0, se usa **ByCuenta** automáticamente.

```python
# ByCuenta (recomendado)
status = client.get_qr_status(GetStatusRequest(idc="abc123", id_cuenta=42))

# Rutas usadas:
# REST:  POST /api/payments/estatusmsjqr
# GraphQL: pagoQrEstatusByCuenta(idc, idCuenta)
```

## Invariantes financieras

* **Sin reintentos automáticos en creates.** `CreateQr` y `CreateTelefonico` realizan como máximo un intento (single-attempt / no-blind-retry). Fallos de transporte con resultado indeterminado se reportan como `TupagoUnknownOutcomeError` (no reintentable) para que el caller reconcilie.
* **Idempotency key preservada verbatim.** El caller es dueño de la clave; el SDK nunca la regenera ni altera.
* **TLS fail-closed.** TLS con verificación de identidad es obligatorio para cualquier endpoint no-loopback. Plaintext solo se permite en loopback real.
* **Auth fail-closed.** Las credenciales son requeridas por defecto. Token en blanco falla antes del dispatch.

## Transportes

| Transporte | Módulo | Estado |
|------------|--------|--------|
| gRPC | default | Completo |
| REST | `tupago3_sdk.rest` | Completo (ByCuenta en status cuando `id_cuenta` presente) |
| GraphQL | `tupago3_sdk.graphql` | Completo (ByCuenta + mutations) |

Selecciona el transporte vía `ClientConfig` / `validate_transport`.

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
