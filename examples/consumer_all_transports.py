"""Consumer example: use the TUPAGO3 SDK with any of the three transports.

The SDK exposes a SINGLE, transport-neutral public API. Pick the wire protocol at
construction time with ``transport=``; the returned objects are identical regardless
of transport:

    transport="grpc"     (default) -> gRPC, fail-closed TLS on non-loopback
    transport="rest"               -> HTTP/JSON (httpx)
    transport="graphql"            -> GraphQL (httpx)

All financial-safety invariants are identical across transports:
SINGLE-ATTEMPT creates (no blind retry), idempotency key sent, network failure
after dispatch on a create fails closed as TupagoUnknownOutcomeError, auth
fail-closed, remote plaintext impossible.

This example is documentation: point ENDPOINT / TOKEN at an ISOLATED cert/loopback
or lab backend3 instance (never the default 127.0.0.1:9090 production gRPC). It does
not start a server on its own.
"""

from __future__ import annotations

from tupago3_sdk import (
    AsyncTupagoPaymentClient,
    ClientConfig,
    TupagoPaymentClient,
    models,
)

# Replace with an isolated cert/loopback endpoint and a real bearer token provider.
ENDPOINT = "127.0.0.1:9090"
TOKEN_PROVIDER = None  # e.g. StaticTokenProvider("eyJ...") or a callable provider


def sync_example() -> None:
    # --- gRPC (default) ---
    grpc_client = TupagoPaymentClient(
        ClientConfig(endpoint=ENDPOINT, use_tls=True),
        token_provider=TOKEN_PROVIDER,
    )

    # --- REST ---
    rest_client = TupagoPaymentClient(
        ClientConfig(endpoint=ENDPOINT, use_tls=True),
        token_provider=TOKEN_PROVIDER,
        transport="rest",
    )

    # --- GraphQL ---
    gql_client = TupagoPaymentClient(
        ClientConfig(endpoint=ENDPOINT, use_tls=True),
        token_provider=TOKEN_PROVIDER,
        transport="graphql",
    )

    # The SAME calls, the SAME return types, across all three transports.
    for client in (grpc_client, rest_client, gql_client):
        perfil = client.get_cliente(models.GetClienteRequest(id=1))
        print("cliente:", perfil.cliente.nombre)
        catalogo = client.get_catalogo(models.GetCatalogoRequest(clave_institucion="TUP01"))
        print("catalogo:", catalogo.nombre_corto)
        resp = client.create_qr(
            models.CreateQrRequest(
                id_cuenta=1,
                monto=10.50,
                concepto="Demo",
                referencia_numerica=2,
                nombre_cliente="Juan",
                email="juan@example.com",
                tipo_pago=1,
                vigencia=30,
                idempotency_key="UNIQUE-KEY-123",  # SINGLE-ATTEMPT safety seam
            )
        )
        print("create_qr:", resp.json)

    grpc_client.close()
    rest_client.close()
    gql_client.close()


async def async_example() -> None:
    async with AsyncTupagoPaymentClient(
        ClientConfig(endpoint=ENDPOINT, use_tls=True),
        token_provider=TOKEN_PROVIDER,
        transport="graphql",
    ) as client:
        perfil = await client.get_cliente(models.GetClienteRequest(id=1))
        print("async cliente:", perfil.cliente.nombre)


if __name__ == "__main__":
    sync_example()
    # import asyncio; asyncio.run(async_example())
