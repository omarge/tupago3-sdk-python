"""TLS transport security for the TUPAGO3 Python SDK.

Implements the backend's fail-closed rule: TLS with identity verification is mandatory for
non-loopback endpoints; plaintext is allowed only on loopback. The public API never exposes
a credential override that disables certificate verification, sets ``check_hostname=False``
or uses ``CERT_NONE`` for remote endpoints.

Channel-level gRPC automatic retries are disabled globally (``grpc.enable_retries=0``) so the
SDK — not the transport — owns the retry policy. This is essential for the financial
create invariant (at most one attempt).
"""

from __future__ import annotations

import grpc

from tupago3_sdk.config import ClientConfig, validate_transport


def channel_options(config: ClientConfig) -> list[tuple[str, object]]:
    return [
        ("grpc.max_send_message_length", config.max_message_bytes),
        ("grpc.max_receive_message_length", config.max_message_bytes),
        # Disable gRPC service-config automatic retries / hedging: the SDK owns retry.
        ("grpc.enable_retries", 0),
        ("grpc.keepalive_time_ms", 30_000),
        ("grpc.keepalive_timeout_ms", 10_000),
    ]


def build_channel_credentials(config: ClientConfig) -> grpc.ChannelCredentials | None:
    """Return ``None`` for plaintext loopback, else verified TLS credentials.

    For TLS, root certificates come from ``tls.ca_cert_path`` if provided, otherwise the
    system trust store. Optional mutual-TLS client material is attached when configured.
    Certificate verification is always enabled for remote endpoints.
    """
    if not config.use_tls:
        return None

    root_certs: bytes | None = None
    if config.tls.ca_cert_path:
        with open(config.tls.ca_cert_path, "rb") as fh:
            root_certs = fh.read()

    client_cert: bytes | None = None
    client_key: bytes | None = None
    if config.tls.client_cert_path and config.tls.client_key_path:
        with open(config.tls.client_cert_path, "rb") as fh:
            client_cert = fh.read()
        with open(config.tls.client_key_path, "rb") as fh:
            client_key = fh.read()

    return grpc.ssl_channel_credentials(
        root_certificates=root_certs,
        certificate_chain=client_cert,
        private_key=client_key,
    )


def create_channel(config: ClientConfig) -> grpc.Channel:
    """Create a configured gRPC channel honouring the fail-closed TLS rule."""
    validate_transport(config)
    target = f"{config.host()}:{config.port()}"
    options = channel_options(config)

    if not config.use_tls:
        # Plaintext permitted only after validate_transport confirmed loopback.
        return grpc.insecure_channel(target, options=options)

    credentials = build_channel_credentials(config)
    assert credentials is not None
    # ``server_name`` is the *explicit* expected server identity (SNI / hostname verification).
    # It is ONLY applied when the caller configures it. We never silently set it to the target
    # host: for a remote endpoint gRPC verifies the certificate against the host in the target
    # URI by default, which is the correct, non-bypass behaviour. Overriding with the host would
    # be redundant and risks masking a misconfiguration if the field were later widened.
    server_name = config.tls.server_name
    if server_name:
        options = options + [("grpc.ssl_target_name_override", server_name)]
    return grpc.secure_channel(target, credentials, options=options)
