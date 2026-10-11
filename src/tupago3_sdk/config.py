"""Configuration types for the TUPAGO3 Python SDK.

The configuration model encodes the financial-transport security rules mandated by the
TUPAGO3 backend (backend3 ``GrpcTransportSecurity``): TLS with identity verification is
mandatory for any non-loopback endpoint, and plaintext is permitted *only* on a real
loopback bind. The SDK never exposes a public override that disables certificate
validation, sets ``check_hostname=False`` or uses ``CERT_NONE`` for remote endpoints.
"""

from __future__ import annotations

import dataclasses
import socket

DEFAULT_GRPC_PORT = 9090
DEFAULT_DEADLINE_SECONDS = 30.0
DEFAULT_MAX_MESSAGE_BYTES = 16 * 1024 * 1024


def _is_loopback_host(host: str) -> bool:
    """Return ``True`` only for a genuine loopback address/hostname.

    IPv4 ``127.0.0.0/8``, IPv6 ``::1`` and the literal hostnames ``localhost`` /
    ``localhost.localdomain`` are treated as loopback. Any other resolvable name is
    NOT loopback; this is deliberately conservative so a hostname that happens to
    resolve to loopback in some environment is never silently trusted off-host.
    """
    host = (host or "").strip()
    if not host:
        return False
    bare = host.split(":", 1)[0]
    bare = bare.strip("[]")
    if bare.lower() in ("localhost", "localhost.localdomain", "127.0.0.1", "::1"):
        return True
    try:
        info = socket.getaddrinfo(bare, None)
    except socket.gaierror:
        return False
    for entry in info:
        addr = str(entry[4][0])
        try:
            packed = socket.inet_pton(entry[0], addr)
        except OSError:
            return False
        if entry[0] == socket.AF_INET:
            if not packed.startswith(b"\x7f"):
                return False
        else:
            if not packed == b"\x00" * 15 + b"\x01":
                return False
    return True


@dataclasses.dataclass(frozen=True)
class TlsConfig:
    """TLS transport options.

    ``verify`` MUST stay ``True`` for any non-loopback endpoint. ``ca_cert_path`` points to
    a PEM bundle used to verify the server certificate chain. Optional mutual-TLS client
    certificate material may be supplied via ``client_cert_path`` / ``client_key_path``.

    ``server_name`` is the TLS SNI / hostname used for certificate verification. It is the
    *expected* server identity and must never be used to bypass hostname verification on a
    remote endpoint.
    """

    verify: bool = True
    ca_cert_path: str | None = None
    client_cert_path: str | None = None
    client_key_path: str | None = None
    server_name: str | None = None


@dataclasses.dataclass(frozen=True)
class ClientConfig:
    """Top-level client configuration.

    ``endpoint`` is ``host[:port]``. When ``use_tls`` is ``False`` the endpoint MUST be a
    loopback address (validated at construction time by :func:`validate_transport`).
    """

    endpoint: str = "127.0.0.1"
    use_tls: bool = False
    tls: TlsConfig = dataclasses.field(default_factory=TlsConfig)
    deadline_seconds: float = DEFAULT_DEADLINE_SECONDS
    max_message_bytes: int = DEFAULT_MAX_MESSAGE_BYTES

    def host(self) -> str:
        return self.endpoint.split(":", 1)[0]

    def port(self) -> int:
        if ":" in self.endpoint:
            return int(self.endpoint.split(":", 1)[1])
        return DEFAULT_GRPC_PORT

    def is_loopback(self) -> bool:
        return _is_loopback_host(self.host())


def validate_transport(config: ClientConfig) -> None:
    """Fail-closed transport security validation.

    Raises :class:`InsecureTransportError` when plaintext (``use_tls=False``) is requested
    for a non-loopback endpoint, or when TLS verification would be disabled for a remote
    endpoint.
    """
    from tupago3_sdk.errors import InsecureTransportError

    if not config.use_tls:
        if not config.is_loopback():
            raise InsecureTransportError(
                f"Plaintext (non-TLS) transport is only permitted on a loopback endpoint; "
                f"endpoint={config.endpoint} is not loopback"
            )
        return

    if not config.tls.verify:
        raise InsecureTransportError(
            "Public API refuses TLS verification disabled; use the test harness for lab scenarios"
        )
