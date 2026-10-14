"""Public REST transport: cross-SDK-consistent ``RestPaymentTransport`` name.

Thin facade over :class:`tupago3_sdk.rest.RestTransport` that accepts a target URL
string and exposes the canonical snake_case capability methods, matching the
public surface used by the other TUPAGO3 SDKs (.NET / TypeScript / Java).
"""

from __future__ import annotations

from typing import Any

from tupago3_sdk.config import ClientConfig
from tupago3_sdk.rest import RestTransport


def _config_from_target(target: str, *, allow_unauthenticated: bool = True) -> ClientConfig:
    scheme, _, rest = target.partition("://")
    if not rest:
        scheme, rest = "http", target
    use_tls = scheme.lower() == "https"
    return ClientConfig(endpoint=rest, use_tls=use_tls, allow_unauthenticated=allow_unauthenticated)


class RestPaymentTransport:
    """Synchronous REST transport (public, cross-SDK-named facade)."""

    def __init__(
        self,
        target: str,
        *,
        token_provider=None,
        allow_unauthenticated: bool = True,
    ) -> None:
        self._inner = RestTransport(
            config=_config_from_target(target, allow_unauthenticated=allow_unauthenticated),
            token_provider=token_provider,
        )

    def create_qr(self, request: Any) -> Any:
        return self._inner.create("CreateQr", "create_qr", request)

    def create_telefonico(self, request: Any) -> Any:
        return self._inner.create("CreateTelefonico", "create_telefonico", request)

    def get_qr_status(self, request: Any) -> Any:
        return self._inner.read("GetQrStatus", "get_qr_status", request)

    def get_telefonico_status(self, request: Any) -> Any:
        return self._inner.read("GetTelefonicoStatus", "get_telefonico_status", request)

    def get_cliente(self, request: Any) -> Any:
        return self._inner.read("GetCliente", "get_cliente", request)

    def get_catalogo(self, request: Any) -> Any:
        return self._inner.read("GetCatalogo", "get_catalogo", request)

    def close(self) -> None:
        self._inner.close()
