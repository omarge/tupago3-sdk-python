"""Transport-neutral abstraction for the TUPAGO3 payment RPCs.

Defines :class:`SyncPaymentTransport` / :class:`AsyncPaymentTransport` protocols and the
gRPC implementations :class:`GrpcTransport` / :class:`AsyncGrpcTransport`. The public clients
(:class:`~tupago3_sdk.client.TupagoPaymentClient` /
:class:`~tupago3_sdk.async_client.AsyncTupagoPaymentClient`) delegate all wire I/O to a
transport instance, so future REST/GraphQL transports can implement the same protocol without
changing the public client surface.
"""

from __future__ import annotations

import asyncio
from typing import Protocol, runtime_checkable

import grpc
import grpc.aio

from tupago3_sdk import auth, errors, retry
from tupago3_sdk.config import ClientConfig
from tupago3_sdk.generated import payment_pb2_grpc as grpc_pb


@runtime_checkable
class SyncPaymentTransport(Protocol):
    """Synchronous transport contract (gRPC now; REST/GraphQL later)."""

    def read(self, operation: str, method_name: str, request: object) -> object: ...

    def create(
        self,
        operation: str,
        method_name: str,
        request: object,
        *,
        idempotency_key: str | None = None,
    ) -> object: ...

    def close(self) -> None: ...


@runtime_checkable
class AsyncPaymentTransport(Protocol):
    """Asynchronous transport contract (gRPC now; REST/GraphQL later)."""

    async def read_async(self, operation: str, method_name: str, request: object) -> object: ...

    async def create_async(
        self,
        operation: str,
        method_name: str,
        request: object,
        *,
        idempotency_key: str | None = None,
    ) -> object: ...

    async def aclose(self) -> None: ...


class GrpcTransport:
    """Synchronous gRPC transport (SDK-authoritative channel)."""

    def __init__(self, channel, *, config: ClientConfig, token_provider, retry_policy) -> None:
        self._channel = channel
        self._config = config
        self._token_provider = token_provider
        self._retry_policy = retry_policy or retry.RetryPolicy()
        self._stub = grpc_pb.PaymentServiceStub(channel)

    # -- metadata / timeout -------------------------------------------------
    def _metadata(self) -> list[tuple[str, str]]:
        metadata: list[tuple[str, str]] = []
        if self._token_provider is not None:
            token = self._token_provider.get_token()
            if token:
                metadata.append(("authorization", f"Bearer {token}"))
        return metadata

    def _timeout(self):
        return self._config.deadline_seconds or None

    # -- RPC dispatch -------------------------------------------------------
    def read(self, operation: str, method_name: str, request: object):
        method = getattr(self._stub, method_name)

        def _invoke():
            return method(request, metadata=self._metadata(), timeout=self._timeout())

        return retry.retry_read_sync(self._retry_policy, operation, _invoke)

    def create(self, operation: str, method_name: str, request: object, *, idempotency_key=None):
        method = getattr(self._stub, method_name)
        try:
            return method(request, metadata=self._metadata(), timeout=self._timeout())
        except grpc.RpcError as exc:  # type: ignore[attr-defined]
            raise errors.from_grpc_error(
                exc,
                operation=operation,
                idempotency_key=idempotency_key,
                unknown_outcome_on_create=True,
            ) from exc

    def close(self) -> None:
        try:
            self._channel.close()
        except Exception:
            pass


class AsyncGrpcTransport:
    """Asynchronous gRPC transport (SDK-authoritative channel)."""

    def __init__(
        self, channel, *, config: ClientConfig, token_provider, max_read_attempts: int = 3
    ) -> None:
        self._channel = channel
        self._config = config
        self._token_provider = token_provider
        self._max_read_attempts = max(1, max_read_attempts)
        self._stub = grpc_pb.PaymentServiceStub(channel)

    async def _token(self) -> str:
        if self._token_provider is None:
            return ""
        provider = self._token_provider
        if isinstance(provider, auth.AsyncTokenProvider):
            return await provider.aget_token()
        return provider.get_token()

    async def _metadata(self) -> list[tuple[str, str]]:
        token = await self._token()
        if token:
            return [("authorization", f"Bearer {token}")]
        return []

    def _timeout(self):
        return self._config.deadline_seconds or None

    async def read_async(self, operation: str, method_name: str, request: object):
        method = getattr(self._stub, method_name)
        metadata = await self._metadata()
        timeout = self._timeout()
        last_exc: Exception | None = None
        for attempt in range(1, self._max_read_attempts + 1):
            try:
                return await method(request, metadata=metadata, timeout=timeout)
            except grpc.RpcError as exc:  # type: ignore[attr-defined]
                last_exc = exc
                if attempt >= self._max_read_attempts or not errors.should_retry_read(
                    exc, operation=operation
                ):
                    raise errors.from_grpc_error(exc, operation=operation) from exc
                server_delay = errors.server_retry_delay(exc)
                if server_delay is not None:
                    await asyncio.sleep(server_delay)
                else:
                    await asyncio.sleep(min(1.0, 0.1 * (2 ** (attempt - 1))))
        assert last_exc is not None
        raise errors.from_grpc_error(last_exc, operation=operation)

    async def create_async(
        self, operation: str, method_name: str, request: object, *, idempotency_key=None
    ):
        method = getattr(self._stub, method_name)
        try:
            metadata = await self._metadata()
            return await method(request, metadata=metadata, timeout=self._timeout())
        except grpc.RpcError as exc:  # type: ignore[attr-defined]
            raise errors.from_grpc_error(
                exc,
                operation=operation,
                idempotency_key=idempotency_key,
                unknown_outcome_on_create=True,
            ) from exc

    async def aclose(self) -> None:
        try:
            await self._channel.close()
        except Exception:
            pass
