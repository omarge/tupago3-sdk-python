"""TUPAGO3 SDK for Python.

Idiomatic, secure client library for the TUPAGO3 ``PaymentService`` gRPC API. Provides
synchronous (:class:`TupagoPaymentClient`) and asynchronous (:class:`AsyncTupagoPaymentClient`)
clients with fail-closed TLS, per-RPC bearer-JWT authentication, a typed error hierarchy and
the financial create invariant (at most one gRPC attempt, no automatic retry).
"""

from __future__ import annotations

from tupago3_sdk.async_client import AsyncTupagoPaymentClient
from tupago3_sdk.auth import (
    AsyncTokenProvider,
    CallableAsyncTokenProvider,
    CallableTokenProvider,
    LabAsyncJwtProvider,
    LabJwtProvider,
    StaticAsyncTokenProvider,
    StaticTokenProvider,
    TokenProvider,
)
from tupago3_sdk.client import TupagoPaymentClient
from tupago3_sdk.config import (
    DEFAULT_DEADLINE_SECONDS,
    DEFAULT_GRPC_PORT,
    ClientConfig,
    TlsConfig,
    validate_transport,
)
from tupago3_sdk.errors import (
    PENDING_RECONCILIATION_CODE,
    InsecureTransportError,
    TupagoAbortedError,
    TupagoAlreadyExistsError,
    TupagoCancelledError,
    TupagoDeadlineExceededError,
    TupagoError,
    TupagoFailedPreconditionError,
    TupagoInternalError,
    TupagoInvalidArgumentError,
    TupagoNotFoundError,
    TupagoPendingReconciliationError,
    TupagoPermissionDeniedError,
    TupagoTransportError,
    TupagoUnauthenticatedError,
    TupagoUnavailableError,
    TupagoUnknownError,
    TupagoUnknownOutcomeError,
    TupagoValidationError,
    from_grpc_error,
    is_retryable,
    server_retry_delay,
    should_retry_read,
)
from tupago3_sdk.models import (
    Amount,
    CreateQrRequest,
    CreateTelefonicoRequest,
    GetCatalogoRequest,
    GetClienteRequest,
    GetStatusRequest,
    to_double_amount,
)
from tupago3_sdk.rest import AsyncRestTransport, RestTransport
from tupago3_sdk.retry import RetryPolicy
from tupago3_sdk.transport import (
    AsyncGrpcTransport,
    AsyncPaymentTransport,
    GrpcTransport,
    SyncPaymentTransport,
)

__version__ = "0.1.0"

__all__ = [
    "AsyncTupagoPaymentClient",
    "TupagoPaymentClient",
    "ClientConfig",
    "TlsConfig",
    "validate_transport",
    "DEFAULT_DEADLINE_SECONDS",
    "DEFAULT_GRPC_PORT",
    "RetryPolicy",
    "TupagoError",
    "TupagoTransportError",
    "InsecureTransportError",
    "TupagoValidationError",
    "TupagoNotFoundError",
    "TupagoUnauthenticatedError",
    "TupagoPermissionDeniedError",
    "TupagoInvalidArgumentError",
    "TupagoAlreadyExistsError",
    "TupagoFailedPreconditionError",
    "TupagoAbortedError",
    "TupagoInternalError",
    "TupagoUnavailableError",
    "TupagoDeadlineExceededError",
    "TupagoCancelledError",
    "TupagoUnknownError",
    "TupagoPendingReconciliationError",
    "TupagoUnknownOutcomeError",
    "PENDING_RECONCILIATION_CODE",
    "TokenProvider",
    "AsyncTokenProvider",
    "StaticTokenProvider",
    "StaticAsyncTokenProvider",
    "CallableTokenProvider",
    "CallableAsyncTokenProvider",
    "LabJwtProvider",
    "LabAsyncJwtProvider",
    "from_grpc_error",
    "is_retryable",
    "should_retry_read",
    "server_retry_delay",
    "GetClienteRequest",
    "GetCatalogoRequest",
    "GetStatusRequest",
    "CreateQrRequest",
    "CreateTelefonicoRequest",
    "Amount",
    "to_double_amount",
    "SyncPaymentTransport",
    "AsyncPaymentTransport",
    "GrpcTransport",
    "AsyncGrpcTransport",
    "RestTransport",
    "AsyncRestTransport",
    "__version__",
]
