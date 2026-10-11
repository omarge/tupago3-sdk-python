# Changelog

All notable changes to the TUPAGO3 SDK for Python are documented here.

## 0.1.0 (2026-10-11)

- Initial controlled-internal distribution candidate.
- Generated Protobuf stubs from `backend3` authoritative `payment.proto`
  (package `tupago.api.v1`, service `PaymentService`, six RPCs).
- Synchronous client `TupagoPaymentClient` and asynchronous client
  `AsyncTupagoPaymentClient` (grpc.aio) with context-manager resource management.
- Fail-closed TLS: verified TLS mandatory for non-loopback; plaintext only on loopback.
- Per-RPC bearer-JWT injection via dynamic `TokenProvider` (sync + async), including a
  lab HS512 JWT provider mirroring backend3's `SecurityJwtConfiguration`.
- Typed error hierarchy mapping all required gRPC status codes, plus
  `TupagoPendingReconciliationError` (TUPAGO_PENDING_RECONCILIATION) and
  `TupagoUnknownOutcomeError` for indeterminate create outcomes.
- Financial invariants: create operations perform at most one gRPC attempt (no automatic
  retry); idempotency key preserved verbatim; gRPC channel auto-retries disabled.
- Monetary validation for `monto` (double): rejects NaN/Infinity, non-positive and
  out-of-technical-guard magnitudes; explicit Decimal/str input conversion.
- Bounded read retry for transient codes only.
