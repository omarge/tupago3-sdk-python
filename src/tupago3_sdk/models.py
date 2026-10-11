"""Pythonic request/response models and monetary validation for the TUPAGO3 SDK.

The wire contract uses ``double`` for ``monto`` and ``int64`` for identifiers. We do NOT
claim decimal exactness that the protocol does not guarantee; rather we convert explicitly to
``float`` and validate the result against a small set of hard invariants (no NaN, no
infinity, strictly positive, within a technical guard bound). The technical bound is a
defence against absurd magnitudes, not a commercial limit — business limits are enforced by
the backend and must not be invented here.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation

from tupago3_sdk import errors

# Technical guard only: magnitude beyond this cannot be a sane payment amount and would
# overflow reasonable double precision. This is NOT a commercial top; the backend enforces
# real business limits. Configurable so callers can relax for legitimate edge cases.
DEFAULT_MAX_AMOUNT = 1e15

Amount = float | int | Decimal | str


def to_double_amount(value: Amount) -> float:
    """Convert an input amount to a ``float`` (the wire ``double`` type).

    Raises :class:`TupagoValidationError` for non-finite, non-positive or out-of-guard values.
    ``Decimal``/``str`` inputs are converted explicitly so the user opts into the lossy
    double mapping rather than silently coercing.
    """
    try:
        if isinstance(value, bool):  # bool is a subclass of int; reject.
            raise errors.TupagoValidationError("amount must not be a bool")
        if isinstance(value, str):
            decimal = Decimal(value)
        elif isinstance(value, Decimal):
            decimal = value
        else:
            decimal = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise errors.TupagoValidationError(f"amount is not a valid number: {value!r}") from exc

    if not decimal.is_finite():
        raise errors.TupagoValidationError(f"amount must be finite: {value!r}")
    if decimal <= 0:
        raise errors.TupagoValidationError(f"amount must be strictly positive: {decimal!r}")

    result = float(decimal)
    if math.isnan(result) or math.isinf(result):
        raise errors.TupagoValidationError(f"amount is not a finite double: {value!r}")
    if abs(result) > DEFAULT_MAX_AMOUNT:
        raise errors.TupagoValidationError(
            f"amount magnitude {result!r} exceeds technical guard {DEFAULT_MAX_AMOUNT!r}"
        )
    return result


@dataclass
class GetClienteRequest:
    id: int


@dataclass
class GetCatalogoRequest:
    clave_institucion: str


@dataclass
class GetStatusRequest:
    idc: str
    id_cuenta: int


@dataclass
class CreateQrRequest:
    id_cuenta: int
    monto: Amount
    concepto: str = ""
    referencia_numerica: int = 0
    nombre_cliente: str = ""
    email: str = ""
    tipo_pago: int = 0
    vigencia: int = 0
    idempotency_key: str = field(default_factory=lambda: _new_idempotency_key())

    def double_monto(self) -> float:
        return to_double_amount(self.monto)


@dataclass
class CreateTelefonicoRequest:
    key: str
    monto: Amount
    concepto: str = ""
    referencia_numerica: int = 0
    numero_celular: str = ""
    vigencia: int = 0
    idempotency_key: str = field(default_factory=lambda: _new_idempotency_key())

    def double_monto(self) -> float:
        return to_double_amount(self.monto)


def _new_idempotency_key() -> str:
    import uuid

    return uuid.uuid4().hex
