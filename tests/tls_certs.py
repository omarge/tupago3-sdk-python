"""TLS test certificate generation (cryptography).

Generates self-contained PEM fixtures to exercise the fail-closed TLS rules without any
external dependency or contact with production infrastructure. For every scenario a *trusted*
CA is emitted (``ca.pem``) and the client verifies against it; only the server certificate
differs per scenario:

* ``trusted``   — server cert signed by the trusted CA, SAN localhost/127.0.0.1 — accepted.
* ``untrusted`` — server cert signed by a DIFFERENT CA — rejected (unknown CA).
* ``expired``   — server cert signed by the trusted CA but expired — rejected (expired).
* ``mismatch``  — server cert signed by the trusted CA but SAN=otherhost — rejected (mismatch).

All material is written to a pytest ``tmp_path`` and never reused outside the test process.
"""

from __future__ import annotations

import datetime
import ipaddress
import os
from dataclasses import dataclass

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


@dataclass
class CertBundle:
    ca_cert_path: str  # trusted CA the client uses for verification
    server_cert_path: str
    server_key_path: str


def _make_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _write(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(data)


def _cert_pem(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.PEM)


def _key_pem(key: rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _looks_like_ip(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def _build_ca(common_name: str = "TUPAGO3-Test-CA") -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = _make_key()
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=True,
                crl_sign=True,
                key_encipherment=False,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(key, hashes.SHA256())
    )
    return key, cert


def _build_server_cert(
    ca_key: rsa.RSAPrivateKey,
    ca_cn: str,
    *,
    common_name: str,
    san: list[str],
    not_before: datetime.datetime | None = None,
    not_after: datetime.datetime | None = None,
) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = _make_key()
    now = datetime.datetime.now(datetime.UTC)
    not_before = not_before or (now - datetime.timedelta(days=1))
    not_after = not_after or (now + datetime.timedelta(days=3650))
    san_list = [x509.DNSName(n) for n in san if not _looks_like_ip(n)] + [
        x509.IPAddress(ipaddress.ip_address(n)) for n in san if _looks_like_ip(n)
    ]
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, ca_cn)]))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.SubjectAlternativeName(san_list), critical=False)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                key_cert_sign=False,
                crl_sign=False,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    return key, cert


def generate_bundle(tmp_path: str, kind: str) -> CertBundle:
    """Generate a trusted CA plus a per-kind server certificate in ``tmp_path``."""
    ca_key, ca_cert = _build_ca()
    trusted_ca_path = os.path.join(tmp_path, "ca.pem")
    _write(trusted_ca_path, _cert_pem(ca_cert))

    san = ["localhost", "127.0.0.1"]

    if kind == "trusted":
        srv_key, srv_cert = _build_server_cert(
            ca_key, "TUPAGO3-Test-CA", common_name="localhost", san=san
        )
    elif kind == "untrusted":
        other_key, other_cert = _build_ca(common_name="TUPAGO3-Other-CA")
        srv_key, srv_cert = _build_server_cert(
            other_key, "TUPAGO3-Other-CA", common_name="localhost", san=san
        )
    elif kind == "expired":
        now = datetime.datetime.now(datetime.UTC)
        srv_key, srv_cert = _build_server_cert(
            ca_key,
            "TUPAGO3-Test-CA",
            common_name="localhost",
            san=san,
            not_before=now - datetime.timedelta(days=30),
            not_after=now - datetime.timedelta(days=1),
        )
    elif kind == "mismatch":
        srv_key, srv_cert = _build_server_cert(
            ca_key, "TUPAGO3-Test-CA", common_name="otherhost", san=["otherhost"]
        )
    else:
        raise ValueError(f"unknown TLS scenario kind: {kind}")

    cert_path = os.path.join(tmp_path, "server.pem")
    key_path = os.path.join(tmp_path, "server.key")
    _write(cert_path, _cert_pem(srv_cert))
    _write(key_path, _key_pem(srv_key))
    return CertBundle(
        ca_cert_path=trusted_ca_path, server_cert_path=cert_path, server_key_path=key_path
    )
