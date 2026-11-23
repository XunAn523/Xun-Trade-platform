"""Short-lived test PKI: local private keys, exchange CSR/certificate only.

No production capability is signed here. Never place output inside tracked
source paths. Reissuing a leaf changes its DER SHA-256 pin on the opposite host.
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def _write_new(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(content)
        stream.flush()
        os.fsync(stream.fileno())


def _key(path: Path) -> ec.EllipticCurvePrivateKey:
    key = ec.generate_private_key(ec.SECP256R1())
    _write_new(
        path,
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )
    return key


def _name(value: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, value)])


def initialize_ca(directory: Path) -> None:
    key = _key(directory / "bridge-ca-key.pem")
    now = datetime.now(timezone.utc)
    name = _name("TradeCore QMT DISARMED test CA")
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(False, False, False, False, False, True, True, None, None), critical=True
        )
        .sign(key, hashes.SHA256())
    )
    _write_new(directory / "bridge-ca.pem", certificate.public_bytes(serialization.Encoding.PEM))


def request_leaf(directory: Path, role: str) -> None:
    key = _key(directory / f"{role}-key.pem")
    request = (
        x509.CertificateSigningRequestBuilder()
        .subject_name(_name(f"TradeCore QMT DISARMED {role}"))
        .sign(key, hashes.SHA256())
    )
    _write_new(directory / f"{role}.csr", request.public_bytes(serialization.Encoding.PEM))


def sign_leaf(ca_directory: Path, csr_path: Path, output: Path, role: str, address: str) -> None:
    request = x509.load_pem_x509_csr(csr_path.read_bytes())
    if not request.is_signature_valid or request.subject != _name(f"TradeCore QMT DISARMED {role}"):
        raise ValueError("CSR role or signature differs")
    public = request.public_key()
    if not isinstance(public, ec.EllipticCurvePublicKey) or not isinstance(
        public.curve, ec.SECP256R1
    ):
        raise ValueError("CSR key differs from the test profile")
    ca = x509.load_pem_x509_certificate((ca_directory / "bridge-ca.pem").read_bytes())
    key = serialization.load_pem_private_key(
        (ca_directory / "bridge-ca-key.pem").read_bytes(), password=None
    )
    if not isinstance(key, ec.EllipticCurvePrivateKey) or key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ) != ca.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    ):
        raise ValueError("CA private key does not match certificate")
    now = datetime.now(timezone.utc)
    if not ca.not_valid_before_utc <= now < ca.not_valid_after_utc - timedelta(days=1):
        raise ValueError("Test CA has insufficient validity")
    usage = (
        ExtendedKeyUsageOID.SERVER_AUTH
        if role == "windows-edge"
        else ExtendedKeyUsageOID.CLIENT_AUTH
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(request.subject)
        .issuer_name(ca.subject)
        .public_key(public)
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=5))
        .not_valid_after(min(now + timedelta(days=7), ca.not_valid_after_utc))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(True, False, False, False, False, False, False, None, None), critical=True
        )
        .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.IPv4Address(address))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    _write_new(output, certificate.public_bytes(serialization.Encoding.PEM))


def certificate_summary(path: Path) -> dict[str, str]:
    certificate = x509.load_pem_x509_certificate(path.read_bytes())
    return {
        "certificate_sha256": certificate.fingerprint(hashes.SHA256()).hex(),
        "expires_utc": certificate.not_valid_after_utc.isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    ca = sub.add_parser("init-ca")
    ca.add_argument("--directory", type=Path, required=True)
    request = sub.add_parser("request")
    request.add_argument("--directory", type=Path, required=True)
    request.add_argument("--role", choices=("windows-edge", "linux-client"), required=True)
    sign = sub.add_parser("sign")
    sign.add_argument("--ca-directory", type=Path, required=True)
    sign.add_argument("--csr", type=Path, required=True)
    sign.add_argument("--output", type=Path, required=True)
    sign.add_argument("--role", choices=("windows-edge", "linux-client"), required=True)
    sign.add_argument("--ip", required=True)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--certificate", type=Path, required=True)
    args = parser.parse_args()
    if args.action == "init-ca":
        initialize_ca(args.directory)
    elif args.action == "request":
        request_leaf(args.directory, args.role)
    elif args.action == "sign":
        sign_leaf(args.ca_directory, args.csr, args.output, args.role, args.ip)
        print(json.dumps(certificate_summary(args.output), sort_keys=True))
    else:
        print(json.dumps(certificate_summary(args.certificate), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
