"""Pinned mutual-TLS socket construction for the QMT bridge.

This module owns only TLS transport setup and framed I/O.  It neither encodes
domain orders nor has access to QMT APIs, Edge SQLite, or shared-memory rings.
"""

from __future__ import annotations

import hashlib
import socket
import ssl
from dataclasses import dataclass
from pathlib import Path

from .bridge_wire import QmtBridgeWireError, QmtBridgeWireFrame, receive_wire_frame, send_wire_frame


class QmtMtlsChannelError(RuntimeError):
    """A QMT bridge TLS identity or lifecycle invariant failed."""


def _require_certificate_digest(value: str, *, name: str) -> str:
    normalized = value.lower()
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise QmtMtlsChannelError(f"QMT bridge {name} must be a SHA-256 certificate digest")
    return normalized


@dataclass(frozen=True)
class QmtMtlsClientConfig:
    """Pinned client material paths; paths contain no credential values."""

    host: str
    port: int
    ca_certificate_path: Path
    certificate_path: Path
    private_key_path: Path
    expected_server_certificate_sha256: str
    server_hostname: str
    timeout_s: float = 3.0
    source_host: str | None = None

    def __post_init__(self) -> None:
        if not self.host or not self.server_hostname or not 1 <= self.port <= 65535:
            raise QmtMtlsChannelError("QMT bridge client endpoint is invalid")
        if self.timeout_s <= 0:
            raise QmtMtlsChannelError("QMT bridge client timeout must be positive")
        object.__setattr__(
            self,
            "expected_server_certificate_sha256",
            _require_certificate_digest(
                self.expected_server_certificate_sha256, name="expected server certificate"
            ),
        )


@dataclass(frozen=True)
class QmtMtlsServerConfig:
    """Pinned server material and its one permitted client certificate digest."""

    host: str
    port: int
    ca_certificate_path: Path
    certificate_path: Path
    private_key_path: Path
    expected_client_certificate_sha256: str

    def __post_init__(self) -> None:
        if not self.host or not 1 <= self.port <= 65535:
            raise QmtMtlsChannelError("QMT bridge server endpoint is invalid")
        object.__setattr__(
            self,
            "expected_client_certificate_sha256",
            _require_certificate_digest(
                self.expected_client_certificate_sha256, name="expected client certificate"
            ),
        )


class QmtMtlsConnection:
    """One live pinned mTLS connection with no reconnect or command policy."""

    def __init__(self, connection: ssl.SSLSocket) -> None:
        self._connection = connection
        self._closed = False

    @classmethod
    def connect(cls, config: QmtMtlsClientConfig) -> "QmtMtlsConnection":
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=str(config.ca_certificate_path))
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.load_cert_chain(str(config.certificate_path), str(config.private_key_path))
        raw_connection: socket.socket | None = None
        try:
            raw_connection = socket.create_connection(
                (config.host, config.port), config.timeout_s,
                source_address=(config.source_host, 0) if config.source_host is not None else None,
            )
            connection = context.wrap_socket(raw_connection, server_hostname=config.server_hostname)
        except OSError as error:
            if raw_connection is not None:
                raw_connection.close()
            raise QmtMtlsChannelError("QMT bridge mTLS connect failed") from error
        try:
            _verify_peer_digest(connection, config.expected_server_certificate_sha256)
        except BaseException:
            connection.close()
            raise
        return cls(connection)

    def send(self, frame: QmtBridgeWireFrame) -> None:
        self._require_open()
        try:
            send_wire_frame(self._connection, frame)
        except QmtBridgeWireError as error:
            raise QmtMtlsChannelError("QMT bridge mTLS frame send failed") from error

    def receive(self) -> QmtBridgeWireFrame:
        self._require_open()
        try:
            return receive_wire_frame(self._connection)
        except QmtBridgeWireError as error:
            raise QmtMtlsChannelError("QMT bridge mTLS frame receive failed") from error

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                self._connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._connection.close()

    def _require_open(self) -> None:
        if self._closed:
            raise QmtMtlsChannelError("QMT bridge mTLS connection is closed")


def create_qmt_mtls_server_context(config: QmtMtlsServerConfig) -> ssl.SSLContext:
    """Build the only permitted TLS server context for a QMT Edge listener."""

    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cafile=str(config.ca_certificate_path))
    context.load_cert_chain(str(config.certificate_path), str(config.private_key_path))
    return context


def verify_qmt_mtls_client(
    connection: ssl.SSLSocket, config: QmtMtlsServerConfig
) -> None:
    """Require the configured client leaf in addition to CA validation."""

    _verify_peer_digest(connection, config.expected_client_certificate_sha256)


def _verify_peer_digest(connection: ssl.SSLSocket, expected_digest: str) -> None:
    certificate = connection.getpeercert(binary_form=True)
    if not certificate:
        raise QmtMtlsChannelError("QMT bridge mTLS peer presented no certificate")
    actual_digest = hashlib.sha256(certificate).hexdigest()
    if actual_digest != expected_digest:
        raise QmtMtlsChannelError("QMT bridge mTLS peer certificate pin does not match")
