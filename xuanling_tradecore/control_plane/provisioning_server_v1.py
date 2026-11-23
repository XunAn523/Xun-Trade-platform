"""Minimal TLS 1.3 mTLS HTTP/2 listener for Provisioning Control API V1.

It is deliberately separate from the Consumer AF_UNIX listener and from every
OMS/EMS execution worker.  The listener accepts only bounded JSON requests,
derives the peer identity from the completed TLS handshake, and delegates
business decisions to :mod:`provisioning_http_v1`.
"""

from __future__ import annotations

import asyncio
import json
import socket
import ssl
import time
from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence, cast

from h2.config import H2Configuration
from h2.connection import H2Connection
from h2.events import ConnectionTerminated, DataReceived, RequestReceived, StreamEnded, StreamReset

from ..domain._validation import require_positive, require_text
from .provisioning_contracts_v1 import ProvisioningErrorV1
from .provisioning_http_v1 import (
    ProvisioningHttpRequestV1,
    ProvisioningHttpResponseV1,
    ProvisioningHttpRouterV1,
    TlsConnectionFactsV1,
)


@dataclass(frozen=True)
class ProvisioningHttp2ServerConfigV1:
    """Listener limits.  These limits protect the non-hot-path control API."""

    host: str
    port: int
    max_request_body_bytes: int = 65_536
    max_request_header_bytes: int = 16_384

    def __post_init__(self) -> None:
        require_text(self.host, "control listener host", max_length=255)
        if not 1 <= self.port <= 65_535:
            raise ProvisioningErrorV1("CONTROL_REQUEST", "control listener port is invalid")
        require_positive(self.max_request_body_bytes, "control request body limit")
        require_positive(self.max_request_header_bytes, "control request header limit")


def build_provisioning_server_ssl_context_v1(
    *, certificate_file: str, private_key_file: str, client_ca_file: str
) -> ssl.SSLContext:
    """Build the only accepted control-plane TLS profile.

    Callers provide protected filesystem paths.  The context requires a client
    certificate chain and negotiates only ALPN ``h2``; it intentionally has no
    HTTP/1.1 fallback.
    """

    for value, label in (
        (certificate_file, "control server certificate file"),
        (private_key_file, "control server private key file"),
        (client_ca_file, "control client CA file"),
    ):
        require_text(value, label, max_length=4_096)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.maximum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    context.options |= ssl.OP_NO_COMPRESSION
    context.load_cert_chain(certfile=certificate_file, keyfile=private_key_file)
    context.load_verify_locations(cafile=client_ca_file)
    context.set_alpn_protocols(["h2"])
    return context


@dataclass
class _StreamRequestV1:
    method: str
    path: str
    body: bytearray
    rejected: bool = False


class _ServerWithSocketsV1(Protocol):
    """Typing shim for the socket list exposed by concrete asyncio servers."""

    @property
    def sockets(self) -> Sequence[socket.socket] | None: ...


class ProvisioningHttp2ServerV1:
    """A small, bounded HTTP/2 server for the independent control plane."""

    def __init__(
        self,
        *,
        router: ProvisioningHttpRouterV1,
        config: ProvisioningHttp2ServerConfigV1,
        ssl_context: ssl.SSLContext,
    ) -> None:
        self._router = router
        self._config = config
        self._ssl_context = ssl_context
        self._server: asyncio.AbstractServer | None = None

    @property
    def addresses(self) -> tuple[tuple[str, int], ...]:
        """Return bound socket addresses after :meth:`start`."""

        if self._server is None:
            return ()
        sockets = cast(_ServerWithSocketsV1, self._server).sockets
        if sockets is None:
            return ()
        result: list[tuple[str, int]] = []
        for bound_socket in sockets:
            address = cast(object, bound_socket.getsockname())
            if isinstance(address, tuple):
                address_tuple = cast(tuple[object, ...], address)
                if (
                    len(address_tuple) >= 2
                    and isinstance(address_tuple[0], str)
                    and isinstance(address_tuple[1], int)
                ):
                    result.append((address_tuple[0], address_tuple[1]))
        return tuple(result)

    async def start(self) -> None:
        if self._server is not None:
            raise ProvisioningErrorV1("CONTROL_RUNTIME", "control listener is already started")
        self._server = await asyncio.start_server(
            self._handle_connection,
            host=self._config.host,
            port=self._config.port,
            ssl=self._ssl_context,
            ssl_handshake_timeout=10.0,
            start_serving=True,
        )

    async def close(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            facts = _tls_facts(writer)
        except ProvisioningErrorV1:
            writer.close()
            await writer.wait_closed()
            return
        connection = H2Connection(
            config=H2Configuration(client_side=False, header_encoding="ascii")
        )
        connection.initiate_connection()
        writer.write(connection.data_to_send())
        await writer.drain()
        streams: dict[int, _StreamRequestV1] = {}
        try:
            while not reader.at_eof():
                data = await reader.read(65_536)
                if not data:
                    break
                for event in connection.receive_data(data):
                    if isinstance(event, RequestReceived):
                        streams[event.stream_id] = _new_stream_request(
                            [(str(name), str(value)) for name, value in event.headers],
                            self._config,
                        )
                    elif isinstance(event, DataReceived):
                        stream = streams.get(event.stream_id)
                        if stream is not None:
                            if (
                                len(stream.body) + len(event.data)
                                > self._config.max_request_body_bytes
                            ):
                                stream.rejected = True
                            elif not stream.rejected:
                                stream.body.extend(event.data)
                        connection.acknowledge_received_data(
                            event.flow_controlled_length, event.stream_id
                        )
                    elif isinstance(event, StreamEnded):
                        stream = streams.pop(event.stream_id, None)
                        response = self._route(stream=stream, facts=facts)
                        _send_response(connection, event.stream_id, response)
                    elif isinstance(event, StreamReset):
                        streams.pop(event.stream_id, None)
                    elif isinstance(event, ConnectionTerminated):
                        return
                pending = connection.data_to_send()
                if pending:
                    writer.write(pending)
                    await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, asyncio.IncompleteReadError):
                pass

    def _route(
        self, *, stream: _StreamRequestV1 | None, facts: TlsConnectionFactsV1
    ) -> ProvisioningHttpResponseV1:
        if stream is None or stream.rejected:
            return _failure_response()
        try:
            decoded_raw: object = json.loads(stream.body.decode("utf-8")) if stream.body else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            return _failure_response()
        if not isinstance(decoded_raw, dict):
            return _failure_response()
        decoded_object = cast(dict[object, object], decoded_raw)
        if not all(isinstance(key, str) for key in decoded_object):
            return _failure_response()
        decoded = cast(Mapping[str, object], decoded_object)
        return self._router.handle(
            request=ProvisioningHttpRequestV1(
                method=stream.method,
                path=stream.path,
                body=decoded,
                tls_facts=facts,
            ),
            now_wall_ns=time.time_ns(),
        )


def _tls_facts(writer: asyncio.StreamWriter) -> TlsConnectionFactsV1:
    ssl_object = writer.get_extra_info("ssl_object")
    if not isinstance(ssl_object, ssl.SSLObject):
        raise ProvisioningErrorV1("CONTROL_AUTH", "control TLS handshake is unavailable")
    certificate = ssl_object.getpeercert(binary_form=True)
    if not isinstance(certificate, bytes):
        raise ProvisioningErrorV1("CONTROL_AUTH", "control client certificate is unavailable")
    return TlsConnectionFactsV1(
        tls_version=ssl_object.version() or "",
        alpn_protocol=ssl_object.selected_alpn_protocol() or "",
        peer_certificate_der=certificate,
        chain_verified=True,
    )


def _new_stream_request(
    headers: Sequence[tuple[str, str]], config: ProvisioningHttp2ServerConfigV1
) -> _StreamRequestV1:
    total_header_bytes = sum(
        len(name.encode("ascii")) + len(value.encode("utf-8")) for name, value in headers
    )
    if total_header_bytes > config.max_request_header_bytes:
        return _StreamRequestV1(method="", path="", body=bytearray(), rejected=True)
    values: Mapping[str, str] = dict(headers)
    method, path = values.get(":method"), values.get(":path")
    content_type = values.get("content-type")
    rejected = (
        method is None
        or path is None
        or any(name.lower() != name for name, _value in headers)
        or (method == "POST" and content_type != "application/json")
    )
    return _StreamRequestV1(
        method=method or "",
        path=path or "",
        body=bytearray(),
        rejected=rejected,
    )


def _send_response(
    connection: H2Connection, stream_id: int, response: ProvisioningHttpResponseV1
) -> None:
    body = json.dumps(
        dict(response.body), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("ascii")
    connection.send_headers(
        stream_id,
        [
            (":status", str(response.status_code)),
            ("content-type", "application/json"),
            ("content-length", str(len(body))),
            ("cache-control", "no-store"),
        ],
    )
    connection.send_data(stream_id, body, end_stream=True)


def _failure_response() -> ProvisioningHttpResponseV1:
    return ProvisioningHttpResponseV1(
        status_code=400,
        body={"request_id": None, "code": "CONTROL_REQUEST", "retryable": False},
    )
