"""Linux AF_UNIX stream assembly and peer-credential primitives for Consumer V2."""

from __future__ import annotations

import json
import os
import socket
import stat
import struct
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from threading import Lock, local
from typing import Callable, Generator, Mapping, Protocol, cast

from jsonschema import Draft202012Validator

from ..domain import ConsumerApiVersion, ConsumerPrincipal
from ..domain._validation import DomainValidationError, require_nonnegative
from .base import (
    ConsumerGatewayError,
    ConsumerSession,
    ConsumerVersionRange,
    PositionedConsumerEventV2,
)
from .cold_admission_v2 import ConsumerColdLookupRequiredV2
from .dispatch_v2 import ConsumerWireDispatchError
from .gateway_v2 import ConsumerGatewayV2
from .projection_reads_v2 import (
    PROJECTION_READ_OPERATIONS,
    ConsumerProjectionReadErrorV2,
    projection_read_scope,
)
from .replay_v2 import ConsumerCursorReplayError
from .request_trace_v2 import mark_request_failure, mark_request_stage, mark_response_kind
from .snapshot_index_v2 import ConsumerPagedSnapshotErrorV2, cached_snapshot_only
from .subscription_v2 import ConsumerLiveSubscriptionV2, ConsumerSubscriptionClosedV2
from .wire_v2 import (
    HEADER_SIZE_BYTES,
    MAX_FRAME_BYTES,
    ConsumerWireError,
    DecodedFrameV2,
    MessageKind,
    decode_frame,
    encode_frame,
    validate_payload,
)

_CURRENT_VERSION_RANGE = ConsumerVersionRange(
    ConsumerApiVersion(2, 0), ConsumerApiVersion(2, 0), 2, 2
)

_HEADER_LENGTH_OFFSET = 24
_UINT32 = struct.Struct(">I")
_UCRED = struct.Struct("3i")
_DEFERRED_READ_OPERATIONS = (
    frozenset({"ReadOrderSnapshotPage", "CreateOrderSnapshot"}) | PROJECTION_READ_OPERATIONS
)
# This shortcut is valid only for the exact, reviewed Consumer V2 request
# schema. A contract change automatically restores full JSON Schema validation.
_SNAPSHOT_REQUEST_SCHEMA_SHA256 = "69aeb520e9926e00f39da20a7b470da8a07f38100d0f8312e692fe4e1c7336d7"


def _matches_snapshot_request_schema(schema: Mapping[str, object]) -> bool:
    try:
        canonical = json.dumps(
            schema, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    except (TypeError, ValueError):
        return False
    return sha256(canonical).hexdigest() == _SNAPSHOT_REQUEST_SCHEMA_SHA256


def _valid_snapshot_request(payload: Mapping[str, object]) -> bool:
    """Accept only the exact valid subset described by the pinned schema.

    Any mismatch falls through to JSON Schema so error codes and rejection
    semantics remain unchanged. The wire decoder already guarantees a JSON
    object, but this function also handles arbitrary test callers safely.
    """

    if not isinstance(payload, dict) or payload.keys() != {
        "message_type",
        "schema_version",
        "operation",
        "request_id",
        "payload",
    }:
        return False
    request_id = payload["request_id"]
    body_raw = payload["payload"]
    if (
        payload["message_type"] != "request"
        or type(payload["schema_version"]) is not int
        or payload["schema_version"] != 2
        or payload["operation"] != "GetAccountOrderSnapshot"
        or not isinstance(request_id, str)
        or not 1 <= len(request_id) <= 128
        or not isinstance(body_raw, dict)
        or body_raw.keys()
        not in (
            {"consumer_account_id"},
            {"consumer_account_id", "reset_context"},
        )
    ):
        return False
    body = cast(Mapping[str, object], body_raw)
    account_id = body["consumer_account_id"]
    if not isinstance(account_id, str) or not 1 <= len(account_id) <= 128:
        return False
    if "reset_context" not in body:
        return True
    reset = body["reset_context"]
    return isinstance(reset, str) and 1 <= len(reset) <= 128


def _valid_submit_request(payload: Mapping[str, object]) -> bool:
    """Validate the common SubmitSignal shape under the pinned schema.

    Any unusual shape still uses JSON Schema. This shortcut cannot weaken
    validation when the contract digest changes.
    """

    if not isinstance(payload, dict) or payload.keys() != {
        "message_type",
        "schema_version",
        "operation",
        "request_id",
        "payload",
    }:
        return False
    request_id = payload["request_id"]
    body = payload["payload"]
    if (
        payload["message_type"] != "request"
        or type(payload["schema_version"]) is not int
        or payload["schema_version"] != 2
        or payload["operation"] != "SubmitSignal"
        or not isinstance(request_id, str)
        or not 1 <= len(request_id) <= 128
        or not isinstance(body, dict)
        or not body.keys()
        >= {
            "consumer_account_id",
            "strategy_id",
            "strategy_order_id",
            "trading_day",
            "symbol",
            "market",
            "side",
            "price_type",
            "business_type",
            "quantity",
            "protection_price_ticks",
        }
        or not body.keys()
        <= {
            "consumer_account_id",
            "strategy_id",
            "strategy_order_id",
            "trading_day",
            "symbol",
            "market",
            "side",
            "price_type",
            "business_type",
            "quantity",
            "protection_price_ticks",
            "limit_price_ticks",
            "execution_parameters",
        }
    ):
        return False
    fields = cast(Mapping[str, object], body)
    for name in ("consumer_account_id", "strategy_id", "strategy_order_id"):
        value = fields[name]
        if not isinstance(value, str) or not 1 <= len(value) <= 128:
            return False
    for name in ("symbol", "market", "side", "price_type", "business_type"):
        value = fields[name]
        if not isinstance(value, str) or not 1 <= len(value) <= 64:
            return False
    day = fields["trading_day"]
    if (
        not isinstance(day, str)
        or len(day) != 10
        or day[4] != "-"
        or day[7] != "-"
        or any(day[index] < "0" or day[index] > "9" for index in (0, 1, 2, 3, 5, 6, 8, 9))
    ):
        return False
    quantity, protection = fields["quantity"], fields["protection_price_ticks"]
    if not (
        type(quantity) is int and quantity >= 1 and type(protection) is int and protection >= 0
    ):
        return False
    if "limit_price_ticks" in fields:
        limit = fields["limit_price_ticks"]
        if limit is not None and (type(limit) is not int or limit < 0):
            return False
    if "execution_parameters" in fields:
        parameters = fields["execution_parameters"]
        if not isinstance(parameters, dict):
            return False
        typed_parameters = cast(Mapping[object, object], parameters)
        if len(typed_parameters) > 32:
            return False
        if any(
            not isinstance(key, str)
            or not 1 <= len(key) <= 128
            or not isinstance(value, str)
            or len(value) > 512
            for key, value in typed_parameters.items()
        ):
            return False
    return True


def _valid_poll_request(payload: Mapping[str, object]) -> bool:
    """Pinned-schema positive shortcut; all mismatches use the full validator."""
    if not isinstance(payload, dict) or payload.keys() != {
        "message_type",
        "schema_version",
        "operation",
        "request_id",
        "payload",
    }:
        return False
    body, request_id = payload["payload"], payload["request_id"]
    if (
        payload["message_type"] != "request"
        or type(payload["schema_version"]) is not int
        or payload["schema_version"] != 2
        or payload["operation"] != "PollEvents"
        or not isinstance(request_id, str)
        or not 1 <= len(request_id) <= 128
        or not isinstance(body, dict)
        or body.keys() != {"cursor", "limit"}
    ):
        return False
    fields = cast(Mapping[str, object], body)
    cursor, limit = fields["cursor"], fields["limit"]
    return (
        isinstance(cursor, str)
        and 1 <= len(cursor) <= 4096
        and type(limit) is int
        and 1 <= limit <= 10_000
    )


def _valid_paged_snapshot_request(payload: Mapping[str, object]) -> bool:
    """Pinned-schema shortcut for bounded snapshot create/page operations."""

    if not isinstance(payload, dict) or payload.keys() != {
        "message_type",
        "schema_version",
        "operation",
        "request_id",
        "payload",
    }:
        return False
    body, request_id, operation = (
        payload["payload"],
        payload["request_id"],
        payload["operation"],
    )
    if (
        payload["message_type"] != "request"
        or type(payload["schema_version"]) is not int
        or payload["schema_version"] != 2
        or not isinstance(request_id, str)
        or not 1 <= len(request_id) <= 128
        or not isinstance(body, dict)
    ):
        return False
    fields = cast(Mapping[str, object], body)
    if operation == "ReadOrderSnapshotPage":
        cursor = fields.get("page_cursor")
        return (
            set(fields) == {"page_cursor"} and isinstance(cursor, str) and 1 <= len(cursor) <= 4_096
        )
    if operation == "CreateOrderSnapshot":
        account_id, page_size = fields.get("consumer_account_id"), fields.get("page_size")
        return (
            set(fields) == {"consumer_account_id", "page_size"}
            and isinstance(account_id, str)
            and 1 <= len(account_id) <= 128
            and type(page_size) is int
            and 1 <= page_size <= 200
        )
    return False


class ConsumerUnixSocketError(DomainValidationError):
    """The local Consumer V2 transport boundary cannot be trusted."""


@dataclass(frozen=True)
class ConsumerUnixPeerV2:
    pid: int
    uid: int
    gid: int

    def __post_init__(self) -> None:
        for value, name in ((self.pid, "peer pid"), (self.uid, "peer uid"), (self.gid, "peer gid")):
            require_nonnegative(value, name)


class ConsumerWireStreamDecoderV2:
    """Bounded assembler that never emits a partial frame."""

    def __init__(self) -> None:
        self._buffer = bytearray()

    def feed(self, chunk: bytes) -> tuple[DecodedFrameV2, ...]:
        if not chunk:
            raise ConsumerUnixSocketError("consumer socket chunk must be non-empty immutable bytes")
        if len(self._buffer) + len(chunk) > MAX_FRAME_BYTES:
            raise ConsumerUnixSocketError("consumer socket assembly exceeds maximum frame size")
        self._buffer.extend(chunk)
        frames: list[DecodedFrameV2] = []
        while len(self._buffer) >= HEADER_SIZE_BYTES:
            payload_length = _UINT32.unpack(
                self._buffer[_HEADER_LENGTH_OFFSET : _HEADER_LENGTH_OFFSET + 4]
            )[0]
            frame_length = HEADER_SIZE_BYTES + payload_length
            if frame_length > MAX_FRAME_BYTES:
                raise ConsumerUnixSocketError("consumer socket frame exceeds maximum size")
            if len(self._buffer) < frame_length:
                break
            frame = bytes(self._buffer[:frame_length])
            del self._buffer[:frame_length]
            frames.append(decode_frame(frame))
        return tuple(frames)

    @property
    def pending_bytes(self) -> int:
        return len(self._buffer)


def read_linux_peer_credentials(connection: socket.socket) -> ConsumerUnixPeerV2:
    """Read immutable Linux SO_PEERCRED data; unsupported hosts fail closed."""

    if not hasattr(socket, "SO_PEERCRED"):
        raise ConsumerUnixSocketError("Consumer V2 requires Linux SO_PEERCRED")
    try:
        raw = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, _UCRED.size)
    except OSError as exc:
        raise ConsumerUnixSocketError("Consumer V2 peer credentials are unavailable") from exc
    if len(raw) != _UCRED.size:
        raise ConsumerUnixSocketError("Consumer V2 peer credentials are malformed")
    return ConsumerUnixPeerV2(*_UCRED.unpack(raw))


class ConsumerUnixRequestHandlerV2(Protocol):
    """Application dispatcher isolated from the connection and socket lifecycle."""

    def __call__(
        self,
        session: ConsumerSession,
        request: Mapping[str, object],
    ) -> Mapping[str, object]: ...


class ConsumerUnixLiveSubscriptionFactoryV2(Protocol):
    """Authorize one push subscription after normal Consumer V2 validation."""

    def __call__(
        self, session: ConsumerSession, request: Mapping[str, object]
    ) -> ConsumerLiveSubscriptionV2: ...


class AuthorizedOwnerSubmissionV1:
    """Process-local, single-frame lease; never an IPC or pickle payload."""

    def __init__(self, run: Callable[[], bytes], close: Callable[[], None]) -> None:
        self._run, self._close = run, close
        self._closed = False
        self._consumed = False
        self._attempts = 0

    def run(self) -> bytes:
        if self._closed or self._consumed or self._attempts >= 4:
            raise ConsumerUnixSocketError("owner submission authorization lease is closed")
        self._attempts += 1
        try:
            result = self._run()
        except ConsumerColdLookupRequiredV2:
            raise  # only this proven pre-commit suspension permits another attempt
        except BaseException:
            self._consumed = True
            raise
        self._consumed = True
        return result

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._close()


class ConsumerUnixSocketListenerV2:
    """Owned AF_UNIX/SOCK_STREAM listener with complete-frame request admission."""

    def __init__(
        self,
        *,
        socket_path: Path,
        gateway: ConsumerGatewayV2,
        principal_for_peer: Callable[[ConsumerUnixPeerV2], ConsumerPrincipal],
        request_schema: Mapping[str, object],
        request_handler: ConsumerUnixRequestHandlerV2,
        live_subscription_for: ConsumerUnixLiveSubscriptionFactoryV2 | None = None,
        connection_timeout_seconds: float = 5.0,
        max_requests_per_connection: int = 1_024,
        live_poll_interval_seconds: float = 0.05,
        remove_verified_stale_socket: bool = False,
        listen_backlog: int = 4_096,
        parallel_read_operations: frozenset[str] = frozenset(),
        socket_mode: int = 0o600,
        inline_cached_snapshot_pages: bool = False,
        allow_socket_writes: bool = True,
        allow_socket_reads: bool = True,
    ) -> None:
        if not socket_path.is_absolute():
            raise ConsumerUnixSocketError("Consumer V2 socket path must be absolute")
        if connection_timeout_seconds <= 0:
            raise ConsumerUnixSocketError("Consumer V2 connection timeout must be positive")
        if max_requests_per_connection <= 0:
            raise ConsumerUnixSocketError("Consumer V2 request limit must be positive")
        if live_poll_interval_seconds <= 0:
            raise ConsumerUnixSocketError("Consumer V2 live poll interval must be positive")
        if listen_backlog <= 0:
            raise ConsumerUnixSocketError("Consumer V2 listen backlog must be positive")
        if socket_mode not in {0o600, 0o660}:
            raise ConsumerUnixSocketError("Consumer V2 socket mode must be 0600 or 0660")
        if type(inline_cached_snapshot_pages) is not bool:
            raise ConsumerUnixSocketError("invalid inline snapshot policy")
        self._inline_cached_snapshot_pages = inline_cached_snapshot_pages
        if type(allow_socket_writes) is not bool:
            raise ConsumerUnixSocketError("invalid socket WRITE policy")
        self._allow_socket_writes = allow_socket_writes
        self._allow_socket_reads = allow_socket_reads
        self._path = socket_path
        self._gateway = gateway
        self._principal_for_peer = principal_for_peer
        self._request_schema = request_schema
        self._fast_request_validation = _matches_snapshot_request_schema(request_schema)
        self._request_validator_local = local()
        self._request_handler = request_handler
        self._live_subscription_for = live_subscription_for
        self._connection_timeout_seconds = connection_timeout_seconds
        self._max_requests_per_connection = max_requests_per_connection
        self._live_poll_interval_seconds = live_poll_interval_seconds
        self._remove_verified_stale_socket = remove_verified_stale_socket
        self._listen_backlog = listen_backlog
        self._parallel_read_operations = parallel_read_operations
        self._socket_mode = socket_mode
        # Mutating OMS operations remain ordered. Explicitly opted-in immutable
        # projections may bypass this lock after their own consistency review.
        self._dispatch_lock = Lock()
        self._listener: socket.socket | None = None
        self._owned_socket_identity: tuple[int, int] | None = None

    def start(self) -> None:
        """Bind a new protected socket; stale paths are never silently replaced."""

        if self._listener is not None:
            raise ConsumerUnixSocketError("Consumer V2 listener is already running")
        parent = self._path.parent
        try:
            metadata = parent.stat()
        except OSError as exc:
            raise ConsumerUnixSocketError("Consumer V2 socket parent is unavailable") from exc
        unsafe_parent_bits = 0o077 if self._socket_mode == 0o600 else 0o027
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or parent.is_symlink()
            or metadata.st_mode & unsafe_parent_bits
            or (hasattr(os, "geteuid") and metadata.st_uid != os.geteuid())
            or (
                self._socket_mode == 0o660
                and hasattr(os, "getegid")
                and metadata.st_gid != os.getegid()
            )
        ):
            raise ConsumerUnixSocketError("Consumer V2 socket parent permissions are unsafe")
        if self._path.exists() or self._path.is_symlink():
            if not self._remove_verified_stale_socket:
                raise ConsumerUnixSocketError("Consumer V2 socket path already exists")
            self._remove_stale_socket()
        if not hasattr(socket, "SO_PEERCRED"):
            raise ConsumerUnixSocketError("Consumer V2 listener requires Linux SO_PEERCRED")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(self._path))
            self._path.chmod(self._socket_mode)
            listener.listen(self._listen_backlog)
            listener.settimeout(0.2)
            socket_metadata = self._path.lstat()
            self._owned_socket_identity = (socket_metadata.st_dev, socket_metadata.st_ino)
        except Exception:
            listener.close()
            raise
        self._listener = listener

    def close(self) -> None:
        listener, self._listener = self._listener, None
        if listener is not None:
            listener.close()
        owned, self._owned_socket_identity = self._owned_socket_identity, None
        if owned is not None:
            try:
                metadata = self._path.lstat()
            except FileNotFoundError:
                return
            if (
                stat.S_ISSOCK(metadata.st_mode)
                and (metadata.st_dev, metadata.st_ino) == owned
                and not self._path.is_symlink()
            ):
                self._path.unlink()

    @property
    def listening_socket(self) -> socket.socket:
        """Expose the owned listener to the service's bounded readiness loop."""

        if self._listener is None:
            raise ConsumerUnixSocketError("Consumer V2 listener is not running")
        return self._listener

    def serve_once(self) -> None:
        """Serve exactly one connection; caller owns concurrency and shutdown policy."""

        if self._listener is None:
            raise ConsumerUnixSocketError("Consumer V2 listener is not running")
        try:
            connection, principal = self.accept_connection()
        except ConsumerUnixSocketError:
            return
        self.serve_authenticated_connection(connection, principal)

    def accept_connection(self) -> tuple[socket.socket, ConsumerPrincipal]:
        """Authenticate before admitting a connection to the bounded worker queue."""

        if self._listener is None:
            raise ConsumerUnixSocketError("Consumer V2 listener is not running")
        connection, _ = self._listener.accept()
        try:
            peer = read_linux_peer_credentials(connection)
            principal = self._principal_for_peer(peer)
        except (ConsumerUnixSocketError, OSError):
            connection.close()
            raise
        return connection, principal

    def accepted_connection_installed(self, connection: socket.socket) -> None:
        """Hook for an inherited-FD listener; direct accept needs no ACK."""

        del connection

    def serve_authenticated_connection(
        self,
        connection: socket.socket,
        principal: ConsumerPrincipal,
        *,
        initial_bytes: bytes = b"",
    ) -> None:
        """Serve one already-authorized connection; always release its session."""

        with connection:
            connection.settimeout(self._connection_timeout_seconds)
            session: ConsumerSession | None = None
            try:
                session = self._gateway.open_session(
                    principal,
                    _CURRENT_VERSION_RANGE,
                )
                self._serve_connection(connection, session, initial_bytes=initial_bytes)
            except (ConsumerUnixSocketError, ConsumerWireError, ConsumerGatewayError, OSError):
                return
            finally:
                if session is not None:
                    self._gateway.close_session(session)

    @property
    def supports_request_reactor(self) -> bool:
        return self._live_subscription_for is None

    @property
    def request_limit(self) -> int:
        return self._max_requests_per_connection

    @property
    def connection_timeout_seconds(self) -> float:
        return self._connection_timeout_seconds

    def open_authenticated_session(self, principal: ConsumerPrincipal) -> ConsumerSession:
        return self._gateway.open_session(
            principal,
            _CURRENT_VERSION_RANGE,
        )

    def close_authenticated_session(self, session: ConsumerSession) -> None:
        self._gateway.close_session(session)

    def validate_owner_frontend_request(
        self, session: ConsumerSession, frame: DecodedFrameV2
    ) -> None:
        """Stateless public precheck; no local Book/handler/side effects."""
        self._require_socket_write_policy(frame)
        self._validate_request_frame(frame)
        if frame.payload.get("operation") not in {"SubmitSignal", "CancelOrder"}:
            raise ConsumerUnixSocketError("owner frontend requires WRITE")
        self.validate_owner_frontend_delivery(session)

    def validate_owner_frontend_delivery(self, session: ConsumerSession) -> None:
        self._gateway.validate_current_session(session)

    def validate_owner_frontend_read(self, session: ConsumerSession, frame: DecodedFrameV2) -> None:
        self._validate_request_frame(frame)
        if frame.payload.get("operation") not in {
            "ListAccounts",
            "GetOrder",
            "GetOrderAuditTrail",
            "GetAccountOrderSnapshot",
            "CreateOrderSnapshot",
            "ReadOrderSnapshotPage",
            "PollEvents",
            "CreateReplayPartitions",
        }:
            raise ConsumerUnixSocketError("owner frontend read operation not reviewed")
        self._gateway.validate_current_session(session)

    @contextmanager
    def authorize_owner_read(
        self, principal: ConsumerPrincipal, frame: DecodedFrameV2, *, authorization_epoch: int
    ) -> Generator[ConsumerSession, None, None]:
        self._validate_request_frame(frame)
        if frame.payload.get("operation") not in {
            "ListAccounts",
            "GetOrder",
            "GetOrderAuditTrail",
            "GetAccountOrderSnapshot",
            "CreateOrderSnapshot",
            "ReadOrderSnapshotPage",
            "PollEvents",
            "CreateReplayPartitions",
        }:
            raise ConsumerUnixSocketError("owner read operation not reviewed")
        session = self._gateway.open_transient_session(principal, _CURRENT_VERSION_RANGE)
        try:
            if session.binding_epoch != authorization_epoch:
                raise ConsumerUnixSocketError("owner read authorization epoch changed")
            self._gateway.validate_current_session(session)
            yield session
        finally:
            self._gateway.close_transient_session(session)

    def can_defer_read(self, frame: DecodedFrameV2) -> bool:
        # Never infer safety from a caller-supplied operation name alone. Both
        # the transport allowlist and composition's reviewed lock bypass apply.
        operation = frame.payload.get("operation")
        return (
            isinstance(operation, str)
            and operation in _DEFERRED_READ_OPERATIONS
            and operation in self._parallel_read_operations
        )

    def try_inline_cached_read(
        self, session: ConsumerSession, frame: DecodedFrameV2
    ) -> bytes | None:
        if (
            not self._inline_cached_snapshot_pages
            or frame.payload.get("operation") != "ReadOrderSnapshotPage"
        ):
            return None
        payload = frame.payload.get("payload")
        cursor = (
            cast(dict[str, object], payload).get("page_cursor")
            if isinstance(payload, dict)
            else None
        )
        if not isinstance(cursor, str) or not self._gateway.cached_snapshot_page_ready(
            session, cursor
        ):
            return None
        # Hint races must fail closed, never turn into a SQL read or expiry
        # cleanup on this socket owner. Final authorization/TTL fencing remains.
        with cached_snapshot_only():
            response = self.response_for_frame(session, frame)
        return self.validate_deferred_delivery(session, frame, response)

    def validate_deferred_read(self, session: ConsumerSession, frame: DecodedFrameV2) -> None:
        self._validate_request_frame(frame)
        if not self.can_defer_read(frame):
            raise ConsumerUnixSocketError("operation is not a reviewed deferred read")
        self._gateway.validate_current_session(session)

    def validate_deferred_delivery(
        self, session: ConsumerSession, frame: DecodedFrameV2, response: bytes
    ) -> bytes:
        self._gateway.validate_current_session(session)
        operation = frame.payload.get("operation")
        if operation in PROJECTION_READ_OPERATIONS:
            decoded = decode_frame(response)
            if decoded.header.message_kind is MessageKind.ERROR:
                return response
            body = decoded.payload.get("payload")
            request = frame.payload.get("payload")
            if not isinstance(body, dict) or not isinstance(request, dict):
                raise ConsumerUnixSocketError("deferred query payload is missing")
            data = cast(dict[str, object], body)
            source = cast(dict[str, object], request)
            tokens: list[str] = []
            if operation == "PollEvents":
                for value in (source.get("cursor"), data.get("next_cursor")):
                    if not isinstance(value, str):
                        raise ConsumerUnixSocketError("deferred event cursor is missing")
                    tokens.append(value)
            elif operation == "CreateReplayPartitions":
                origin = source.get("after_cursor")
                if origin is not None:
                    if not isinstance(origin, str):
                        raise ConsumerUnixSocketError("deferred suffix cursor is invalid")
                    tokens.append(origin)
                partitions = data.get("partitions")
                if not isinstance(partitions, list) or len(cast(list[object], partitions)) > 64:
                    raise ConsumerUnixSocketError("deferred partitions are invalid")
                for part in cast(list[object], partitions):
                    if not isinstance(part, dict):
                        raise ConsumerUnixSocketError("deferred partition is invalid")
                    value = cast(dict[str, object], part).get("cursor")
                    if not isinstance(value, str):
                        raise ConsumerUnixSocketError("deferred partition cursor is missing")
                    tokens.append(value)
            try:
                self._gateway.validate_projection_read_delivery(
                    session, operation=cast(str, operation), tokens=tuple(tokens)
                )
            except (ConsumerCursorReplayError, ConsumerProjectionReadErrorV2) as exc:
                code = exc.code
                if operation == "GetOrderAuditTrail" and isinstance(
                    exc, ConsumerProjectionReadErrorV2
                ):
                    code = "AUDIT_EVIDENCE_UNAVAILABLE"
                mark_request_failure("deferred_delivery", code)
                mark_response_kind("ERROR")
                return encode_frame(
                    MessageKind.ERROR,
                    frame.header.correlation_id,
                    {
                        "message_type": "error",
                        "schema_version": 2,
                        "request_id": frame.payload["request_id"],
                        "code": code,
                        "retryable": code
                        in {
                            "INTERNAL_ERROR",
                            "CURSOR_RECOVERY_UNAVAILABLE",
                            "AUDIT_EVIDENCE_UNAVAILABLE",
                        },
                    },
                )
            return response
        if frame.payload.get("operation") == "CreateOrderSnapshot":
            # A create response is small metadata, not an order page. Its new
            # signed cursor supplies the generation/TTL fence without touching
            # files, expiring pins, or rebuilding the snapshot on this lane.
            decoded = decode_frame(response)
            if decoded.header.message_kind is MessageKind.ERROR:
                return response
            body = decoded.payload.get("payload")
        else:
            body = frame.payload.get("payload")
        cursor = (
            cast(Mapping[str, object], body).get("page_cursor") if isinstance(body, dict) else None
        )
        if not isinstance(cursor, str):
            raise ConsumerUnixSocketError("deferred page cursor is missing")
        try:
            self._gateway.validate_snapshot_page_delivery(session, cursor)
        except ConsumerPagedSnapshotErrorV2 as exc:
            mark_request_failure("deferred_delivery", exc.code)
            mark_response_kind("ERROR")
            return encode_frame(
                MessageKind.ERROR,
                frame.header.correlation_id,
                {
                    "message_type": "error",
                    "schema_version": 2,
                    "request_id": frame.payload["request_id"],
                    "code": exc.code,
                    "retryable": exc.retryable,
                },
            )
        return response

    def response_for_deferred_read(self, session: ConsumerSession, frame: DecodedFrameV2) -> bytes:
        self.validate_deferred_read(session, frame)
        operation = frame.payload.get("operation")
        with projection_read_scope(cast(str, operation)):
            return self.response_for_frame(session, frame)

    def response_for_single_request(
        self, principal: ConsumerPrincipal, frame: DecodedFrameV2
    ) -> bytes:
        """Authorize and dispatch one reactor request without a shared session entry."""

        session = self._gateway.open_transient_session(principal, _CURRENT_VERSION_RANGE)
        try:
            return self.response_for_frame(session, frame)
        finally:
            self._gateway.close_transient_session(session)

    def _validate_request_frame(self, frame: DecodedFrameV2) -> None:
        if frame.header.message_kind is not MessageKind.REQUEST:
            raise ConsumerUnixSocketError("Consumer V2 connection received a non-request frame")
        if not (
            self._fast_request_validation
            and (
                _valid_snapshot_request(frame.payload)
                or _valid_submit_request(frame.payload)
                or _valid_poll_request(frame.payload)
                or _valid_paged_snapshot_request(frame.payload)
            )
        ):
            validator = getattr(self._request_validator_local, "validator", None)
            if validator is None:
                validator = Draft202012Validator(dict(self._request_schema))
                self._request_validator_local.validator = validator
            validate_payload(frame.payload, self._request_schema, compiled_validator=validator)

    def response_for_submission(self, session: ConsumerSession, frame: DecodedFrameV2) -> bytes:
        """One schema precheck, then the unchanged authorized WRITE handler.

        Only errors from this pre-handler check are known not to have committed.
        Handler/storage/encoding failures must never be classified this way.
        """
        from .submit_lane_v2 import SubmissionPrecheckFailedV2

        try:
            self._require_socket_write_policy(frame)
            self._validate_request_frame(frame)
            if frame.payload.get("operation") not in {"SubmitSignal", "CancelOrder"}:
                raise ConsumerUnixSocketError("submission lane requires a WRITE operation")
        except (ConsumerUnixSocketError, ConsumerWireError):
            raise SubmissionPrecheckFailedV2() from None
        mark_request_stage("schema_validated_ns")
        return self._response_for_validated_frame(session, frame)

    def response_for_frame(self, session: ConsumerSession, frame: DecodedFrameV2) -> bytes:
        """Run the same authorization, OMS commit and receipt path for a ready frame."""
        self._require_socket_write_policy(frame)
        self._validate_request_frame(frame)
        mark_request_stage("schema_validated_ns")
        return self._response_for_validated_frame(session, frame)

    def _require_socket_write_policy(self, frame: DecodedFrameV2) -> None:
        if not self._allow_socket_reads and frame.payload.get("operation") not in {
            "SubmitSignal",
            "CancelOrder",
        }:
            raise ConsumerUnixSocketError("socket READ disabled; inherited read IPC required")
        if not self._allow_socket_writes and frame.payload.get("operation") in {
            "SubmitSignal",
            "CancelOrder",
        }:
            raise ConsumerUnixSocketError("socket WRITE disabled; inherited owner IPC required")

    def response_for_owner_read(self, session: ConsumerSession, frame: DecodedFrameV2) -> bytes:
        """Only the inherited read actor uses this after authorize_owner_read."""
        if self.can_defer_read(frame):
            with projection_read_scope(cast(str, frame.payload["operation"])):
                return self._response_for_validated_frame(session, frame)
        return self._response_for_validated_frame(session, frame)

    @contextmanager
    def authorize_owner_submission(
        self, principal: ConsumerPrincipal, frame: DecodedFrameV2, *, authorization_epoch: int
    ) -> Generator[AuthorizedOwnerSubmissionV1, None, None]:
        """Owner checks schema/current principal once, retries only cold pre-commit.

        This private composition port does not authorize a request merely from
        its operation label. Every run retains the unchanged gateway/account
        checks; no resolver/session/lease is serialized back to the frontend.
        """
        self._validate_request_frame(frame)
        if frame.payload.get("operation") not in {"SubmitSignal", "CancelOrder"}:
            raise ConsumerUnixSocketError("owner submission requires a WRITE operation")
        session = self._gateway.open_transient_session(principal, _CURRENT_VERSION_RANGE)

        def run() -> bytes:
            self._gateway.validate_current_session(session)
            return self._response_for_validated_frame(session, frame)

        lease = AuthorizedOwnerSubmissionV1(
            run, lambda: self._gateway.close_transient_session(session)
        )
        try:
            if session.binding_epoch != authorization_epoch:
                raise ConsumerUnixSocketError("owner submission authorization epoch changed")
            self._gateway.validate_current_session(session)
            mark_request_stage("schema_validated_ns")
            yield lease
        finally:
            lease.close()

    def _response_for_validated_frame(
        self, session: ConsumerSession, frame: DecodedFrameV2
    ) -> bytes:
        try:
            operation = frame.payload.get("operation")
            if isinstance(operation, str) and operation in self._parallel_read_operations:
                response = self._request_handler(session, frame.payload)
            else:
                with self._dispatch_lock:
                    response = self._request_handler(session, frame.payload)
        except ConsumerWireDispatchError as exc:
            response = {
                "message_type": "error",
                "schema_version": 2,
                "request_id": frame.payload["request_id"],
                "code": exc.code,
                "retryable": exc.retryable,
            }
            kind = MessageKind.ERROR
        else:
            kind = MessageKind.RESPONSE
        mark_request_stage("handler_finished_ns")
        mark_response_kind(kind.name)
        try:
            encoded = encode_frame(kind, frame.header.correlation_id, response)
        except ConsumerWireError as exc:
            # The request has already been authenticated and carries a safe
            # request id. Return a small deterministic error rather than
            # silently closing after expensive response construction.
            mark_request_failure("response_encode", exc.code)
            kind = MessageKind.ERROR
            mark_response_kind(kind.name)
            encoded = encode_frame(
                kind,
                frame.header.correlation_id,
                {
                    "message_type": "error",
                    "schema_version": 2,
                    "request_id": frame.payload["request_id"],
                    "code": exc.code,
                    "retryable": False,
                },
            )
        mark_request_stage("response_encoded_ns")
        return encoded

    def _serve_connection(
        self, connection: socket.socket, session: ConsumerSession, *, initial_bytes: bytes = b""
    ) -> None:
        decoder = ConsumerWireStreamDecoderV2()
        requests_served = 0
        while True:
            chunk = initial_bytes or self._receive_without_ancillary(connection)
            initial_bytes = b""
            if not chunk:
                return
            for frame in decoder.feed(chunk):
                requests_served += 1
                if requests_served > self._max_requests_per_connection:
                    raise ConsumerUnixSocketError("Consumer V2 connection request limit exceeded")
                self._validate_request_frame(frame)
                if (
                    self._live_subscription_for is not None
                    and frame.payload.get("operation") == "SubscribeOrderEvents"
                ):
                    with self._dispatch_lock:
                        live = self._live_subscription_for(session, frame.payload)
                    response = {
                        "message_type": "response",
                        "schema_version": 2,
                        "request_id": frame.payload["request_id"],
                        "status": "COMPLETED",
                        "payload": {"subscription": "LIVE"},
                    }
                    connection.sendall(
                        encode_frame(MessageKind.RESPONSE, frame.header.correlation_id, response)
                    )
                    self._serve_live_subscription(
                        connection, live=live, request_id=frame.payload["request_id"]
                    )
                    return
                connection.sendall(self._response_for_validated_frame(session, frame))

    def _serve_live_subscription(
        self,
        connection: socket.socket,
        *,
        live: ConsumerLiveSubscriptionV2,
        request_id: object,
    ) -> None:
        if not isinstance(request_id, str):
            raise ConsumerUnixSocketError("Consumer V2 live request ID is invalid")
        connection.settimeout(self._live_poll_interval_seconds)
        try:
            while True:
                try:
                    items = live.drain(limit=64)
                except ConsumerSubscriptionClosedV2:
                    error = {
                        "message_type": "error",
                        "schema_version": 2,
                        "request_id": request_id,
                        "code": "EVENT_BACKPRESSURE",
                        "retryable": True,
                    }
                    connection.sendall(encode_frame(MessageKind.ERROR, 0, error))
                    return
                for item in items:
                    connection.sendall(
                        encode_frame(MessageKind.EVENT, 0, self._live_event_payload(live, item))
                    )
                try:
                    incoming = connection.recv(1, socket.MSG_PEEK)
                except socket.timeout:
                    continue
                if not incoming:
                    return
                raise ConsumerUnixSocketError("Consumer V2 live subscription rejects client frames")
        finally:
            live.close()

    @staticmethod
    def _live_event_payload(
        live: ConsumerLiveSubscriptionV2, item: PositionedConsumerEventV2
    ) -> Mapping[str, object]:
        event = item.event
        return {
            "message_type": "event",
            "schema_version": 2,
            "event_id": event.consumer_event_id,
            "event_kind": event.event_kind.value,
            "position": item.position.to_external_id(),
            "payload": {
                "event_cursor": live.cursor_for_event(item),
                "journal_id": event.journal_id,
                "journal_lsn": event.journal_lsn,
            },
        }

    def _remove_stale_socket(self) -> None:
        """Remove only our own verified pathname socket after proving it is not listening."""

        try:
            metadata = self._path.lstat()
        except OSError as exc:
            raise ConsumerUnixSocketError("Consumer V2 socket path cannot be inspected") from exc
        if (
            self._path.is_symlink()
            or not stat.S_ISSOCK(metadata.st_mode)
            or (hasattr(os, "geteuid") and metadata.st_uid != os.geteuid())
        ):
            raise ConsumerUnixSocketError("Consumer V2 stale socket is not safely owned")
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.settimeout(0.1)
            probe.connect(str(self._path))
        except (ConnectionRefusedError, FileNotFoundError):
            pass
        except OSError as exc:
            raise ConsumerUnixSocketError(
                "Consumer V2 stale socket listener cannot be disproven"
            ) from exc
        else:
            raise ConsumerUnixSocketError("Consumer V2 socket path has a live listener")
        finally:
            probe.close()
        self._path.unlink()

    @staticmethod
    def _receive_without_ancillary(connection: socket.socket, maximum_bytes: int = 65_536) -> bytes:
        """Receive one bounded chunk and reject descriptor/credential control data."""

        if type(maximum_bytes) is not int or not 1 <= maximum_bytes <= 65_536:
            raise ConsumerUnixSocketError("invalid bounded receive size")
        if not hasattr(connection, "recvmsg"):
            # The production listener already refuses hosts without Linux
            # SO_PEERCRED.  This fallback keeps pure framing tests portable;
            # it is unreachable for a started Consumer V2 listener.
            return connection.recv(maximum_bytes)
        try:
            chunk, ancillary, flags, _ = connection.recvmsg(maximum_bytes, socket.CMSG_SPACE(64))
        except OSError:
            raise
        if ancillary or flags & getattr(socket, "MSG_CTRUNC", 0):
            # SCM_RIGHTS installs new receiver-owned descriptors even though
            # this protocol rejects control data. Close every returned FD,
            # including the visible prefix of a truncated control message.
            for level, kind, data in ancillary:
                if level == socket.SOL_SOCKET and kind == getattr(socket, "SCM_RIGHTS", None):
                    usable = len(data) - len(data) % struct.calcsize("=i")
                    for (descriptor,) in struct.iter_unpack("=i", data[:usable]):
                        try:
                            os.close(descriptor)
                        except OSError:
                            pass
            raise ConsumerUnixSocketError("Consumer V2 rejects ancillary socket data")
        return chunk
