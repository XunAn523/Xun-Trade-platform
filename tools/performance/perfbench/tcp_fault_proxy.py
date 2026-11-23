"""Loopback TCP proxy for bounded, local transport-fault fixtures.

The proxy never interprets, stores, or logs protocol bytes.  It can only
forward a TCP stream or close the currently connected pair.  This permits a
real SDK session to observe a local transport interruption without changing a
host firewall, routing table, or another process's network traffic.
"""

from __future__ import annotations

import os
import platform
import signal
import socket
import struct
from ctypes import CDLL, c_int
from dataclasses import dataclass
from threading import Condition, Event, Lock, RLock, Thread
from time import monotonic


class TcpFaultProxyError(RuntimeError):
    """The loopback proxy cannot establish or control a stream safely."""


@dataclass(frozen=True)
class TcpFaultProxySettings:
    target_host: str
    target_port: int
    connect_timeout_seconds: float = 5.0
    listen_port: int = 0

    def validate(self) -> None:
        if not self.target_host:
            raise TcpFaultProxyError("target_host is required")
        if not 1 <= self.target_port <= 65535:
            raise TcpFaultProxyError("target_port must be in 1..65535")
        if not 0.1 <= self.connect_timeout_seconds <= 30.0:
            raise TcpFaultProxyError("connect_timeout_seconds must be in 0.1..30.0")
        if not 0 <= self.listen_port <= 65535:
            raise TcpFaultProxyError("listen_port must be in 0..65535")


class _ConnectionPair:
    def __init__(self, client: socket.socket, upstream: socket.socket) -> None:
        self.client = client
        self.upstream = upstream
        self.closed = Event()
        self._lock = Lock()

    def close(self) -> None:
        with self._lock:
            if self.closed.is_set():
                return
            self.closed.set()
            for endpoint in (self.client, self.upstream):
                try:
                    endpoint.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    endpoint.close()
                except OSError:
                    pass

    def abort(self) -> None:
        """Force an RST instead of an orderly FIN for a fault-test stream."""

        with self._lock:
            if self.closed.is_set():
                return
            self.closed.set()
            for endpoint in (self.client, self.upstream):
                _abort_socket(endpoint)


def _abort_socket(endpoint: socket.socket) -> None:
    """Close one test socket with an abortive linger policy."""

    try:
        endpoint.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    except OSError:
        pass
    try:
        endpoint.close()
    except OSError:
        pass


class TcpFaultProxy:
    """A single-active-stream local proxy with an explicit close injection."""

    def __init__(self, settings: TcpFaultProxySettings) -> None:
        settings.validate()
        self._settings = settings
        self._listener: socket.socket | None = None
        self._accept_thread: Thread | None = None
        self._stopped = Event()
        self._lock = Lock()
        self._active: _ConnectionPair | None = None
        self._connection_condition = Condition(self._lock)
        self._connection_count = 0
        self._block_new_connections_until = 0.0

    @property
    def host(self) -> str:
        return "127.0.0.1"

    @property
    def port(self) -> int:
        with self._lock:
            if self._listener is None:
                raise TcpFaultProxyError("proxy is not running")
            return int(self._listener.getsockname()[1])

    @property
    def connection_count(self) -> int:
        with self._lock:
            return self._connection_count

    def start(self) -> None:
        with self._lock:
            if self._listener is not None:
                raise TcpFaultProxyError("proxy is already running")
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                listener.bind((self.host, self._settings.listen_port))
                listener.listen(4)
                listener.settimeout(0.1)
            except OSError as exc:
                listener.close()
                raise TcpFaultProxyError("cannot start loopback proxy") from exc
            self._listener = listener
        self._accept_thread = Thread(
            target=self._accept_loop, name="emt-perf-tcp-proxy", daemon=True
        )
        self._accept_thread.start()

    def wait_for_connection(self, minimum_count: int, timeout_seconds: float) -> bool:
        """Wait only for a new TCP pair; no application data is inspected."""

        if minimum_count < 1 or timeout_seconds <= 0:
            raise TcpFaultProxyError("minimum_count and timeout_seconds must be positive")
        deadline = monotonic() + timeout_seconds
        with self._connection_condition:
            while self._connection_count < minimum_count:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    return False
                self._connection_condition.wait(remaining)
            return True

    def disconnect_active(self, blackout_seconds: float = 0.0) -> bool:
        """Reset the active pair and optionally reject new pairs for one window."""

        if not 0.0 <= blackout_seconds <= 30.0:
            raise TcpFaultProxyError("disconnect blackout must be in 0..30 seconds")

        with self._lock:
            active = self._active
            self._block_new_connections_until = max(
                self._block_new_connections_until, monotonic() + blackout_seconds
            )
        if active is None:
            return False
        active.abort()
        return True

    def close(self) -> None:
        self._stopped.set()
        with self._lock:
            listener, active = self._listener, self._active
            self._listener = None
            self._active = None
            self._connection_condition.notify_all()
        if active is not None:
            active.close()
        if listener is not None:
            try:
                listener.close()
            except OSError:
                pass
        if self._accept_thread is not None:
            self._accept_thread.join(timeout=1.0)

    def __enter__(self) -> "TcpFaultProxy":
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _accept_loop(self) -> None:
        while not self._stopped.is_set():
            listener = self._listener
            if listener is None:
                return
            try:
                client, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            with self._lock:
                reject_for_blackout = monotonic() < self._block_new_connections_until
            if reject_for_blackout:
                _abort_socket(client)
                continue
            try:
                upstream = socket.create_connection(
                    (self._settings.target_host, self._settings.target_port),
                    timeout=self._settings.connect_timeout_seconds,
                )
            except OSError:
                try:
                    client.close()
                except OSError:
                    pass
                continue
            pair = _ConnectionPair(client, upstream)
            with self._connection_condition:
                previous = self._active
                self._active = pair
                self._connection_count += 1
                self._connection_condition.notify_all()
            if previous is not None:
                previous.close()
            for source, destination in ((client, upstream), (upstream, client)):
                Thread(target=self._pump, args=(pair, source, destination), daemon=True).start()

    def _pump(
        self, pair: _ConnectionPair, source: socket.socket, destination: socket.socket
    ) -> None:
        try:
            while not pair.closed.is_set():
                chunk = source.recv(65_536)
                if not chunk:
                    return
                destination.sendall(chunk)
        except OSError:
            return
        finally:
            pair.close()
            with self._lock:
                if self._active is pair:
                    self._active = None


class ProcessTcpFaultProxy:
    """Linux-only control plane for a loopback proxy outside a native caller.

    Some vendor Python bindings retain the GIL while their synchronous login
    waits for a protocol handshake.  A proxy thread in that same process then
    cannot accept the TCP connection, producing a test-only deadlock.  This
    helper puts the unchanged byte-blind ``TcpFaultProxy`` in a short-lived
    child process and exposes only fixed control commands to its parent.
    """

    def __init__(self, settings: TcpFaultProxySettings) -> None:
        settings.validate()
        if platform.system() != "Linux":
            raise TcpFaultProxyError("process fault proxy requires Linux")
        self._settings = settings
        self._control: socket.socket | None = None
        self._pid: int | None = None
        self._port: int | None = None
        self._buffer = b""
        self._lock = RLock()

    @property
    def host(self) -> str:
        return "127.0.0.1"

    @property
    def port(self) -> int:
        if self._port is None:
            raise TcpFaultProxyError("process proxy is not running")
        return self._port

    @property
    def connection_count(self) -> int:
        return self._command_int("count")

    def start(self) -> None:
        with self._lock:
            if self._pid is not None:
                raise TcpFaultProxyError("process proxy is already running")
            parent, child = socket.socketpair()
            child_pid = os.fork()
            if child_pid == 0:
                parent.close()
                _run_process_proxy_child(child, self._settings)
            child.close()
            self._control, self._pid = parent, child_pid
            try:
                response = self._receive_line(5.0)
                prefix, separator, value = response.partition(" ")
                if prefix != "ready" or not separator or not value.isdecimal():
                    raise TcpFaultProxyError("process proxy did not become ready")
                port = int(value)
                if not 1 <= port <= 65535:
                    raise TcpFaultProxyError("process proxy returned an invalid port")
                self._port = port
            except BaseException:
                self.close()
                raise

    def wait_for_connection(self, minimum_count: int, timeout_seconds: float) -> bool:
        if minimum_count < 1 or timeout_seconds <= 0:
            raise TcpFaultProxyError("minimum_count and timeout_seconds must be positive")
        milliseconds = max(1, round(timeout_seconds * 1000))
        return self._command_bool(f"wait {minimum_count} {milliseconds}")

    def disconnect_active(self, blackout_seconds: float = 0.0) -> bool:
        if not 0.0 <= blackout_seconds <= 30.0:
            raise TcpFaultProxyError("disconnect blackout must be in 0..30 seconds")
        milliseconds = round(blackout_seconds * 1000)
        return self._command_bool(f"disconnect {milliseconds}")

    def close(self) -> None:
        with self._lock:
            control, child_pid = self._control, self._pid
            self._control, self._pid, self._port, self._buffer = None, None, None, b""
        if control is None or child_pid is None:
            return
        try:
            control.sendall(b"close\n")
            self._receive_line_from(control, 2.0)
        except OSError:
            pass
        finally:
            control.close()
        deadline = monotonic() + 2.0
        while monotonic() < deadline:
            observed_pid, _status = os.waitpid(child_pid, os.WNOHANG)
            if observed_pid == child_pid:
                return
            Event().wait(0.02)
        try:
            os.kill(child_pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        os.waitpid(child_pid, 0)

    def __enter__(self) -> "ProcessTcpFaultProxy":
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def _command_int(self, command: str) -> int:
        response = self._command(command)
        if not response.isdecimal():
            raise TcpFaultProxyError("process proxy returned an invalid integer")
        return int(response)

    def _command_bool(self, command: str) -> bool:
        response = self._command(command)
        if response == "true":
            return True
        if response == "false":
            return False
        raise TcpFaultProxyError("process proxy returned an invalid boolean")

    def _command(self, command: str) -> str:
        with self._lock:
            control = self._control
            if control is None:
                raise TcpFaultProxyError("process proxy is not running")
            try:
                control.sendall(command.encode("ascii") + b"\n")
                return self._receive_line_from(control, 35.0)
            except OSError as exc:
                raise TcpFaultProxyError("process proxy control channel failed") from exc

    def _receive_line(self, timeout_seconds: float) -> str:
        control = self._control
        if control is None:
            raise TcpFaultProxyError("process proxy is not running")
        return self._receive_line_from(control, timeout_seconds)

    def _receive_line_from(self, control: socket.socket, timeout_seconds: float) -> str:
        previous_timeout = control.gettimeout()
        control.settimeout(timeout_seconds)
        try:
            while b"\n" not in self._buffer:
                chunk = control.recv(256)
                if not chunk:
                    raise TcpFaultProxyError("process proxy control channel closed")
                self._buffer += chunk
            raw, self._buffer = self._buffer.split(b"\n", 1)
            return raw.decode("ascii")
        except (UnicodeDecodeError, TimeoutError) as exc:
            raise TcpFaultProxyError("process proxy control response failed") from exc
        finally:
            control.settimeout(previous_timeout)


def _run_process_proxy_child(control: socket.socket, settings: TcpFaultProxySettings) -> None:
    """Run only the byte-blind proxy and leave with the parent process."""

    _set_parent_death_signal()
    # A transparent redirect shim belongs only to the native fixture process.
    # The proxy must reach the real configured endpoint rather than redirecting
    # its own upstream connection back into itself.
    os.environ["TRADECORE_SESSION_FAULT_CONNECT_REDIRECT_BYPASS"] = "1"
    proxy = TcpFaultProxy(settings)
    try:
        proxy.start()
        control.sendall(f"ready {proxy.port}\n".encode("ascii"))
        buffer = b""
        while True:
            chunk = control.recv(256)
            if not chunk:
                return
            buffer += chunk
            while b"\n" in buffer:
                raw, buffer = buffer.split(b"\n", 1)
                response, close = _process_proxy_command(proxy, raw)
                control.sendall(response + b"\n")
                if close:
                    return
    except BaseException:
        try:
            control.sendall(b"error\n")
        except OSError:
            pass
    finally:
        proxy.close()
        control.close()
        os._exit(0)


def _process_proxy_command(proxy: TcpFaultProxy, raw: bytes) -> tuple[bytes, bool]:
    """Decode a tiny fixed command set; it never carries protocol payloads."""

    if raw == b"count":
        return str(proxy.connection_count).encode("ascii"), False
    if raw == b"disconnect":
        return (b"true" if proxy.disconnect_active() else b"false"), False
    fields = raw.split(b" ")
    if len(fields) == 2 and fields[0] == b"disconnect" and fields[1].isdigit():
        milliseconds = int(fields[1])
        if 0 <= milliseconds <= 30_000:
            return (b"true" if proxy.disconnect_active(milliseconds / 1000) else b"false"), False
    if raw == b"close":
        return b"closed", True
    if len(fields) == 3 and fields[0] == b"wait" and all(field.isdigit() for field in fields[1:]):
        minimum_count, milliseconds = (int(field) for field in fields[1:])
        if minimum_count >= 1 and 1 <= milliseconds <= 30_000:
            observed = proxy.wait_for_connection(minimum_count, milliseconds / 1000)
            return (b"true" if observed else b"false"), False
    return b"error", False


def _set_parent_death_signal() -> None:
    """Make a hard-killed fixture parent clean up its proxy child as well."""

    parent_pid = os.getppid()
    try:
        result = CDLL(None, use_errno=True).prctl(c_int(1), c_int(signal.SIGTERM))
    except OSError:
        return
    if result != 0 or os.getppid() != parent_pid:
        os._exit(0)
