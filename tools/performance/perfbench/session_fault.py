"""Real EMT session transport-fault fixture through a local loopback proxy.

Only the endpoint between this fixture's SDK session and the configured EMT
counter is interrupted.  The driver never places orders, invokes queries, or
changes funds; it records payload-free C0--C7 SPI evidence only.
"""

from __future__ import annotations

import json
import os
import platform
import select
import signal
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Callable
from uuid import uuid4

from emt_gateway.gateway import CallbackPipeline
from emt_gateway.journal import FileJournal
from emt_gateway.platform_loader import EmtPlatformLoader, SdkLoadSettings
from emt_gateway.session import EmtSession, SessionError, SessionSettings

from .gateway_report import build_gateway_report
from .simulated_accounts import SimulatedAccounts
from .tcp_fault_proxy import (
    ProcessTcpFaultProxy,
    TcpFaultProxy,
    TcpFaultProxyError,
    TcpFaultProxySettings,
)
from .trace import GatewayTraceCollector


class SessionFaultError(RuntimeError):
    """The bounded real-session fault fixture cannot be constructed safely."""


_STRICT_CALLBACK_SEQUENCE = "documented_callback_sequence_v1"
_EMT_2_27_CLIENT_RST_AUTO_RECONNECT = "emt_2_27_client_rst_auto_reconnect_v1"
_ACCEPTANCE_STANDARDS = frozenset(
    {_STRICT_CALLBACK_SEQUENCE, _EMT_2_27_CLIENT_RST_AUTO_RECONNECT}
)
_EMT_2_27_API_VERSION = "2.27.0.0"
_EMT_2_27_LINUX_HASHES = {
    "libemt_api.so": "6eb8c354cea94bbde14074b891fe2c5979282fb72ceccf89bfe3f386126dd4c9",
    "vnemttrader.so": "d15c6343c45d844992212f987b2d63fd51fb27037991e89086cff367eebce9ee",
}


@dataclass(frozen=True)
class SessionFaultSettings:
    sdk_root: Path
    runtime_dir: Path
    accounts: SimulatedAccounts
    fixture_owner: str
    cycles: int = 1
    callback_timeout_seconds: float = 12.0
    interval_seconds: float = 1.0
    disconnect_blackout_seconds: float = 0.0
    client_id: int = 94
    trace_output: Path | None = None
    require_native_fastspi: bool = True
    # Used only by the CLI's throw-away acceptance process after its report
    # has been written.  Some Linux EMT binding finalizers block after a
    # deliberate transport interruption, so do not let cleanup erase the
    # fault evidence or widen the test into a host-level intervention.
    retain_native_reference: Callable[[object], None] | None = None
    # The supervisor itself never loads EMT.  On Linux it confines a native
    # abort after the intentionally interrupted loopback transport to a
    # child, then writes a payload-free failure artifact from the parent.
    isolate_native_worker: bool = False
    # The native EMT login call may retain the GIL while it waits for its
    # handshake.  Linux isolated acceptance therefore puts the byte-blind
    # loopback proxy in a sibling process rather than a Python thread.
    use_process_proxy: bool = False
    # ``transparent_redirect`` keeps the actual EMT endpoint in the native
    # Login call.  A test-only, per-process connect shim redirects only that
    # socket to the sibling loopback proxy.
    transport_mode: str = "loopback_proxy"
    proxy_listen_port: int = 0
    # The current EMT Linux 2.27.0.0 binary has a version-scoped, real-RST
    # baseline.  It accepts transport reconnect plus a recovery OnConnected;
    # the absent OnDisconnected is retained as an observation, never forged.
    acceptance_standard: str = _EMT_2_27_CLIENT_RST_AUTO_RECONNECT

    def validate(self) -> None:
        if not self.fixture_owner.strip():
            raise SessionFaultError("fixture_owner is required for a real session fault fixture")
        if not 1 <= self.cycles <= 3:
            raise SessionFaultError("session fault cycles must be in 1..3")
        if not 3.0 <= self.callback_timeout_seconds <= 30.0:
            raise SessionFaultError("callback_timeout_seconds must be in 3..30")
        if not 1.0 <= self.interval_seconds <= 10.0:
            raise SessionFaultError("interval_seconds must be in 1..10")
        if not 0.0 <= self.disconnect_blackout_seconds <= 30.0:
            raise SessionFaultError("disconnect_blackout_seconds must be in 0..30")
        if not 1 <= self.client_id <= 255:
            raise SessionFaultError("client_id must be in 1..255")
        if self.isolate_native_worker and platform.system() != "Linux":
            raise SessionFaultError("isolated session fault fixture requires Linux")
        if self.use_process_proxy and platform.system() != "Linux":
            raise SessionFaultError("process session fault proxy requires Linux")
        if self.transport_mode not in {"loopback_proxy", "transparent_redirect"}:
            raise SessionFaultError("session fault transport_mode is invalid")
        if not 0 <= self.proxy_listen_port <= 65535:
            raise SessionFaultError("session fault proxy_listen_port must be in 0..65535")
        if self.acceptance_standard not in _ACCEPTANCE_STANDARDS:
            raise SessionFaultError("session fault acceptance_standard is invalid")
        if self.transport_mode == "transparent_redirect":
            if platform.system() != "Linux" or not (
                self.use_process_proxy or self.isolate_native_worker
            ):
                raise SessionFaultError(
                    "transparent session fault redirect requires an isolated Linux process proxy"
                )
            if not 1 <= self.proxy_listen_port <= 65535:
                raise SessionFaultError(
                    "transparent session fault redirect requires proxy_listen_port"
                )
            _validate_transparent_redirect_environment(self)


def run_session_fault(settings: SessionFaultSettings) -> dict[str, Any]:
    """Run one to three real, locally interrupted EMT session cycles.

    A normal ``logout`` is never classified as a network disconnect.  This
    runner instead closes its loopback proxy pair and accepts only callbacks
    emitted by the real native SDK after that controlled transport event.
    """

    settings.validate()
    if settings.isolate_native_worker:
        return _run_session_fault_isolated(settings)
    return _run_session_fault_inprocess(settings)


def _validate_transparent_redirect_environment(settings: SessionFaultSettings) -> None:
    """Require a preloaded shim that can only redirect this fixture endpoint."""

    expected = {
        "TRADECORE_SESSION_FAULT_CONNECT_REDIRECT": "1",
        "TRADECORE_SESSION_FAULT_TARGET_HOST": settings.accounts.host,
        "TRADECORE_SESSION_FAULT_TARGET_PORT": str(settings.accounts.port),
        "TRADECORE_SESSION_FAULT_PROXY_PORT": str(settings.proxy_listen_port),
    }
    if any(os.environ.get(name) != value for name, value in expected.items()):
        raise SessionFaultError("transparent session fault redirect environment is incomplete")
    if os.environ.get("TRADECORE_SESSION_FAULT_CONNECT_REDIRECT_BYPASS") is not None:
        raise SessionFaultError(
            "transparent session fault redirect bypass must not be set in fixture"
        )


def _run_session_fault_inprocess(
    settings: SessionFaultSettings,
    *,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Execute the real SDK fixture in the current process only when requested."""

    trace = GatewayTraceCollector()
    settings.runtime_dir.mkdir(parents=True, exist_ok=True)
    event_journal = FileJournal(settings.runtime_dir / f"session-fault-{uuid4().hex}.emtj")
    callbacks = CallbackPipeline(
        journal=event_journal,
        trace=trace,
        lookup_source=lambda record: None,
        capacity=1024,
        require_native_fastspi=settings.require_native_fastspi,
    )
    proxy_settings = TcpFaultProxySettings(
        target_host=settings.accounts.host,
        target_port=settings.accounts.port,
        listen_port=settings.proxy_listen_port,
    )
    proxy = (
        ProcessTcpFaultProxy(proxy_settings)
        if settings.use_process_proxy
        else TcpFaultProxy(proxy_settings)
    )
    session: EmtSession | None = None
    evidence_ids: set[str] = set()
    cycle_outcomes: list[dict[str, bool | int | str | None]] = []
    sdk_api_version: str | None = None
    sdk_binary_sha256: dict[str, str] = {}
    try:
        proxy.start()
        _emit_progress(progress, "proxy_started")
        loaded = EmtPlatformLoader(SdkLoadSettings(sdk_root=settings.sdk_root)).load()
        _emit_progress(progress, "sdk_loaded")
        session = EmtSession.from_loaded_sdk(
            loaded,
            callback_ingress=callbacks,
            settings=SessionSettings(
                host=(
                    settings.accounts.host
                    if settings.transport_mode == "transparent_redirect"
                    else proxy.host
                ),
                port=(
                    settings.accounts.port
                    if settings.transport_mode == "transparent_redirect"
                    else proxy.port
                ),
                account=settings.accounts.ordinary.account,
                password=settings.accounts.ordinary.password,
                client_id=settings.client_id,
                sdk_log_dir=settings.runtime_dir / "sdk-logs",
                account_profile="ordinary",
                sdk_log_level=0,
            ),
        )
        _emit_progress(progress, "session_start_entered")
        session_snapshot = session.start()
        sdk_api_version = session_snapshot.api_version
        sdk_binary_sha256 = _sdk_binary_sha256(loaded.library_dir)
        _emit_progress(progress, "session_started")
        initial_connection_count = proxy.connection_count
        initial_callbacks = _drain_for(
            callbacks,
            ("OnConnected",),
            min(5.0, settings.callback_timeout_seconds),
        )
        initial_connected_ids = [
            correlation_id for name, correlation_id in initial_callbacks if name == "OnConnected"
        ]
        initial_native_on_connected = bool(initial_connected_ids)
        evidence_ids.update(initial_connected_ids)
        for cycle in range(1, settings.cycles + 1):
            if not initial_native_on_connected:
                cycle_outcomes.append(
                    {
                        "cycle": cycle,
                        "initial_native_on_connected": False,
                        "proxy_pair_ready": False,
                        "proxy_disconnect_injected": False,
                        "disconnect_blackout_seconds": settings.disconnect_blackout_seconds,
                        "native_on_disconnected": False,
                        "transport_reconnected": False,
                        "native_on_connected": False,
                        "reconnect_callback_complete": False,
                        "acceptance_complete": False,
                    }
                )
                break
            active = proxy.wait_for_connection(
                max(1, initial_connection_count), settings.callback_timeout_seconds
            )
            if active:
                _emit_progress(progress, "proxy_pair_observed")
            reconnect_count_before = proxy.connection_count
            disconnected = (
                proxy.disconnect_active(settings.disconnect_blackout_seconds) if active else False
            )
            if disconnected:
                _emit_progress(progress, "disconnect_injected")
            disconnected_callbacks = _drain_for(
                callbacks,
                ("OnDisconnected",),
                settings.callback_timeout_seconds,
            )
            disconnected_ids = [
                correlation_id
                for name, correlation_id in disconnected_callbacks
                if name == "OnDisconnected"
            ]
            early_connected_ids = [
                correlation_id
                for name, correlation_id in disconnected_callbacks
                if name == "OnConnected"
            ]
            evidence_ids.update(disconnected_ids)
            if disconnected_ids:
                _emit_progress(progress, "native_disconnected")
            reconnected_transport = (
                proxy.connection_count > reconnect_count_before
                or proxy.wait_for_connection(
                    reconnect_count_before + 1,
                    settings.callback_timeout_seconds,
                )
            )
            if reconnected_transport:
                _emit_progress(progress, "transport_reconnected")
            reconnected_callbacks = _drain_for(
                callbacks,
                ("OnConnected",),
                settings.callback_timeout_seconds if reconnected_transport else 0.2,
            )
            connected_ids = early_connected_ids + [
                correlation_id
                for name, correlation_id in reconnected_callbacks
                if name == "OnConnected"
            ]
            evidence_ids.update(connected_ids)
            if connected_ids:
                _emit_progress(progress, "native_connected")
            documented_callback_complete = bool(
                initial_native_on_connected
                and disconnected_ids
                and reconnected_transport
                and connected_ids
            )
            cycle_outcomes.append(
                {
                    "cycle": cycle,
                    "initial_native_on_connected": initial_native_on_connected,
                    "proxy_pair_ready": active,
                    "proxy_disconnect_injected": disconnected,
                    "disconnect_blackout_seconds": settings.disconnect_blackout_seconds,
                    "native_on_disconnected": bool(disconnected_ids),
                    "automatic_reconnect_expected": True,
                    "transport_reconnected": reconnected_transport,
                    "native_on_connected": bool(connected_ids),
                    "reconnect_callback_complete": documented_callback_complete,
                    "acceptance_complete": _acceptance_complete(
                        settings=settings,
                        sdk_api_version=sdk_api_version,
                        sdk_binary_sha256=sdk_binary_sha256,
                        initial_native_on_connected=initial_native_on_connected,
                        proxy_pair_ready=active,
                        proxy_disconnect_injected=disconnected,
                        native_on_disconnected=bool(disconnected_ids),
                        transport_reconnected=reconnected_transport,
                        native_on_connected=bool(connected_ids),
                    ),
                }
            )
            initial_connection_count = proxy.connection_count
            if cycle != settings.cycles:
                sleep(settings.interval_seconds)
        report = _report(
            trace=trace,
            evidence_ids=evidence_ids,
            settings=settings,
            callbacks=callbacks,
            cycle_outcomes=cycle_outcomes,
            proxy_connection_count=proxy.connection_count,
            sdk_api_version=sdk_api_version,
            sdk_binary_sha256=sdk_binary_sha256,
        )
        _emit_progress(progress, "report_built")
        return report
    except (SessionError, TcpFaultProxyError, RuntimeError) as exc:
        report = _report(
            trace=trace,
            evidence_ids=evidence_ids,
            settings=settings,
            callbacks=callbacks,
            cycle_outcomes=cycle_outcomes,
            proxy_connection_count=proxy.connection_count,
            sdk_api_version=sdk_api_version,
            sdk_binary_sha256=sdk_binary_sha256,
        )
        report["run"]["fixture_error"] = _fixture_failure_class(exc, proxy.connection_count)
        report["run"]["fixture_error_type"] = type(exc).__name__
        _emit_progress(progress, "fixture_failure")
        return report
    finally:
        try:
            if session is not None:
                if settings.retain_native_reference is not None:
                    settings.retain_native_reference(session)
                else:
                    session.close()
        finally:
            proxy.close()
            event_journal.close()


def _run_session_fault_isolated(settings: SessionFaultSettings) -> dict[str, Any]:
    """Contain an EMT native abort and preserve an explicit failure artifact.

    The child owns the SDK and loopback proxy.  It sends back only the existing
    payload-free report; the parent never receives an endpoint, account,
    request, callback payload or native object.  A nonzero exit, signal, or
    hard timeout is failure evidence, never a substitute for a disconnect
    callback or recovery proof.
    """

    read_fd, write_fd = os.pipe()
    progress_read_fd, progress_write_fd = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        try:
            os.close(read_fd)
            os.close(progress_read_fd)
            retained: list[object] = []
            report = _run_session_fault_inprocess(
                replace(
                    settings,
                    isolate_native_worker=False,
                    retain_native_reference=retained.append,
                    use_process_proxy=True,
                ),
                progress=lambda stage: _write_worker_progress(progress_write_fd, stage),
            )
            _write_child_report(write_fd, report)
        except BaseException as exc:
            # Native wrapper errors and vendor callback text must not escape
            # the isolated child.  The parent receives only the exception
            # class as a fixed diagnostic milestone, never its text.
            try:
                _write_worker_progress(progress_write_fd, f"worker_exception_{type(exc).__name__}")
            except BaseException:
                pass
            os._exit(3)
        finally:
            try:
                os.close(write_fd)
            except OSError:
                pass
            try:
                os.close(progress_write_fd)
            except OSError:
                pass
        os._exit(0)

    os.close(write_fd)
    os.close(progress_write_fd)
    status, raw_report, worker_stages = _wait_for_fault_worker_and_collect(
        child_pid, read_fd, settings, progress_fd=progress_read_fd
    )
    os.close(read_fd)
    os.close(progress_read_fd)
    if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0:
        try:
            report = json.loads(raw_report.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return _isolated_worker_failure_report(
                settings, "worker_report_invalid", worker_stages=worker_stages
            )
        if isinstance(report, dict):
            report["run"]["isolated_native_worker"] = True
            report["run"]["worker_stages"] = worker_stages
            return report
        return _isolated_worker_failure_report(
            settings, "worker_report_invalid", worker_stages=worker_stages
        )
    return _isolated_worker_failure_report(
        settings, _worker_exit_kind(status), worker_stages=worker_stages
    )


def _wait_for_fault_worker_and_collect(
    child_pid: int,
    read_fd: int,
    settings: SessionFaultSettings,
    *,
    progress_fd: int | None = None,
) -> tuple[int, bytes, list[str]]:
    """Drain the child report while enforcing the bounded fault window.

    A complete gateway report contains the capability matrix and can be much
    larger than a Linux pipe buffer.  Waiting for the child before reading the
    pipe creates a deadlock: the worker blocks in ``write`` and the parent
    later misclassifies the healthy worker as a timeout.  Read concurrently so
    the supervisor's timeout retains its intended meaning.
    """

    # One cycle can wait once for disconnect and once for reconnect.  The
    # allowance also covers local startup and report transfer, never a host
    # service timeout or an unbounded vendor finalizer.
    deadline = (
        monotonic()
        + settings.cycles * (settings.callback_timeout_seconds * 2 + settings.interval_seconds)
        + min(5.0, settings.callback_timeout_seconds)
        + 10.0
    )
    chunks: list[bytes] = []
    worker_stages: list[str] = []
    progress_buffer = b""
    status: int | None = None
    stream_open = True
    progress_open = progress_fd is not None
    while monotonic() < deadline:
        read_fds = ([read_fd] if stream_open else []) + ([progress_fd] if progress_open else [])
        if read_fds:
            readable, _unused_writable, _unused_error = select.select(read_fds, [], [], 0.02)
            if readable:
                for readable_fd in readable:
                    chunk = os.read(readable_fd, 65_536)
                    if readable_fd == read_fd:
                        if chunk:
                            chunks.append(chunk)
                        else:
                            stream_open = False
                    elif chunk:
                        progress_buffer = _collect_worker_stages(
                            progress_buffer + chunk, worker_stages
                        )
                    else:
                        progress_open = False
        if status is None:
            observed_pid, observed_status = os.waitpid(child_pid, os.WNOHANG)
            if observed_pid == child_pid:
                status = observed_status
                if not stream_open and not progress_open:
                    return status, b"".join(chunks), worker_stages
                continue
        if not stream_open and not progress_open:
            sleep(0.02)
    if status is None:
        os.kill(child_pid, signal.SIGKILL)
        _observed_pid, status = os.waitpid(child_pid, 0)
    # A killed or crashed worker may already have emitted a useful, complete
    # payload-free report.  Drain all bytes that were successfully written;
    # final classification still follows its exit status.
    while stream_open or progress_open:
        read_fds = ([read_fd] if stream_open else []) + ([progress_fd] if progress_open else [])
        readable, _unused_writable, _unused_error = select.select(read_fds, [], [], 0.1)
        if not readable:
            break
        for readable_fd in readable:
            chunk = os.read(readable_fd, 65_536)
            if readable_fd == read_fd:
                if chunk:
                    chunks.append(chunk)
                else:
                    stream_open = False
            elif chunk:
                progress_buffer = _collect_worker_stages(progress_buffer + chunk, worker_stages)
            else:
                progress_open = False
    if progress_buffer:
        _collect_worker_stages(progress_buffer + b"\n", worker_stages)
    assert status is not None
    return status, b"".join(chunks), worker_stages


def _emit_progress(progress: Callable[[str], None] | None, stage: str) -> None:
    """Emit a fixed, payload-free fixture milestone when supervision is enabled."""

    if progress is not None:
        progress(stage)


def _write_worker_progress(fd: int, stage: str) -> None:
    """Write a tiny fixed milestone without leaking endpoint or account data."""

    if not stage.isidentifier():
        raise RuntimeError("invalid isolated fault-worker progress stage")
    encoded = stage.encode("ascii")
    os.write(fd, encoded + b"\n")


def _collect_worker_stages(buffer: bytes, stages: list[str]) -> bytes:
    """Collect fixed child milestones from arbitrary pipe chunk boundaries."""

    *complete, remainder = buffer.split(b"\n")
    for encoded in complete:
        if not encoded:
            continue
        try:
            stage = encoded.decode("ascii")
        except UnicodeDecodeError:
            continue
        if stage.isidentifier():
            stages.append(stage)
    return remainder


def _write_child_report(fd: int, report: dict[str, Any]) -> None:
    payload = json.dumps(report, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        view = view[written:]


def _read_child_report(fd: int) -> bytes:
    chunks: list[bytes] = []
    while True:
        chunk = os.read(fd, 65_536)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)


def _worker_exit_kind(status: int) -> str:
    if os.WIFSIGNALED(status):
        signal_number = os.WTERMSIG(status)
        return (
            "worker_timeout_terminated"
            if signal_number == signal.SIGKILL
            else "native_worker_signaled"
        )
    return "native_worker_nonzero_exit"


def _isolated_worker_failure_report(
    settings: SessionFaultSettings,
    failure_kind: str,
    *,
    worker_stages: list[str] | None = None,
) -> dict[str, Any]:
    """Return only bounded supervisor evidence after a child failure."""

    report = build_gateway_report([])
    report["driver"] = "gateway-session-fault"
    report["measurement_scope"] = (
        "real EMT SDK session inside an isolated child and local loopback TCP proxy; "
        "native-worker failure is explicit and is not a successful fault/recovery result"
    )
    report["safety"] = {
        "order_entry": False,
        "cancellation": False,
        "query": False,
        "fund_or_credit_mutation": False,
        "system_firewall_or_route_change": False,
        "native_fastspi_required": settings.require_native_fastspi,
    }
    report["run"] = {
        "fixture_id": "gateway-loopback-session-fault-v1",
        "fixture_owner": settings.fixture_owner,
        "cycles": settings.cycles,
        "transport_mode": settings.transport_mode,
        "callback_timeout_seconds": settings.callback_timeout_seconds,
        "interval_seconds": settings.interval_seconds,
        "disconnect_blackout_seconds": settings.disconnect_blackout_seconds,
        "initial_native_on_connected": False,
        "native_fastspi_available": None,
        "proxy_connection_count": 0,
        "cycle_outcomes": [],
        "trace_artifact": None,
        "isolated_native_worker": True,
        "worker_stages": list(worker_stages or ()),
        "fixture_error": failure_kind,
        "acceptance_standard": settings.acceptance_standard,
        "sdk_api_version": None,
        "sdk_binary_sha256": {},
        "acceptance_complete": False,
    }
    return report


def _drain_for(
    callbacks: CallbackPipeline,
    expected_names: tuple[str, ...],
    timeout_seconds: float,
) -> list[tuple[str, str]]:
    """ACK all pending callbacks and return only their names/correlation IDs."""

    deadline = monotonic() + timeout_seconds
    received: list[tuple[str, str]] = []
    expected = set(expected_names)
    while True:
        delivery = callbacks.normalize_one()
        if delivery is not None:
            callbacks.ack(delivery.delivery_id)
            received.append((delivery.callback_name, delivery.callback_correlation_id))
            if expected and delivery.callback_name in expected:
                return received
            continue
        if monotonic() >= deadline:
            return received
        sleep(0.005)


def _report(
    *,
    trace: GatewayTraceCollector,
    evidence_ids: set[str],
    settings: SessionFaultSettings,
    callbacks: CallbackPipeline,
    cycle_outcomes: list[dict[str, bool | int | str | None]],
    proxy_connection_count: int,
    sdk_api_version: str | None,
    sdk_binary_sha256: dict[str, str],
) -> dict[str, Any]:
    events = [
        event
        for event in trace.export_events()
        if event.get("event_type") == "metric" or event.get("correlation_id") in evidence_ids
    ]
    report = build_gateway_report(events)
    if settings.trace_output is not None:
        _write_payload_free_trace(settings.trace_output, events)
        trace_artifact: str | None = settings.trace_output.name
    else:
        trace_artifact = None
    report["driver"] = "gateway-session-fault"
    report["measurement_scope"] = (
        "real EMT SDK session and real native callback path through a local loopback TCP proxy; "
        "the transport close is locally injected, so this is not a counter-side network latency claim"
    )
    report["safety"] = {
        "order_entry": False,
        "cancellation": False,
        "query": False,
        "fund_or_credit_mutation": False,
        "system_firewall_or_route_change": False,
        "native_fastspi_required": settings.require_native_fastspi,
    }
    report["run"] = {
        "fixture_id": "gateway-loopback-session-fault-v1",
        "fixture_owner": settings.fixture_owner,
        "cycles": settings.cycles,
        "transport_mode": settings.transport_mode,
        "callback_timeout_seconds": settings.callback_timeout_seconds,
        "interval_seconds": settings.interval_seconds,
        "disconnect_blackout_seconds": settings.disconnect_blackout_seconds,
        "initial_native_on_connected": any(
            bool(outcome.get("initial_native_on_connected")) for outcome in cycle_outcomes
        ),
        "native_fastspi_available": callbacks.native_fastspi_available,
        "proxy_connection_count": proxy_connection_count,
        "cycle_outcomes": cycle_outcomes,
        "trace_artifact": trace_artifact,
        "acceptance_standard": settings.acceptance_standard,
        "sdk_api_version": sdk_api_version,
        "sdk_binary_sha256": dict(sdk_binary_sha256),
        "acceptance_complete": bool(cycle_outcomes)
        and len(cycle_outcomes) == settings.cycles
        and all(bool(outcome.get("acceptance_complete")) for outcome in cycle_outcomes),
        "post_reconnect_route_state": "RECOVERED_DISARMED",
        "automatic_relogin": False,
    }
    return report


def _sdk_binary_sha256(library_dir: Path) -> dict[str, str]:
    """Return the two Linux binding identities needed by the versioned standard."""

    return {
        name: sha256((library_dir / name).read_bytes()).hexdigest()
        for name in _EMT_2_27_LINUX_HASHES
        if (library_dir / name).is_file()
    }


def _acceptance_complete(
    *,
    settings: SessionFaultSettings,
    sdk_api_version: str | None,
    sdk_binary_sha256: dict[str, str],
    initial_native_on_connected: bool,
    proxy_pair_ready: bool,
    proxy_disconnect_injected: bool,
    native_on_disconnected: bool,
    transport_reconnected: bool,
    native_on_connected: bool,
) -> bool:
    """Evaluate only the selected, explicitly versioned session-fault contract."""

    base_complete = (
        initial_native_on_connected
        and proxy_pair_ready
        and proxy_disconnect_injected
        and transport_reconnected
        and native_on_connected
    )
    if settings.acceptance_standard == _STRICT_CALLBACK_SEQUENCE:
        return base_complete and native_on_disconnected
    return (
        base_complete
        and settings.transport_mode == "transparent_redirect"
        # The forked worker executes this function with
        # ``isolate_native_worker=False`` after the parent has already
        # established isolation.  It retains the process-proxy marker.
        and (settings.isolate_native_worker or settings.use_process_proxy)
        and sdk_api_version == _EMT_2_27_API_VERSION
        and sdk_binary_sha256 == _EMT_2_27_LINUX_HASHES
    )


def _fixture_failure_class(exc: BaseException, proxy_connection_count: int) -> str:
    """Return a payload-free diagnosis suitable for fixture planning."""

    if isinstance(exc, SessionError) and proxy_connection_count == 0:
        return "session_start_failed_before_loopback_connection"
    if isinstance(exc, SessionError):
        return "session_start_failed_after_loopback_connection"
    if isinstance(exc, TcpFaultProxyError):
        return "loopback_proxy_failure"
    return "runtime_fixture_failure"


def _write_payload_free_trace(path: Path, events: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = "\n".join(json.dumps(event, ensure_ascii=True, sort_keys=True) for event in events)
    path.write_text(f"{serialized}\n" if serialized else "", encoding="utf-8")
