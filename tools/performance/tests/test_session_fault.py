from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

from emt_gateway.session import SessionError
from perfbench.session_fault import (
    SessionFaultError,
    SessionFaultSettings,
    _acceptance_complete,
    _fixture_failure_class,
    _isolated_worker_failure_report,
    _wait_for_fault_worker_and_collect,
    _write_child_report,
)
from perfbench.simulated_accounts import SimulatedAccount, SimulatedAccounts


def _accounts() -> SimulatedAccounts:
    credentials = SimulatedAccount(account="simulated", password="local-only")
    return SimulatedAccounts(
        host="local-counter",
        port=19088,
        ordinary=credentials,
        credit=credentials,
        option=credentials,
    )


class SessionFaultSettingsTests(unittest.TestCase):
    def test_real_fixture_requires_owner_and_bounded_controls(self) -> None:
        base = {
            "sdk_root": Path("sdk"),
            "runtime_dir": Path("runtime"),
            "accounts": _accounts(),
            "fixture_owner": "fengyu",
        }
        SessionFaultSettings(**base).validate()
        for extra, message in (
            ({"fixture_owner": ""}, "fixture_owner"),
            ({"cycles": 4}, "cycles"),
            ({"callback_timeout_seconds": 2.0}, "callback_timeout_seconds"),
            ({"interval_seconds": 0.5}, "interval_seconds"),
            ({"disconnect_blackout_seconds": 31.0}, "disconnect_blackout_seconds"),
        ):
            with self.subTest(extra=extra):
                with self.assertRaisesRegex(SessionFaultError, message):
                    SessionFaultSettings(**(base | extra)).validate()

    def test_preflight_failure_is_classified_without_endpoint_or_account_data(self) -> None:
        self.assertEqual(
            "session_start_failed_before_loopback_connection",
            _fixture_failure_class(SessionError("safe"), 0),
        )
        self.assertEqual(
            "session_start_failed_after_loopback_connection",
            _fixture_failure_class(SessionError("safe"), 1),
        )

    def test_isolated_native_worker_failure_is_explicit_and_side_effect_free(self) -> None:
        report = _isolated_worker_failure_report(
            SessionFaultSettings(
                sdk_root=Path("sdk"),
                runtime_dir=Path("runtime"),
                accounts=_accounts(),
                fixture_owner="fengyu",
                isolate_native_worker=True,
            ),
            "native_worker_signaled",
        )
        self.assertEqual("native_worker_signaled", report["run"]["fixture_error"])
        self.assertTrue(report["run"]["isolated_native_worker"])
        self.assertFalse(report["safety"]["order_entry"])
        self.assertNotIn("account", report["run"])

    def test_current_version_accepts_the_observed_transparent_rst_sequence(self) -> None:
        settings = SessionFaultSettings(
            sdk_root=Path("sdk"),
            runtime_dir=Path("runtime"),
            accounts=_accounts(),
            fixture_owner="fengyu",
            isolate_native_worker=True,
            transport_mode="transparent_redirect",
            proxy_listen_port=29494,
        )
        self.assertTrue(
            _acceptance_complete(
                settings=settings,
                sdk_api_version="2.27.0.0",
                sdk_binary_sha256={
                    "libemt_api.so": "6eb8c354cea94bbde14074b891fe2c5979282fb72ceccf89bfe3f386126dd4c9",
                    "vnemttrader.so": "d15c6343c45d844992212f987b2d63fd51fb27037991e89086cff367eebce9ee",
                },
                initial_native_on_connected=True,
                proxy_pair_ready=True,
                proxy_disconnect_injected=True,
                native_on_disconnected=False,
                transport_reconnected=True,
                native_on_connected=True,
            )
        )

    def test_current_version_rejects_identity_drift_and_missing_reconnect(self) -> None:
        settings = SessionFaultSettings(
            sdk_root=Path("sdk"),
            runtime_dir=Path("runtime"),
            accounts=_accounts(),
            fixture_owner="fengyu",
            isolate_native_worker=True,
            transport_mode="transparent_redirect",
            proxy_listen_port=29494,
        )
        common = {
            "settings": settings,
            "sdk_api_version": "2.27.0.0",
            "sdk_binary_sha256": {},
            "initial_native_on_connected": True,
            "proxy_pair_ready": True,
            "proxy_disconnect_injected": True,
            "native_on_disconnected": False,
            "transport_reconnected": True,
            "native_on_connected": True,
        }
        self.assertFalse(_acceptance_complete(**common))
        self.assertFalse(_acceptance_complete(**(common | {"transport_reconnected": False})))

    @unittest.skipUnless(sys.platform == "linux", "isolated supervisor uses Linux fork")
    def test_supervisor_drains_a_report_larger_than_the_pipe_buffer(self) -> None:
        read_fd, write_fd = os.pipe()
        child_pid = os.fork()
        if child_pid == 0:
            try:
                os.close(read_fd)
                _write_child_report(write_fd, {"payload": "x" * 262_144})
            finally:
                os.close(write_fd)
            os._exit(0)

        os.close(write_fd)
        try:
            status, payload, worker_stages = _wait_for_fault_worker_and_collect(
                child_pid,
                read_fd,
                SessionFaultSettings(
                    sdk_root=Path("sdk"),
                    runtime_dir=Path("runtime"),
                    accounts=_accounts(),
                    fixture_owner="fengyu",
                    isolate_native_worker=True,
                ),
            )
        finally:
            os.close(read_fd)
        self.assertTrue(os.WIFEXITED(status))
        self.assertEqual(0, os.WEXITSTATUS(status))
        self.assertGreater(len(payload), 262_144)
        self.assertEqual([], worker_stages)
