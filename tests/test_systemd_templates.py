from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class SystemdTemplateTests(unittest.TestCase):
    def test_runtime_template_has_private_runtime_and_safe_stop_policy(self) -> None:
        value = (ROOT / "deploy" / "systemd" / "xuanling-tradecore.service").read_text("utf-8")
        for directive in (
            "User=xuanling-tradecore",
            "Type=notify",
            "NotifyAccess=main",
            "WatchdogSec=10s",
            "RuntimeDirectory=xuanling-tradecore",
            "RuntimeDirectoryPreserve=restart",
            "Restart=on-failure",
            "KillSignal=SIGTERM",
            "TimeoutStopSec=45",
            "LimitCORE=0",
            "ProtectSystem=strict",
            "--fault-gap-page /run/xuanling-tradecore/fault-gap-page.v1",
        ):
            self.assertIn(directive, value)

    def test_fault_supervisor_template_uses_explicit_page_and_evidence_paths(self) -> None:
        value = (
            ROOT / "deploy" / "systemd" / "xuanling-tradecore-fault-supervisor.service"
        ).read_text("utf-8")
        self.assertIn(
            "fault-supervisor --gap-page /run/xuanling-tradecore/fault-gap-page.v1", value
        )
        self.assertIn("--evidence-root /var/lib/xuanling-tradecore/control/fault-evidence", value)
        self.assertIn("RuntimeDirectory=xuanling-tradecore-supervisor", value)

    def test_consumer_uds_unit_waits_for_the_protected_socket(self) -> None:
        value = (
            ROOT / "deploy" / "systemd" / "xuanling-tradecore-consumer-uds.service"
        ).read_text("utf-8")
        self.assertIn(
            "ExecStartPost=/bin/sh /opt/xuanling-tradecore/deploy/consumer-uds/wait-ready.sh",
            value,
        )
        for directive in (
            "AllowedCPUs=64-79,160-175",
            "CPUWeight=10000",
            "Nice=-5",
        ):
            self.assertIn(directive, value)
        probe = (ROOT / "deploy" / "consumer-uds" / "wait-ready.sh").read_text("utf-8")
        self.assertIn("[ -S \"$socket_path\" ]", probe)
        self.assertIn("xuanling:xuanling:600", probe)
        self.assertIn('attempt" -lt 400', probe)

    def test_performance_candidate_reclaims_before_the_hard_memory_limit(self) -> None:
        value = (
            ROOT / "deploy" / "systemd" / "tradecore-performance-candidate"
        ).read_text("utf-8")
        self.assertIn("--property=MemoryHigh=4831838208", value)
        self.assertIn("--property=MemoryMax=6442450944", value)
        self.assertLess(
            value.index("--property=MemoryHigh=4831838208"),
            value.index("--property=MemoryMax=6442450944"),
        )
        self.assertIn("create-yield1|create-nice|stop|status", value)
        self.assertIn("background_args=(--background-cpus 171,172)", value)
        self.assertIn("create-baseline) create baseline", value)
        self.assertIn("create-experimental) create experimental", value)
        self.assertIn("create-isolated) create isolated", value)
        self.assertIn("create-gil1) create gil1", value)
        self.assertIn("create-mock32) create mock32", value)
        self.assertIn("TRADECORE_DIAGNOSTIC_MOCK_BATCH=32", value)
        self.assertIn("gil_args=(--gil-switch-us 1000)", value)
        self.assertIn("frontend_cpus=70,71,72,73,74,75,76,78", value)
        self.assertIn("background_args=(--background-cpus 75,76)", value)
        self.assertIn("frontend_cpus=70,71,72,73,74,78,167,168", value)
        self.assertIn('--frontend-cpus "$frontend_cpus"', value)
        self.assertIn("create-diagnostic) create diagnostic", value)
        self.assertIn("create-cpu-profile) create cpu-profile", value)
        self.assertIn("TRADECORE_DIAGNOSTIC_OWNER_PROFILE=1", value)
        self.assertIn("create-yield) create yield", value)
        self.assertIn("create-yield1) create yield1", value)
        self.assertIn("create-nice) create nice", value)
        self.assertIn("if [ \"${1:-}\" = 'experimental' ]; then", value)

    def test_isolated_candidate_keeps_all_direct_read_frontend_slots(self) -> None:
        value = (ROOT / "deploy" / "systemd" / "tradecore-performance-candidate").read_text(
            "utf-8"
        )
        block = value.split("if [ \"${1:-}\" = 'isolated' ]; then", 1)[1].split("\n  fi", 1)[0]
        match = re.search(r"frontend_cpus=([0-9,]+)", block)
        self.assertIsNotNone(match)
        assert match is not None
        frontends = tuple(int(cpu) for cpu in match.group(1).split(","))
        self.assertEqual(len(frontends), 8)
        self.assertEqual(len(set(frontends)), 8)
        self.assertTrue(set(frontends).isdisjoint({75, 76, 171, 172}))


if __name__ == "__main__":
    unittest.main()
