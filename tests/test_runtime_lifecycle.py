from __future__ import annotations

import unittest

from xuanling_tradecore.runtime import (
    HostHealthImpactV1,
    HostHealthObservationV1,
    HostHealthWorkerV1,
    RuntimeLifecycleStateV1,
    RuntimeShutdownCoordinatorV1,
)


class RuntimeLifecycleTests(unittest.TestCase):
    def test_stop_closes_admission_drains_then_releases_fences(self) -> None:
        calls: list[tuple[str, object]] = []
        coordinator = RuntimeShutdownCoordinatorV1(
            stop_new_admissions=lambda reason: calls.append(("admission", reason)),
            drain_workers=(lambda deadline: calls.append(("drain", deadline)),),
            release_ownership_fences=(lambda: calls.append(("release", None)),),
        )
        coordinator.start_disarmed()
        coordinator.request_stop(reason="sigterm", deadline_monotonic_ns=99)
        self.assertEqual([("admission", "sigterm"), ("drain", 99), ("release", None)], calls)
        self.assertIs(coordinator.state, RuntimeLifecycleStateV1.STOPPED_DISARMED)

    def test_health_failures_protect_but_a_healthy_probe_never_unlocks(self) -> None:
        calls: list[tuple[str, str, str]] = []
        worker = HostHealthWorkerV1(
            read_observations=lambda: (
                HostHealthObservationV1(
                    "journal", False, HostHealthImpactV1.INTEGRITY, "account-a", "probe-1"
                ),
                HostHealthObservationV1(
                    "archive", False, HostHealthImpactV1.CAPACITY, "runtime", "probe-2"
                ),
                HostHealthObservationV1(
                    "clock", True, HostHealthImpactV1.INTEGRITY, "runtime", "probe-3"
                ),
            ),
            disarm_integrity=lambda scope, source: calls.append(("disarm", scope, source)),
            stop_new_work=lambda scope, source: calls.append(("stop", scope, source)),
            clock_ns=lambda: 88,
        )
        report = worker.probe_once()
        self.assertFalse(report.healthy)
        self.assertEqual(
            [("disarm", "account-a", "probe-1"), ("stop", "runtime", "probe-2")], calls
        )

    def test_health_reader_exception_disarms_the_runtime_before_propagating(self) -> None:
        calls: list[tuple[str, str]] = []

        def unavailable() -> tuple[HostHealthObservationV1, ...]:
            raise OSError("probe transport failed")

        worker = HostHealthWorkerV1(
            read_observations=unavailable,
            disarm_integrity=lambda scope, source: calls.append((scope, source)),
            stop_new_work=lambda _scope, _source: self.fail("capacity action is not expected"),
        )
        with self.assertRaises(OSError):
            worker.probe_once()
        self.assertEqual([("runtime", "host-health-reader:OSError")], calls)

    def test_health_matrix_applies_every_failed_observation_without_auto_recovery(self) -> None:
        calls: list[tuple[str, str, str]] = []
        observations = (
            HostHealthObservationV1("control-root", False, HostHealthImpactV1.INTEGRITY, "a", "p1"),
            HostHealthObservationV1(
                "sdk-abi", False, HostHealthImpactV1.INTEGRITY, "runtime", "p2"
            ),
            HostHealthObservationV1(
                "journal-capacity", False, HostHealthImpactV1.CAPACITY, "a", "p3"
            ),
            HostHealthObservationV1(
                "archive-lag", False, HostHealthImpactV1.CAPACITY, "runtime", "p4"
            ),
            HostHealthObservationV1(
                "healthy-recheck", True, HostHealthImpactV1.INTEGRITY, "a", "p5"
            ),
        )
        worker = HostHealthWorkerV1(
            read_observations=lambda: observations,
            disarm_integrity=lambda scope, source: calls.append(("disarm", scope, source)),
            stop_new_work=lambda scope, source: calls.append(("stop", scope, source)),
            clock_ns=lambda: 99,
        )
        report = worker.probe_once()
        self.assertFalse(report.healthy)
        self.assertEqual(
            [
                ("disarm", "a", "p1"),
                ("disarm", "runtime", "p2"),
                ("stop", "a", "p3"),
                ("stop", "runtime", "p4"),
            ],
            calls,
        )


if __name__ == "__main__":
    unittest.main()
