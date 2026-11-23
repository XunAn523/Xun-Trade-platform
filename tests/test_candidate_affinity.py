from pathlib import Path
from unittest.mock import patch

import pytest
from xuanling_tradecore.consumer.candidate_affinity import (
    preserve_candidate_affinity,
    set_thread_affinity_exact,
    temporary_thread_affinity,
    validate_role_cpus,
)


def test_no_role_profile_does_not_touch_process_affinity():
    with patch(
        "xuanling_tradecore.consumer.candidate_affinity.os.sched_getaffinity",
        create=True,
        side_effect=AssertionError,
    ):
        validate_role_cpus(None)


def test_background_roles_cannot_overlap_any_existing_role_or_smt():
    def topology(path, **_):
        return (
            "0"
            if path.name == "physical_package_id"
            else str(int(path.parent.parent.name[3:]) % 96)
        )

    with (
        patch(
            "xuanling_tradecore.consumer.candidate_affinity.os.sched_getaffinity",
            create=True,
            return_value={40, 41, 42, 43, 44, 45, 46, 47, 136},
        ),
        patch.object(Path, "read_text", new=topology),
    ):
        validate_role_cpus((40, 41, 42, 43), (44, 45), (46, 47))
        for background in ((), (40,), (44,), (136,), (46, 46), (46, 48), (True,), (46, 47, 41)):
            with pytest.raises(ValueError):
                validate_role_cpus((40, 41, 42, 43), (44, 45), background)
        with pytest.raises(ValueError):
            validate_role_cpus(None, None, (46,))


def test_fixed_thread_context_restores_affinity_after_exception_or_reduced_grant():
    current = [{1, 2, 3}]

    def set_mask(_pid, cpus):
        current[0] = set(cpus) & {1, 2, 3}

    with (
        patch(
            "xuanling_tradecore.consumer.candidate_affinity.os.sched_getaffinity",
            create=True,
            side_effect=lambda _: set(current[0]),
        ),
        patch(
            "xuanling_tradecore.consumer.candidate_affinity.os.sched_setaffinity",
            create=True,
            side_effect=set_mask,
        ),
    ):
        with pytest.raises(ValueError):
            with temporary_thread_affinity((2, 3)):
                assert current[0] == {2, 3}
                raise ValueError("fixture")
        assert current[0] == {1, 2, 3}
        with pytest.raises(RuntimeError):
            with temporary_thread_affinity((2, 9)):
                pytest.fail("reduced grant was accepted")
        assert current[0] == {1, 2, 3}


def test_constructor_failure_and_background_success_restore_caller():
    current = [{1, 2, 3}]

    def set_mask(_pid, cpus):
        current[0] = set(cpus)

    @preserve_candidate_affinity
    def construct(*, role_cpus, background_cpus=None, fail=False):
        set_thread_affinity_exact(set(background_cpus or role_cpus[:1]))
        if fail:
            raise ValueError("fixture failure")
        return "done"

    with (
        patch("xuanling_tradecore.consumer.candidate_affinity.sys.platform", "linux"),
        patch(
            "xuanling_tradecore.consumer.candidate_affinity.os.sched_getaffinity",
            create=True,
            side_effect=lambda _: set(current[0]),
        ),
        patch(
            "xuanling_tradecore.consumer.candidate_affinity.os.sched_setaffinity",
            create=True,
            side_effect=set_mask,
        ),
    ):
        with pytest.raises(ValueError):
            construct(role_cpus=(1, 2, 3), fail=True)
        assert current[0] == {1, 2, 3}
        assert construct(role_cpus=(1,), background_cpus=(2, 3)) == "done"
        assert current[0] == {1, 2, 3}


def test_fixed_roles_must_fit_existing_grant_and_not_share_smt():
    def topology(path, **_kwargs):
        return (
            "0"
            if path.name == "physical_package_id"
            else str(int(path.parent.parent.name[3:]) % 96)
        )

    with (
        patch(
            "xuanling_tradecore.consumer.candidate_affinity.os.sched_getaffinity",
            create=True,
            return_value={40, 41, 42, 43, 136},
        ),
        patch.object(Path, "read_text", new=topology),
    ):
        validate_role_cpus((40, 41, 42))
        validate_role_cpus((40, 41, 42, 43))
        for invalid in (
            (40, 41),
            (40, 40, 42),
            (40, 136, 42),
            (40, 41, 42, 136),
            (40, 41, 42, 44),
            (40, 41, 99),
            (-1, 41, 42),
            (True, 41, 42),
        ):
            with pytest.raises(ValueError):
                validate_role_cpus(invalid)


def test_read_owners_must_fit_same_existing_grant_and_distinct_physical_cores():
    def topology(path, **_kwargs):
        return (
            "0"
            if path.name == "physical_package_id"
            else str(int(path.parent.parent.name[3:]) % 96)
        )

    with (
        patch(
            "xuanling_tradecore.consumer.candidate_affinity.os.sched_getaffinity",
            create=True,
            return_value={40, 41, 42, 43, 44, 45, 136},
        ),
        patch.object(Path, "read_text", new=topology),
    ):
        validate_role_cpus((40, 41, 42, 43), (44, 45))
        for roles, reads in (
            (None, (44,)),
            ((40, 41, 42), ()),
            ((40, 41, 42), (40,)),
            ((40, 41, 42), (136,)),
            ((40, 41, 42), (44, 44)),
            ((40, 41, 42), (44, 46)),
            ((40, 41, 42), (True,)),
            ((40, 41, 42), (43, 44, 45)),
        ):
            with pytest.raises(ValueError):
                validate_role_cpus(roles, reads)
