"""Consumer stream-registry reset graph tests."""

from __future__ import annotations

import unittest

from xuanling_tradecore.consumer.stream_registry_v2 import (
    CursorStreamRefV2,
    CursorStreamRegistryError,
    CursorStreamRegistryV2,
    CursorStreamResetV2,
)


class ConsumerStreamRegistryV2Tests(unittest.TestCase):
    def test_resolves_a_single_registered_reset_chain(self) -> None:
        first = CursorStreamRefV2("journal-a", 1)
        second = CursorStreamRefV2("journal-b", 2)
        current = CursorStreamRefV2("journal-c", 3)
        registry = CursorStreamRegistryV2(
            current=current,
            streams=(first, second, current),
            resets=(
                CursorStreamResetV2("reset-a", first, second),
                CursorStreamResetV2("reset-b", second, current),
            ),
        )
        self.assertEqual(("reset-a", "reset-b"), registry.resolve_reset_context(source=first))
        self.assertEqual(second, registry.require_context(source=first, reset_context="reset-a"))

    def test_rejects_branch_cycle_and_unknown_context(self) -> None:
        first = CursorStreamRefV2("journal-a", 1)
        second = CursorStreamRefV2("journal-b", 2)
        third = CursorStreamRefV2("journal-c", 3)
        with self.assertRaisesRegex(CursorStreamRegistryError, "branches"):
            CursorStreamRegistryV2(
                current=third,
                streams=(first, second, third),
                resets=(
                    CursorStreamResetV2("a", first, second),
                    CursorStreamResetV2("b", first, third),
                ),
            )
        with self.assertRaisesRegex(CursorStreamRegistryError, "root|cyclic"):
            CursorStreamRegistryV2(
                current=second,
                streams=(first, second),
                resets=(
                    CursorStreamResetV2("a", first, second),
                    CursorStreamResetV2("b", second, first),
                ),
            )
        registry = CursorStreamRegistryV2(
            current=second,
            streams=(first, second),
            resets=(CursorStreamResetV2("a", first, second),),
        )
        with self.assertRaisesRegex(CursorStreamRegistryError, "does not match"):
            registry.require_context(source=first, reset_context="other")


if __name__ == "__main__":
    unittest.main()
