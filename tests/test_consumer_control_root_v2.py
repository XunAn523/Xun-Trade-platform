"""Protected atomic consumer cursor control-root tests."""

from __future__ import annotations

import os
import stat
import tempfile
import unittest
from pathlib import Path

from xuanling_tradecore.consumer import CursorKeyRingV2, CursorSigningKeyV2
from xuanling_tradecore.consumer.control_root_v2 import (
    AccountCursorControlRootV2,
    ConsumerControlRootError,
)
from xuanling_tradecore.consumer.stream_registry_v2 import (
    CursorStreamRefV2,
    CursorStreamRegistryV2,
    CursorStreamResetV2,
)


class ConsumerControlRootV2Tests(unittest.TestCase):
    def _ring(self) -> CursorKeyRingV2:
        return CursorKeyRingV2(
            current=CursorSigningKeyV2("current", b"c" * 32),
            retired=(CursorSigningKeyV2("old", b"o" * 32, verify_until_wall_ns=200),),
        )

    def test_round_trip_uses_owner_only_atomic_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = AccountCursorControlRootV2(Path(directory).resolve() / "account")
            root.write_cursor_key_ring(self._ring())
            loaded = root.read_cursor_key_ring()
            token = loaded.issue(
                consumer_id="c",
                consumer_account_id="ca",
                journal_id="j",
                stream_generation=1,
                after_position=None,
                authorization_epoch=1,
                issued_at_wall_ns=1,
                expires_at_wall_ns=100,
            )
            self.assertEqual("c", loaded.verify(token, evaluated_wall_ns=2).consumer_id)
            if os.name != "nt":
                self.assertEqual(
                    0,
                    stat.S_IMODE((root.path / "cursor-key-ring-v2.json").stat().st_mode) & 0o077,
                )

    def test_rejects_broad_permissions_and_symlink_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root_path = Path(directory).resolve() / "account"
            root_path.mkdir(mode=0o700)
            if os.name == "nt":
                self.skipTest("Windows lacks POSIX modes and ordinary symlink creation")
            os.chmod(root_path, 0o755)
            with self.assertRaisesRegex(ConsumerControlRootError, "permissions"):
                AccountCursorControlRootV2(root_path).initialize()
            os.chmod(root_path, 0o700)
            root = AccountCursorControlRootV2(root_path)
            external = Path(directory).resolve() / "external"
            external.write_text("{}", encoding="ascii")
            (root_path / "cursor-key-ring-v2.json").symlink_to(external)
            with self.assertRaisesRegex(ConsumerControlRootError, "regular file"):
                root.read_cursor_key_ring()

    def test_round_trips_registry_and_rejects_tampered_graph(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = AccountCursorControlRootV2(Path(directory).resolve() / "account")
            first, current = CursorStreamRefV2("j1", 1), CursorStreamRefV2("j2", 2)
            registry = CursorStreamRegistryV2(
                current=current,
                streams=(first, current),
                resets=(CursorStreamResetV2("reset-1", first, current),),
            )
            root.write_stream_registry(registry)
            self.assertEqual(registry, root.read_stream_registry())
            path = root.path / "cursor-stream-registry-v2.json"
            path.write_text(
                '{"schema_version":1,"current":{"journal_id":"j1","stream_generation":1},"streams":[],"resets":[]}',
                encoding="ascii",
            )
            if os.name != "nt":
                os.chmod(path, 0o600)
            with self.assertRaisesRegex(ConsumerControlRootError, "invalid or unavailable"):
                root.read_stream_registry()


if __name__ == "__main__":
    unittest.main()
