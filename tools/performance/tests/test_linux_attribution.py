from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from perfbench.linux_attribution import LinuxAttributionError, collect_preflight


class LinuxAttributionTests(unittest.TestCase):
    def test_non_linux_host_is_rejected_before_any_host_collection(self) -> None:
        with patch("perfbench.linux_attribution.platform.system", return_value="Windows"):
            with self.assertRaisesRegex(LinuxAttributionError, "requires Linux"):
                collect_preflight(profile_name="c0-c7-linux-v1", journal_path=Path("."))

    def test_unknown_profile_is_rejected(self) -> None:
        with self.assertRaisesRegex(LinuxAttributionError, "unknown attribution profile"):
            collect_preflight(profile_name="unreviewed", journal_path=Path("."))
