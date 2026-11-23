from __future__ import annotations

import unittest

from perfbench.vendor_sweep import _prepare_source


class VendorSweepTests(unittest.TestCase):
    def test_prepares_vendor_source_without_embedded_credentials(self) -> None:
        source = """from time import sleep\nif __name__ == '__main__':\n    ip = 'host'\n    port = 19088\n    user = 'user'\n    password = 'password'\n    local_ip = '127.0.0.1'\n    api.createTraderApi(3, os.getcwd(), 4)\n    session = api.login(ip, port, user, password, 1, local_ip)\n    reqid += 1\n    retQueryAsset = api.queryAsset(session, reqid)\n    api.QueryEndpointId({})\n"""
        prepared = _prepare_source(source)
        self.assertIn("_profile['password']", prepared)
        self.assertIn("_instrument_vendor_api(TestApi)", prepared)
        self.assertIn("api.createTraderApi(_client_id, _runtime_dir, 0)", prepared)
        self.assertIn("api.queryEndpointId({})", prepared)
        self.assertIn("api.setHeartBeatInterval(30)", prepared)
