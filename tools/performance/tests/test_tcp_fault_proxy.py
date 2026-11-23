from __future__ import annotations

import socket
import sys
import unittest
from threading import Event, Thread

from perfbench.tcp_fault_proxy import ProcessTcpFaultProxy, TcpFaultProxySettings


@unittest.skipUnless(sys.platform == "linux", "process fault proxy requires Linux")
class ProcessTcpFaultProxyTests(unittest.TestCase):
    def setUp(self) -> None:
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self._listener.settimeout(0.1)
        self._stop = Event()
        self._accepted = Event()
        self._thread = Thread(target=self._serve, daemon=True)
        self._thread.start()

    def tearDown(self) -> None:
        self._stop.set()
        self._listener.close()
        self._thread.join(timeout=1.0)

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                connection, _address = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self._accepted.set()
            with connection:
                self._stop.wait(1.0)
            return

    def test_process_proxy_accepts_and_disconnects_only_its_own_pair(self) -> None:
        target_port = int(self._listener.getsockname()[1])
        proxy = ProcessTcpFaultProxy(
            TcpFaultProxySettings(target_host="127.0.0.1", target_port=target_port)
        )
        proxy.start()
        client = socket.create_connection((proxy.host, proxy.port), timeout=1.0)
        try:
            self.assertTrue(proxy.wait_for_connection(1, 1.0))
            self.assertTrue(self._accepted.wait(1.0))
            self.assertEqual(1, proxy.connection_count)
            self.assertTrue(proxy.disconnect_active())
        finally:
            client.close()
            proxy.close()


if __name__ == "__main__":
    unittest.main()
