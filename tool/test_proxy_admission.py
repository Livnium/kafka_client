#!/usr/bin/env python3
"""No-broker boundary regression for two-worker proxy admission."""
import unittest
from unittest.mock import Mock, patch

import run_network_fault_integration as plain
import run_tls_network_fault_integration as tls


class ProxyAdmissionTest(unittest.TestCase):
    def test_31_existing_workers_cannot_admit_two_more(self):
        for module, proxy_type in [(plain, plain.FaultProxy), (tls, tls.OpaqueFaultProxy)]:
            with self.subTest(proxy=proxy_type.__name__):
                proxy = proxy_type(0, 1)
                real_listener = proxy.listener
                client = Mock()
                client.close.side_effect = proxy.stopping.set
                proxy.listener = Mock()
                proxy.listener.accept.return_value = (client, ('127.0.0.1', 1))
                proxy.workers.update(object() for _ in range(31))
                try:
                    with patch.object(module.socket, 'create_connection') as connect:
                        proxy.accept()
                        connect.assert_not_called()
                    client.close.assert_called_once()
                    self.assertEqual(proxy.stats['rejected'], 1)
                    self.assertEqual(len(proxy.workers), 31)
                    self.assertEqual(len(proxy.connections), 0)
                finally:
                    real_listener.close()
                    proxy.workers.clear()


if __name__ == '__main__':
    unittest.main()
