from __future__ import annotations

import time
import unittest
from unittest import mock

from spreadsheet_tool.background_worker import BackgroundWorker


class BackgroundWorkerTests(unittest.TestCase):
    def test_background_worker_returns_success_result(self) -> None:
        worker = BackgroundWorker()
        try:
            worker.submit("add", lambda: 1 + 2)
            deadline = time.time() + 2
            results = []
            while time.time() < deadline and not results:
                results = worker.poll_results()
                if not results:
                    time.sleep(0.01)

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].task_name, "add")
            self.assertEqual(results[0].payload, 3)
            self.assertIsNone(results[0].error)
        finally:
            worker.shutdown(wait=True)

    def test_background_worker_returns_exception_result(self) -> None:
        worker = BackgroundWorker()
        try:
            worker.submit("boom", lambda: (_ for _ in ()).throw(ValueError("bad")))
            deadline = time.time() + 2
            results = []
            while time.time() < deadline and not results:
                results = worker.poll_results()
                if not results:
                    time.sleep(0.01)

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].task_name, "boom")
            self.assertIsInstance(results[0].error, ValueError)
            self.assertEqual(str(results[0].error), "bad")
        finally:
            worker.shutdown(wait=True)

    def test_shutdown_waits_for_running_task_to_finish(self) -> None:
        worker = BackgroundWorker()
        try:
            worker.submit("slow", lambda: (time.sleep(0.05), 7)[1])
            worker.shutdown(wait=True)
            results = worker.poll_results()

            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].task_name, "slow")
            self.assertEqual(results[0].payload, 7)
            self.assertIsNone(results[0].error)
        finally:
            worker.shutdown(wait=True)

    def test_shutdown_wait_joins_without_timeout(self) -> None:
        worker = BackgroundWorker()
        try:
            with mock.patch.object(worker._thread, "join", wraps=worker._thread.join) as join_mock:
                worker.shutdown(wait=True)

            self.assertGreaterEqual(join_mock.call_count, 1)
            self.assertEqual(join_mock.call_args.args, ())
            self.assertEqual(join_mock.call_args.kwargs, {})
        finally:
            worker.shutdown(wait=True)


if __name__ == "__main__":
    unittest.main()
