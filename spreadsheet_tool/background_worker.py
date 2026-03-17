from __future__ import annotations

from dataclasses import dataclass
from queue import Empty, Queue
from threading import Thread
from typing import Callable


@dataclass(slots=True)
class BackgroundTaskResult:
    task_name: str
    payload: object | None = None
    error: Exception | None = None


class BackgroundWorker:
    def __init__(self) -> None:
        self._task_queue: Queue[tuple[str, Callable[[], object]] | None] = Queue()
        self._result_queue: Queue[BackgroundTaskResult] = Queue()
        self._shutdown_requested = False
        self._thread = Thread(target=self._run, name="spreadsheet-tool-worker", daemon=False)
        self._thread.start()

    def submit(self, task_name: str, task_func: Callable[[], object]) -> None:
        if self._shutdown_requested:
            raise RuntimeError("Background worker has been shut down.")
        self._task_queue.put((task_name, task_func))

    def poll_results(self) -> list[BackgroundTaskResult]:
        results: list[BackgroundTaskResult] = []
        while True:
            try:
                results.append(self._result_queue.get_nowait())
            except Empty:
                break
        return results

    def shutdown(self, wait: bool = False) -> None:
        if not self._shutdown_requested:
            self._shutdown_requested = True
            self._task_queue.put(None)
        if wait and self._thread.is_alive():
            self._thread.join()

    def _run(self) -> None:
        while True:
            task = self._task_queue.get()
            if task is None:
                return
            task_name, task_func = task
            try:
                payload = task_func()
            except Exception as exc:  # noqa: BLE001
                self._result_queue.put(BackgroundTaskResult(task_name=task_name, error=exc))
            else:
                self._result_queue.put(BackgroundTaskResult(task_name=task_name, payload=payload))
