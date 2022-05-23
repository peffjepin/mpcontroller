import time

from .conftest import BlankWorker
from .conftest import ExampleTask
from .conftest import happens_soon
from .conftest import doesnt_happen
from .conftest import example_task
from .conftest import VERY_FAST_TIMEOUT

import mpcontroller as mpc


def test_dead_on_controller_init():
    worker = BlankWorker()

    assert worker.status == mpc.DEAD


def test_idle_when_not_handling_messages():
    worker = MainloopImplementationWorker.spawn()

    @happens_soon
    def worker_goes_idle():
        assert worker.status == mpc.IDLE

    @doesnt_happen
    def worker_goes_busy():
        assert worker.status == mpc.BUSY


def test_busy_when_handling_messages():
    worker = BusyMessageWorker.spawn()
    worker.send(example_task)

    @happens_soon
    def worker_goes_busy():
        assert worker.status == mpc.BUSY


def test_dead_after_join():
    worker = BlankWorker.spawn()
    worker.join()
    assert worker.status == mpc.DEAD


def test_dead_after_kill():
    worker = BlankWorker.spawn()
    worker.kill()
    assert worker.status == mpc.DEAD


class MainloopImplementationWorker(mpc.Worker):
    def mainloop(self):
        time.sleep(VERY_FAST_TIMEOUT)


class BusyMessageWorker(mpc.Worker):
    @mpc.handler.worker(ExampleTask)
    def busy_handler(self, _):
        time.sleep(VERY_FAST_TIMEOUT)
