import time

from .conftest import Worker
from .conftest import BlankWorker
from .conftest import happens_soon
from .conftest import doesnt_happen
from .conftest import example_message
from .conftest import VERY_FAST_TIMEOUT

import mpcontroller as mpc


def test_dead_on_controller_init():
    controller = BlankWorker.controller()
    assert controller.status == mpc.DEAD


def test_idle_when_not_handling_messages():
    controller = MainloopImplementationWorker.spawn()

    @happens_soon
    def worker_goes_idle():
        assert controller.status == mpc.IDLE

    @doesnt_happen
    def worker_goes_busy():
        assert controller.status == mpc.BUSY


def test_busy_when_handling_messages():
    controller = BusyMessageWorker.spawn()
    controller.send(example_message)

    @happens_soon
    def worker_goes_busy():
        assert controller.status == mpc.BUSY


def test_dead_after_join():
    controller = BlankWorker.spawn()
    controller.join()
    assert controller.status == mpc.DEAD


def test_dead_after_kill():
    controller = BlankWorker.spawn()
    controller.kill()
    assert controller.status == mpc.DEAD


class MainloopImplementationWorker(Worker):
    def main(self):
        time.sleep(VERY_FAST_TIMEOUT)


class BusyMessageWorker(Worker):
    @mpc.message_handler(type(example_message))
    def busy_handler(self, _):
        time.sleep(VERY_FAST_TIMEOUT)
