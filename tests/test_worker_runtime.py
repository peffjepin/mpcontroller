from .conftest import happens_soon
from .conftest import exception_soon
from .conftest import Worker
from .conftest import RecordedController
from .conftest import ExampleMessage
from .conftest import EqualityException
from .conftest import FAST_TIMEOUT

import mpcontroller as mpc


def test_setup_is_called_first():
    controller = VerboseWorker.spawn()

    @happens_soon
    def setup_message_shows_up():
        assert controller.msg_cb.nth(0).called_with(
            VerboseWorker.SETUP_MESSAGE
        )


def test_main_executes_in_a_loop():
    controller = VerboseWorker.spawn()

    @happens_soon
    def multiple_main_messages_appear():
        assert controller.msg_cb.nth(0).called_with(
            VerboseWorker.SETUP_MESSAGE
        )
        assert controller.msg_cb.nth(1).called_with(VerboseWorker.MAIN_MESSAGE)
        assert controller.msg_cb.nth(2).called_with(VerboseWorker.MAIN_MESSAGE)


def test_teardown_executes_before_exit():
    controller = VerboseWorker.spawn()

    @happens_soon
    def process_begins_execution():
        assert controller.msg_cb.nth(0).called_with(
            VerboseWorker.SETUP_MESSAGE
        )
        assert controller.msg_cb.nth(1).called_with(VerboseWorker.MAIN_MESSAGE)

    controller.join(FAST_TIMEOUT)
    assert controller.msg_cb.nth(-1).called_with(
        VerboseWorker.TEARDOWN_MESSAGE
    )


def test_teardown_still_executes_after_an_error_occurs():
    controller = VerboseWorkerErrorInMain.controller()

    @exception_soon(VerboseWorkerErrorInMain.EXC)
    def cause():
        controller.spawn()

    assert controller.msg_cb.nth(-1).called_with(
        VerboseWorker.TEARDOWN_MESSAGE
    )


class VerboseWorker(Worker):
    CONTROLLER = RecordedController

    SETUP_MESSAGE = ExampleMessage("setup")
    MAIN_MESSAGE = ExampleMessage("main")
    HANDLER_MESSAGE = ExampleMessage("handler")
    TEARDOWN_MESSAGE = ExampleMessage("teardown")

    def setup(self):
        self.send_message(self.SETUP_MESSAGE)

    def main(self):
        self.send_message(self.MAIN_MESSAGE)

    def teardown(self):
        self.send_message(self.TEARDOWN_MESSAGE)

    @mpc.message_handler(ExampleMessage)
    def handler(self, _):
        self.send_message(self.HANDLER_MESSAGE)


class VerboseWorkerErrorInMain(VerboseWorker):
    EXC = EqualityException("error in main")

    def main(self):
        raise self.EXC


class VerboseWorkerErrorInTeardown(VerboseWorker):
    EXC = EqualityException("error in teardown")

    def teardown(self):
        super().teardown()
        raise self.EXC
