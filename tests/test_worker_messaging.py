import pytest

from .conftest import happens_soon
from .conftest import doesnt_happen
from .conftest import exception_soon
from .conftest import Worker
from .conftest import Controller
from .conftest import BlankWorker
from .conftest import RecordedController
from .conftest import ExampleMessage
from .conftest import example_message
from .conftest import ExampleSignal

import mpcontroller as mpc


def test_sending_a_worker_an_unknown_message():
    controller = BlankWorker.spawn()

    with pytest.raises(mpc.UnknownMessageError) as excinfo:
        controller.send(example_message)

    assert excinfo.value == mpc.UnknownMessageError(
        example_message, controller._worker
    )


def test_sending_a_worker_an_unknown_signal():
    controller = BlankWorker.spawn()

    with pytest.raises(mpc.UnknownMessageError) as excinfo:
        controller.send(ExampleSignal)

    assert excinfo.value == mpc.UnknownMessageError(
        ExampleSignal, controller._worker
    )


def test_sending_a_controller_an_unknown_message():
    controller = EchoWithIncompatibleController.spawn()

    exception = mpc.UnknownMessageError(example_message, repr(controller))

    @exception_soon(exception)
    def cause():
        controller.send(example_message)


def test_worker_handles_message_with_registered_callback():
    controller = Echo.spawn()
    controller.send(example_message)

    @happens_soon
    def message_is_sent_back():
        controller.msg_cb.assert_called_with(example_message)


def test_worker_handles_signal_with_registered_callback():
    controller = Echo.spawn()
    controller.send(ExampleSignal)

    @happens_soon
    def message_is_sent_back():
        controller.msg_cb.assert_called_with(example_message)

    @doesnt_happen
    def more_messages_arrive():
        assert controller.msg_cb.called > 1


def test_worker_can_register_multiple_callbacks_for_a_single_message():
    controller = EchoTwice.spawn()
    controller.send(example_message)

    @happens_soon
    def message_is_sent_back_twice():
        controller.msg_cb.nth(0).assert_called_with(example_message)
        controller.msg_cb.nth(1).assert_called_with(example_message)


def test_worker_handles_messages_in_the_order_sent():
    controller = Echo.spawn()

    example_messages = [ExampleMessage(i) for i in range(3)]
    for m in example_messages:
        controller.send(m)

    @happens_soon
    def responses_arive_in_order():
        for i in range(3):
            controller.msg_cb.nth(i).assert_called_with(example_messages[i])


def test_messaging_all_workers():
    controllers = [Echo.spawn() for i in range(2)]
    mpc.message_all(example_message)

    @happens_soon
    def all_controllers_recieve_resposne():
        for c in controllers:
            c.msg_cb.assert_called_with(example_message)


def test_messaging_all_workers_of_a_given_type():
    echo_once = [Echo.spawn() for i in range(2)]
    echo_twice = [EchoTwice.spawn() for i in range(2)]
    mpc.message_all(example_message, type=Echo)

    @happens_soon
    def only_echo_once_controllers_are_called():
        for c in echo_once:
            c.msg_cb.assert_called_with(example_message)
        for c in echo_twice:
            assert not c.msg_cb.called


def test_message_callbacks_are_inherited_if_not_overwritten():
    controller = EchoChild.spawn()
    controller.send(example_message)

    @happens_soon
    def message_is_echoed():
        controller.msg_cb.assert_called_with(example_message)


def test_message_callbacks_can_be_overwritten():
    controller = EchoChildReimplementedAsEchoTwice.spawn()
    controller.send(example_message)

    controller.join()

    # should only get the two overwritten calls
    assert controller.msg_cb.called == 2


class Echo(Worker):
    CONTROLLER = RecordedController

    @mpc.message_handler(ExampleMessage)
    def echomsg(self, msg):
        self.send(msg)

    @mpc.signal_handler(ExampleSignal)
    def echosig(self):
        self.send(example_message)


class EchoTwice(Worker):
    CONTROLLER = RecordedController

    @mpc.message_handler(ExampleMessage)
    def echomsg1(self, msg):
        self.send(example_message)

    @mpc.message_handler(ExampleMessage)
    def echomsg2(self, msg):
        self.send(example_message)


class EchoWithIncompatibleController(Worker):
    CONTROLLER = Controller

    @mpc.message_handler(ExampleMessage)
    def echomsg(self, msg):
        self.send(msg)


class EchoChild(Echo):
    pass


class EchoChildReimplementedAsEchoTwice(Echo):
    def echomsg(self, msg):
        self.send(msg)
        self.send(msg)
