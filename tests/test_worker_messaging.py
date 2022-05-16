import pytest

from .conftest import happens_soon
from .conftest import exception_soon
from .conftest import Worker
from .conftest import Controller
from .conftest import BlankWorker
from .conftest import RecordedController
from .conftest import ExampleMessage
from .conftest import example_message
from .conftest import example_signal

import mpcontroller as mpc


def test_sending_a_worker_an_unknown_message():
    controller = BlankWorker.spawn()

    with pytest.raises(mpc.UnknownMessageError) as excinfo:
        controller.send_message(example_message)

    assert excinfo.value == mpc.UnknownMessageError(
        controller.worker, example_message
    )


def test_sending_a_worker_an_unknown_signal():
    controller = BlankWorker.spawn()

    with pytest.raises(mpc.UnknownMessageError) as excinfo:
        controller.send_message(example_signal)

    assert excinfo.value == mpc.UnknownMessageError(
        controller.worker, example_signal
    )


def test_sending_a_controller_an_unknown_message():
    controller = Controller(Echo)
    controller.spawn()

    exception = mpc.UnknownMessageError(repr(controller), example_message)
    assert controller.id in str(exception)

    @exception_soon(exception)
    def cause():
        controller.send_message(example_message)


def test_worker_handles_message_with_registered_callback():
    controller = Echo.spawn()
    controller.send_message(example_message)

    @happens_soon
    def message_is_sent_back():
        assert controller.example_message_cb.called_with(example_message)


def test_worker_handles_signal_with_registered_callback():
    controller = Echo.spawn()
    controller.send_message(example_signal)

    @happens_soon
    def message_is_sent_back():
        assert controller.example_message_cb.called_with()


def test_worker_can_register_multiple_callbacks_for_a_single_message():
    controller = EchoTwice.spawn()
    controller.send_message(example_message)

    @happens_soon
    def message_is_sent_back_twice():
        assert controller.example_message_cb.nth(0).called_with(
            example_message
        )
        assert controller.example_message_cb.nth(1).called_with(
            example_message
        )


def test_worker_handles_messages_in_the_order_sent():
    controller = Echo.spawn()

    example_messages = [ExampleMessage(i) for i in range(3)]
    for m in example_messages:
        controller.send_message(m)

    @happens_soon
    def responses_arive_in_order():
        for i in range(3):
            assert controller.example_message_cb.nth(i).called_with(
                example_messages[i]
            )


def test_messaging_all_workers():
    controllers = [Echo.spawn() for i in range(2)]
    mpc.message_all(example_message)

    @happens_soon
    def all_controllers_recieve_resposne():
        assert all(
            c.example_message_cb.called_with(example_message)
            for c in controllers
        )


def test_messaging_all_workers_of_a_given_type():
    echo_once = [Echo.spawn() for i in range(2)]
    echo_twice = [EchoTwice.spawn() for i in range(2)]
    mpc.message_all(example_message, type=Echo)

    @happens_soon
    def only_echo_once_controllers_are_called():
        assert all(
            c.example_message_cb.called_with(example_message)
            for c in echo_once
        )
        assert all(not c.example_message_cb.called for c in echo_twice)


class Echo(Worker):
    CONTROLLER = RecordedController

    @mpc.message_handler(type(example_message))
    def echomsg(self, msg):
        self.send_message(example_message)

    @mpc.message_handler(example_signal)
    def echosig(self):
        self.send_message(example_signal)


class EchoTwice(Worker):
    CONTROLLER = RecordedController

    @mpc.message_handler(type(example_message))
    def echomsg1(self, msg):
        self.send_message(example_message)

    @mpc.message_handler(type(example_message))
    def echomsg2(self, msg):
        self.send_message(example_message)
