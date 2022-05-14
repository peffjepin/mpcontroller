import typing
import time

import pytest
from .conftest import (
    happens_soon,
    FAST_TIMEOUT,
    EqualityException,
    Worker,
    Controller,
)

import mpcontroller as mpc


class ExampleMessage(typing.NamedTuple):
    content: str


class ExampleWorker(Worker):
    pass


test_exception = EqualityException("testing")
test_message = ExampleMessage("testing")


@pytest.fixture
def message():
    return ExampleMessage("testing")


@pytest.fixture
def controller():
    controller = Controller(ExampleWorker)
    yield controller
    controller.kill()


def test_controller_on_init(controller):
    assert controller.status == Worker.DEAD
    assert controller.worker is None
    assert controller.pid is None


def test_spawning_a_worker(controller):
    controller.spawn()

    @happens_soon
    def worker_initializes():
        assert not controller.exceptions
        assert controller.status == Worker.IDLE
        assert controller.worker is not None
        assert controller.pid is not None


def test_sending_a_worker_an_unknown_message(controller, message):
    controller.spawn()
    controller.send_message(message)

    exc = mpc.UnknownMessageError(recipient=controller.worker, msg=message)

    @happens_soon
    def unknown_message_exception_appears():
        assert exc in controller.exceptions


class Echo(Worker):
    @mpc.message_handler(ExampleMessage)
    def echo(self, msg):
        self.send_message(msg)


def test_recieving_a_response_from_a_valid_message(message):
    controller = Echo.spawn()
    controller.send_message(message)

    @happens_soon
    def response_is_recieved():
        assert not controller.exceptions
        assert message in controller.messages


def test_joining_a_worker(controller):
    controller.spawn()
    controller.join(timeout=FAST_TIMEOUT)

    assert not controller.exceptions
    assert controller.status == Worker.DEAD
    assert controller.pid is None
    assert controller.worker is None


def test_killing_a_worker(controller):
    controller.spawn()
    controller.kill()

    assert controller.status == Worker.DEAD
    assert controller.pid is None
    assert controller.worker is None


class SimpleMain(Worker):
    value = 0

    def main(self):
        self.value += 1
        self.send_message(ExampleMessage(self.value))


def test_worker_main_executes_on_loop():
    controller = SimpleMain.spawn()

    expected = tuple(ExampleMessage(i + 1) for i in range(2))

    @happens_soon
    def multiple_messages_recieved():
        assert not controller.exceptions
        assert all(msg in controller.messages for msg in expected)


class SimpleSetup(Worker):
    value = 0

    def setup(self):
        self.value += 1

    def main(self):
        self.send_message(ExampleMessage(self.value))


def test_worker_setup():
    controller = SimpleSetup.spawn()
    expected = ExampleMessage(1)

    @happens_soon
    def expected_message_is_recieved():
        assert not controller.exceptions
        assert expected in controller.messages


class Blocking(Worker):
    @mpc.message_handler(ExampleMessage)
    def sleep(self, msg):
        time.sleep(FAST_TIMEOUT * 2)


def test_worker_can_communicate_while_busy(message):
    controller = Blocking.spawn()
    controller.send_message(message)
    previous_check = controller.last_health_check
    controller.send_message(mpc.Signal.HEALTH_CHECK)

    @happens_soon
    def a_new_health_check_arrives():
        assert not controller.exceptions
        assert controller.last_health_check > previous_check
        assert controller.status == Worker.BUSY


class MultiEcho(Worker):
    @mpc.message_handler(ExampleMessage)
    def echo(self, msg):
        self.send_message(msg)

    @mpc.message_handler(ExampleMessage)
    def echo2(self, msg):
        self.send_message(msg)
        self.send_message(msg)


def test_worker_can_handle_the_same_message_multiple_times(message):
    controller = MultiEcho.spawn()
    controller.send_message(message)

    @happens_soon
    def should_get_3_messages_back():
        assert not controller.exceptions
        assert len(controller.messages) == 3
        assert all(msg == message for msg in controller.messages)


class HandlesSignal(Worker):
    @mpc.message_handler(mpc.Signal.HEALTH_CHECK)
    def handler(self):
        self.send_message(test_message)


def test_worker_can_handle_signals():
    controller = HandlesSignal.spawn()
    controller.send_message(mpc.Signal.HEALTH_CHECK)

    @happens_soon
    def should_recieve_response():
        assert not controller.exceptions
        assert test_message in controller.messages


class ExceptionInMain(Worker):
    def main(self):
        raise test_exception


def test_worker_error_in_main():
    controller = ExceptionInMain.spawn()

    @happens_soon
    def worker_dies():
        assert test_exception in controller.exceptions
        assert mpc.WorkerExitError(controller.id) in controller.exceptions
        assert len(controller.exceptions) == 2
        assert controller.status == Worker.DEAD


class ExceptionInSetup(Worker):
    def setup(self):
        raise test_exception


def test_worker_error_in_setup(exception):
    controller = ExceptionInSetup.spawn()

    @happens_soon
    def worker_dies():
        assert test_exception in controller.exceptions
        assert mpc.WorkerExitError(controller.id) in controller.exceptions
        assert len(controller.exceptions) == 2
        assert controller.status == Worker.DEAD


class ExceptionInHandler(Worker):
    @mpc.message_handler(ExampleMessage)
    def error(self, msg):
        raise test_exception


def test_worker_error_in_message_handler(message, exception):
    controller = ExceptionInHandler.spawn()
    controller.send_message(message)

    @happens_soon
    def worker_dies():
        assert test_exception in controller.exceptions
        assert mpc.WorkerExitError(controller.id) in controller.exceptions
        assert len(controller.exceptions) == 2
        assert controller.status == Worker.DEAD


class Error3(Worker):
    ERROR_TOLERANCE = 3

    def setup(self):
        self.i = 0

    def main(self):
        self.i += 1
        raise EqualityException(self.i)


def test_worker_with_error_tolerance_3():
    expected1 = EqualityException(1)
    expected2 = EqualityException(2)
    expected3 = EqualityException(3)
    controller = Error3.spawn()
    exitexc = mpc.WorkerExitError(controller.id)

    @happens_soon
    def recieve_3_errors_before_death():
        assert expected1 in controller.exceptions
        assert expected2 in controller.exceptions
        assert expected3 in controller.exceptions
        assert exitexc in controller.exceptions
        assert controller.status == Worker.DEAD
        assert len(controller.exceptions) == 4


def test_spawn_errors_if_worker_already_exists(controller):
    controller.spawn()

    with pytest.raises(mpc.WorkerExistsError) as excinfo:
        controller.spawn()

    assert str(controller.id) in str(excinfo.value)


def test_can_spawn_new_worker_with_controller(controller):
    controller.spawn()

    @happens_soon
    def worker_goes_idle():
        assert controller.status == Worker.IDLE

    controller.kill()
    controller.spawn()

    @happens_soon
    def worker_goes_idle_again():
        assert controller.status == Worker.IDLE


def test_worker_exit_exception():
    controller = ExceptionInMain.spawn()
    exc = mpc.WorkerExitError(controller.id)
    assert exc.id == controller.id

    @happens_soon
    def worker_exit_exception_appears():
        assert exc in controller.exceptions


def test_controller_id_remains_constant_between_workers(controller):
    id = controller.id

    controller.spawn()
    assert controller.id == id
    controller.kill()
    controller.spawn()
    assert controller.id == id
