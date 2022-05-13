import typing
import time

import pytest
from .conftest import happens_soon, FAST_TIMEOUT, EqualityException

import mpcontroller as mpc


class ExampleMessage(typing.NamedTuple):
    content: str


class ExampleWorker(mpc.Worker):
    pass


@pytest.fixture
def message():
    return ExampleMessage("testing")


@pytest.fixture
def controller():
    controller = mpc.Controller(ExampleWorker)
    yield controller
    controller.kill()


def test_controller_on_init(controller):
    assert controller.status == mpc.Worker.DEAD
    assert controller.worker is None
    assert controller.pid is None


def test_spawning_a_worker(controller):
    controller.spawn()

    assert controller.status == mpc.Worker.INITIALIZING

    @happens_soon
    def worker_initializes():
        assert not controller.exceptions
        assert controller.status == mpc.Worker.IDLE
        assert controller.worker is not None
        assert controller.pid is not None


def test_sending_a_worker_an_unknown_message(controller, message):
    controller.spawn()
    controller.send_message(message)

    exc = mpc.UnknownMessageError(recipient=controller.worker, msg=message)

    @happens_soon
    def unknown_message_exception_appears():
        assert exc in controller.exceptions


def test_recieving_a_response_from_a_valid_message(message):
    class MyWorker(mpc.Worker):
        @mpc.message_handler(type(message))
        def echo(self, msg):
            self.send_message(msg)

    controller = MyWorker.spawn()
    controller.send_message(message)

    @happens_soon
    def response_is_recieved():
        assert not controller.exceptions
        assert message in controller.messages


def test_joining_a_worker(controller):
    controller.spawn()

    assert controller.status == mpc.Worker.INITIALIZING
    controller.join(timeout=FAST_TIMEOUT)

    assert not controller.exceptions
    assert controller.status == mpc.Worker.DEAD
    assert controller.pid is None
    assert controller.worker is None


def test_killing_a_worker(controller):
    controller.spawn()
    controller.kill()

    assert controller.status == mpc.Worker.DEAD
    assert controller.pid is None
    assert controller.worker is None


def test_worker_main_executes_on_loop():
    class MyWorker(mpc.Worker):
        value = 0

        def main(self):
            self.value += 1
            self.send_message(ExampleMessage(self.value))

    controller = MyWorker.spawn()

    expected = tuple(ExampleMessage(i + 1) for i in range(2))

    @happens_soon
    def multiple_messages_recieved():
        assert not controller.exceptions
        assert all(msg in controller.messages for msg in expected)


def test_worker_setup():
    class MyWorker(mpc.Worker):
        value = 0

        def setup(self):
            self.value += 1

        def main(self):
            self.send_message(ExampleMessage(self.value))

    controller = MyWorker.spawn()
    expected = ExampleMessage(1)

    @happens_soon
    def expected_message_is_recieved():
        assert not controller.exceptions
        assert expected in controller.messages


def test_worker_can_communicate_while_busy(message):
    class MyWorker(mpc.Worker):
        @mpc.message_handler(type(message))
        def sleep(self, msg):
            time.sleep(FAST_TIMEOUT * 5)

    controller = MyWorker.spawn()
    controller.send_message(message)
    previous_check = controller.last_health_check
    controller.send_message(mpc.Signal.HEALTH_CHECK)

    @happens_soon
    def a_new_health_check_arrives():
        assert not controller.exceptions
        assert controller.last_health_check > previous_check
        assert controller.status == mpc.Worker.BUSY


def test_worker_can_handle_the_same_message_multiple_times(message):
    class MyWorker(mpc.Worker):
        @mpc.message_handler(ExampleMessage)
        def echo(self, msg):
            self.send_message(msg)

        @mpc.message_handler(ExampleMessage)
        def echo2(self, msg):
            self.send_message(msg)
            self.send_message(msg)

    controller = MyWorker.spawn()
    controller.send_message(message)

    @happens_soon
    def should_get_3_messages_back():
        assert not controller.exceptions
        assert len(controller.messages) == 3
        assert all(msg == message for msg in controller.messages)


def test_worker_can_handle_signals(message):
    class MyWorker(mpc.Worker):
        @mpc.message_handler(mpc.Signal.HEALTH_CHECK)
        def handler(self):
            self.send_message(message)

    controller = MyWorker.spawn()
    controller.send_message(mpc.Signal.HEALTH_CHECK)

    @happens_soon
    def should_recieve_response():
        assert not controller.exceptions
        assert message in controller.messages


def test_worker_error_in_main(exception):
    class MyWorker(mpc.Worker):
        def main(self):
            raise exception

    controller = MyWorker.spawn()
    assert controller.status == mpc.Worker.INITIALIZING

    @happens_soon
    def worker_dies():
        assert exception in controller.exceptions
        assert mpc.WorkerExitError(controller.id) in controller.exceptions
        assert len(controller.exceptions) == 2
        assert controller.status == mpc.Worker.DEAD


def test_worker_error_in_setup(exception):
    class MyWorker(mpc.Worker):
        def setup(self):
            raise exception

    controller = MyWorker.spawn()
    assert controller.status == mpc.Worker.INITIALIZING

    @happens_soon
    def worker_dies():
        assert exception in controller.exceptions
        assert mpc.WorkerExitError(controller.id) in controller.exceptions
        assert len(controller.exceptions) == 2
        assert controller.status == mpc.Worker.DEAD


def test_worker_error_in_message_handler(message, exception):
    class MyWorker(mpc.Worker):
        @mpc.message_handler(type(message))
        def error(self, msg):
            raise exception

    controller = MyWorker.spawn()
    assert controller.status == mpc.Worker.INITIALIZING
    controller.send_message(message)

    @happens_soon
    def worker_dies():
        assert exception in controller.exceptions
        assert mpc.WorkerExitError(controller.id) in controller.exceptions
        assert len(controller.exceptions) == 2
        assert controller.status == mpc.Worker.DEAD


def test_worker_with_error_tolerance_3():
    class MyWorker(mpc.Worker):
        ERROR_TOLERANCE = 3

        def setup(self):
            self.i = 0

        def main(self):
            self.i += 1
            raise EqualityException(self.i)

    expected1 = EqualityException(1)
    expected2 = EqualityException(2)
    expected3 = EqualityException(3)
    controller = MyWorker.spawn()
    exitexc = mpc.WorkerExitError(controller.id)

    @happens_soon
    def recieve_3_errors_before_death():
        assert expected1 in controller.exceptions
        assert expected2 in controller.exceptions
        assert expected3 in controller.exceptions
        assert exitexc in controller.exceptions
        assert controller.status == mpc.Worker.DEAD
        assert len(controller.exceptions) == 4


def test_spawn_errors_if_worker_already_exists(controller):
    controller.spawn()

    with pytest.raises(mpc.WorkerExistsError) as excinfo:
        controller.spawn()

    assert str(controller.id) in str(excinfo.value)


def test_can_spawn_new_worker_with_controller(controller):
    controller.spawn()

    assert controller.status == mpc.Worker.INITIALIZING

    @happens_soon
    def worker_goes_idle():
        assert controller.status == mpc.Worker.IDLE

    controller.kill()
    controller.spawn()

    assert controller.status == mpc.Worker.INITIALIZING

    @happens_soon
    def worker_goes_idle_again():
        assert controller.status == mpc.Worker.IDLE


def test_worker_exit_exception():
    class MyWorker(mpc.Worker):
        def main(self):
            raise Exception()

    controller = MyWorker.spawn()
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
