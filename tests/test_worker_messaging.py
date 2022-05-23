import time

import pytest

from .conftest import Worker
from .conftest import ExampleSignal
from .conftest import ExampleTask
from .conftest import RecordedCallback
from .conftest import doesnt_happen
from .conftest import example_task
from .conftest import happens_soon
from .conftest import VERY_FAST_TIMEOUT

import mpcontroller as mpc


def test_worker_handles_task_with_registered_callback():
    worker = Echo.spawn()
    worker.send(example_task)

    @happens_soon
    def message_is_sent_back():
        worker.record.assert_called_with(example_task)

    @doesnt_happen
    def more_messages_arrive():
        assert worker.record.called > 1


def test_worker_handles_signal_with_registered_callback():
    worker = Echo.spawn()
    worker.send(ExampleSignal)

    @happens_soon
    def message_is_sent_back():
        assert worker.record.called

    @doesnt_happen
    def more_messages_arrive():
        assert worker.record.called > 1


def test_worker_handles_signal_as_many_times_as_it_is_sent():
    worker = Echo.spawn()

    worker.send(ExampleSignal)
    worker.send(ExampleSignal)

    @happens_soon
    def multiple_responses():
        worker.record.called == 2


def test_worker_can_register_multiple_callbacks_for_a_single_message():
    worker = EchoTwice.spawn()
    worker.send(example_task)

    @happens_soon
    def message_is_sent_back_twice():
        worker.record.nth(0).assert_called_with(example_task)
        worker.record.nth(1).assert_called_with(example_task)


def test_worker_handles_messages_in_the_order_sent():
    worker = Echo.spawn()
    example_tasks = [ExampleTask(i) for i in range(3)]

    for task in example_tasks:
        worker.send(task)

    @happens_soon
    def responses_arive_in_order():
        for i in range(3):
            worker.record.nth(i).assert_called_with(example_tasks[i])


def test_worker_handles_all_tasks_before_joining():
    worker = Echo.spawn()
    example_tasks = [ExampleTask(i) for i in range(3)]

    for task in example_tasks:
        worker.send(task)

    worker.join()

    @happens_soon
    def responses_are_polled_here_in_the_main_thread():
        for i in range(3):
            worker.record.nth(i).assert_called_with(example_tasks[i])


def test_worker_tasks_dont_block_mainloop():
    worker = BlockedEcho.spawn()

    time.sleep(VERY_FAST_TIMEOUT / 10)
    worker.send(example_task)

    @happens_soon
    def response_received():
        worker.record.called


def test_helpful_exception_when_worker_or_main_handler_not_chosen():
    with pytest.raises(RuntimeError) as excinfo:

        class Incorrect(Worker):
            @mpc.handler(ExampleTask)
            def bad_syntax(self, task):
                pass

    assert "@mpc.handler.main" in str(excinfo.value)


def test_messaging_all_workers():
    workers = [Echo.spawn() for i in range(2)]
    mpc.send_all(example_task)

    @happens_soon
    def all_workers_record_response_in_main():
        for worker in workers:
            worker.record.assert_called_with(example_task)


def test_messaging_all_workers_of_a_given_type():
    echo_once = [Echo.spawn() for i in range(2)]
    echo_twice = [EchoTwice.spawn() for i in range(2)]
    mpc.send_all(example_task, type=Echo)

    @happens_soon
    def only_echo_once_workers_recieve_response():
        for worker in echo_once:
            worker.record.assert_called_with(example_task)
        for worker in echo_twice:
            assert not worker.record.called


class RecordedWorker(Worker):
    def __init__(self, *args, **kwargs):
        self.record = RecordedCallback()
        super().__init__(*args, **kwargs)


class Echo(RecordedWorker):
    @mpc.handler.worker(ExampleTask)
    def echo_task(self, task):
        self.send(task)

    @mpc.handler.worker(ExampleSignal)
    def echo_sig(self):
        self.send(ExampleSignal)

    @mpc.handler.main(ExampleTask)
    def record_task(self, task):
        self.record(task)

    @mpc.handler.main(ExampleSignal)
    def record_sig(self):
        self.record(ExampleSignal)


class EchoTwice(Echo):
    @mpc.handler.worker(ExampleTask)
    def echo_task_again(self, task):
        self.send(task)

    @mpc.handler.worker(ExampleSignal)
    def echo_signal_again(self):
        self.send(ExampleSignal)


class SlowEcho(Echo):
    def echo_task(self, task):
        time.sleep(VERY_FAST_TIMEOUT / 10)
        self.send(task)


class BlockedEcho(Echo):
    def main(self):
        time.sleep(VERY_FAST_TIMEOUT)
