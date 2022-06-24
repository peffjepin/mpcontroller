import multiprocessing as mp
import traceback

import pytest

from .conftest import (
    ExampleTask,
    ExampleSignal,
    happens_soon,
    exception_soon,
    exception_soon_repeat,
)

import mpcontroller as mpc

from mpcontroller import ipc
from mpcontroller import config


MAIN_CONTEXT_NAME = "testing-main"
WORKER_CONTEXT_NAME = "testing-worker"

config.local_context.name = MAIN_CONTEXT_NAME
traceback.format_exc = lambda: ""


def unknown_sent_from_worker(msg):
    config.local_context.name = WORKER_CONTEXT_NAME
    exc = mpc.UnknownMessageError(msg)
    config.local_context.name = MAIN_CONTEXT_NAME
    return exc


def unknown_sent_from_main(msg):
    exc = mpc.UnknownMessageError(msg)
    return exc


@pytest.fixture(autouse=True, scope="function")
def _kill_remaining_processes():
    yield
    while IPCTestCase.processes:
        try:
            IPCTestCase.processes.pop().kill()
        except Exception:
            pass


class IPCTestCase(mp.Process):
    processes = []

    def __init__(self, manager=None):
        self.manager = manager or ipc.CommunicationManager()
        self._success = mp.Value("i", 0)
        self._shared_trigger = mp.Value("i", 0)
        self.exception = None
        self.processes.append(self)
        super().__init__(target=self.main)

    def main(self):
        traceback.format_exc = lambda: ""
        config.local_context.name = WORKER_CONTEXT_NAME

        self.manager.start(auto=True)
        while True:
            try:
                if self._shared_trigger.value == 1:
                    self._shared_trigger.value = 2
                    self.process_trigger()
                self.check_process_state()
            except Exception as exc:
                self.exception = exc

    def trigger(self):
        self.local_trigger()
        self._process_trigger()

    @property
    def success(self):
        return self._success.value != 0

    @success.setter
    def success(self, value):
        self._success.value = 1

    def check_process_state(self):
        pass

    def local_trigger(self):
        pass

    def process_trigger(self):
        pass

    def blank(self):
        pass

    def _process_trigger(self):
        self._shared_trigger.value = 1


class LeadsToError(IPCTestCase):
    pass


class ErrorImmediately(IPCTestCase):
    pass


class LeadsToSuccessFlag(IPCTestCase):
    pass


class UnknownTaskSentToWorker(ErrorImmediately):
    task = ExampleTask()
    expected = unknown_sent_from_main(task)

    def local_trigger(self):
        self.manager.send(self.task)


class UnknownTaskSentToMain(LeadsToError):
    task = ExampleTask()
    expected = unknown_sent_from_worker(task)

    def process_trigger(self):
        self.manager.send(self.task)


class UnknownSignalSentToWorker(ErrorImmediately):
    expected = unknown_sent_from_main(ExampleSignal)

    def local_trigger(self):
        self.manager.send(ExampleSignal)


class UnknownSignalSentToMain(LeadsToError):
    expected = unknown_sent_from_worker(ExampleSignal)

    def process_trigger(self):
        self.manager.send(ExampleSignal)


class KnownTaskSentToWorker(LeadsToSuccessFlag):
    task = ExampleTask()

    def local_trigger(self):
        self.manager.send(self.task)

    def __init__(self):
        manager = ipc.CommunicationManager(
            worker_messages={ExampleTask: [self.succeed]},
        )
        super().__init__(manager)

    def succeed(self, task):
        self.success = True

    def check_process_state(self):
        if self.manager.taskq:
            handler = self.manager.taskq.popleft()
            handler()


class KnownSignalSentToWorker(LeadsToSuccessFlag):
    def local_trigger(self):
        self.manager.send(ExampleSignal)

    def __init__(self):
        manager = ipc.CommunicationManager(
            worker_signals={ExampleSignal: [self.handle_signal]},
        )
        super().__init__(manager)

    def handle_signal(self):
        self.success = True


class KnownTaskSentToMain(LeadsToSuccessFlag):
    task = ExampleTask()

    def process_trigger(self):
        self.manager.send(self.task)

    def __init__(self):
        manager = ipc.CommunicationManager(
            main_messages={ExampleTask: [self.handle_task]},
        )
        super().__init__(manager)

    def handle_task(self, task):
        self.success = True


class KnownSignalSentToMain(LeadsToSuccessFlag):
    def process_trigger(self):
        self.manager.send(ExampleSignal)

    def __init__(self):
        manager = ipc.CommunicationManager(
            main_signals={ExampleSignal: [self.handle_sig]},
        )
        super().__init__(manager)

    def handle_sig(self):
        self.success = True


class ExceptionInWorkerSignalHandler(LeadsToError):
    exc = mpc.Exception("testing")
    expected = mpc.WorkerRuntimeError(exc)

    def local_trigger(self):
        self.manager.send(ExampleSignal)

    def __init__(self):
        manager = ipc.CommunicationManager(
            worker_signals={ExampleSignal: [self.raises_error]},
        )
        manager._signal_thread = None
        super().__init__(manager)

    def raises_error(self):
        raise self.exc


class ExceptionInMainTaskHandler(LeadsToError):
    exc = mpc.Exception("testing")
    expected = mpc.WorkerRuntimeError(exc)

    def process_trigger(self):
        self.manager.send(ExampleTask())

    def __init__(self):
        manager = ipc.CommunicationManager(
            main_messages={ExampleTask: [self.raises_error]},
        )
        super().__init__(manager)

    def raises_error(self, _):
        raise self.exc


class ExceptionInMainSignalHandler(LeadsToError):
    exc = mpc.Exception("testing")
    expected = mpc.WorkerRuntimeError(exc)

    def process_trigger(self):
        self.manager.send(ExampleSignal)

    def __init__(self):
        manager = ipc.CommunicationManager(
            main_signals={ExampleSignal: [self.raises_error]},
        )
        super().__init__(manager)

    def raises_error(self):
        raise self.exc


class TasksHandledInOrder(LeadsToSuccessFlag):
    seen = []
    expected = [ExampleTask(n) for n in range(3)]

    def local_trigger(self):
        self.manager.send(self.expected[0])
        self.manager.send(self.expected[1])
        self.manager.send(self.expected[2])

    def __init__(self):
        manager = ipc.CommunicationManager(
            worker_messages={ExampleTask: [self.handle]},
        )
        super().__init__(manager)

    def handle(self, task):
        self.seen.append(task)

    def check_process_state(self):
        while self.manager.taskq:
            handler = self.manager.taskq.popleft()
            handler()
        if self.seen == self.expected:
            self.success = True


class SignalWithMultipleHandlers(LeadsToSuccessFlag):
    def local_trigger(self):
        self.manager.send(ExampleSignal)

    def __init__(self):
        manager = ipc.CommunicationManager(
            worker_signals={ExampleSignal: [self.handler1, self.handler2]}
        )
        self.n = 0
        super().__init__(manager)

    def handler1(self):
        self.n += 1
        if self.n == 2:
            self.success = True

    def handler2(self):
        self.n += 1
        if self.n == 2:
            self.success = True


class TaskWithMultipleHandlers(LeadsToSuccessFlag):
    def process_trigger(self):
        self.manager.send(ExampleTask())

    def __init__(self):
        manager = ipc.CommunicationManager(
            main_messages={ExampleTask: [self.handler1, self.handler2]}
        )
        self.n = 0
        super().__init__(manager)

    def handler1(self, _):
        self.n += 1
        if self.n == 2:
            self.success = True

    def handler2(self, _):
        self.n += 1
        if self.n == 2:
            self.success = True


@pytest.mark.parametrize("case", ErrorImmediately.__subclasses__())
@pytest.mark.parametrize("auto", (True, False))
def test_error_immediately_implementation(case, auto):
    p = case()
    p.start()
    p.manager.start(auto=auto)

    with pytest.raises(type(p.expected)) as excinfo:
        p.trigger()

    assert excinfo.value == p.expected


@pytest.mark.parametrize("case", LeadsToError.__subclasses__())
@pytest.mark.parametrize("auto", (True, False))
def test_leads_to_error_implementation(case, auto):
    p = case()
    p.start()
    p.manager.start(auto=auto)

    if not auto:
        p.trigger()

        @exception_soon_repeat(p.expected)
        def cause():
            p.manager.recv()

    else:

        @exception_soon(p.expected)
        def cause():
            p.trigger()


@pytest.mark.parametrize("case", LeadsToSuccessFlag.__subclasses__())
@pytest.mark.parametrize("auto", (True, False))
def test_leads_to_success_implementation(case, auto):
    p = case()
    p.start()
    p.manager.start(auto=auto)

    p.trigger()

    @happens_soon
    def success_flag_enabled():
        if not auto:
            p.manager.recv()
        assert p.success
