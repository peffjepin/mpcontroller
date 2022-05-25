import multiprocessing as mp
import time

import pytest

from .conftest import happens_soon
from .conftest import doesnt_happen
from .conftest import exception_soon
from .conftest import exception_soon_repeat
from .conftest import VERY_FAST_TIMEOUT
from .conftest import FAST_TIMEOUT

import mpcontroller as mpc
from mpcontroller import global_state


class TimeTrackingWorker(mpc.Worker):
    def __init__(self):
        self.setuptime = mp.Value("d", -1)
        self.mainlooptime = mp.Value("d", -1)
        self.teardowntime = mp.Value("d", -1)
        super().__init__()

    def setup(self):
        self.setuptime.value = time.time()

    def mainloop(self):
        self.mainlooptime.value = time.time()

    def teardown(self):
        self.teardowntime.value = time.time()


class TimeTrackingWorkerErrorInMainloop(TimeTrackingWorker):
    exc = mpc.Exception("testing")
    expected = mpc.WorkerRuntimeError(exc)

    def mainloop(self):
        super().mainloop()
        raise self.exc


def test_setup_is_called_first():
    worker = TimeTrackingWorker.spawn()

    @happens_soon
    def setup_time_recorded():
        assert worker.setuptime.value > 0


def test_main_executes_in_a_loop_after_setup():
    worker = TimeTrackingWorker.spawn()
    times = []

    @happens_soon
    def multiple_main_invokations_recorded_after_setup():
        if worker.mainlooptime.value > 0:
            times.append(worker.mainlooptime.value)

        assert len(times) > 0
        assert sorted(times) == times
        assert times[-1] >= times[0]
        assert worker.setuptime.value <= times[0]


def test_teardown_executes_before_exit():
    worker = TimeTrackingWorker.spawn()

    @happens_soon
    def process_begins_execution():
        assert worker.mainlooptime.value > 0

    worker.join()
    assert worker.teardowntime.value > 0


def test_teardown_still_executes_after_an_error_occurs():
    worker = TimeTrackingWorkerErrorInMainloop()

    @exception_soon(TimeTrackingWorkerErrorInMainloop.expected)
    def cause():
        worker.start()

    worker.join()
    assert worker.teardowntime.value > 0


class GlobalStateTestCase(mpc.Worker):
    expected = 1 / 1000

    def __init__(self):
        self.value = mp.Value("d", 0)
        super().__init__()

    def setup(self):
        self.value.value = global_state.config.poll_interval


def test_global_state_transfered_to_worker():
    original = global_state.config.poll_interval
    global_state.config.poll_interval = GlobalStateTestCase.expected

    try:
        worker = GlobalStateTestCase.spawn()

        @happens_soon
        def value_is_set():
            worker.value.value == GlobalStateTestCase.expected

    finally:
        global_state.config.poll_interval = original


class LeadsToErrorTestCase(mpc.Worker):
    exc = NotImplemented
    expected = NotImplemented
    requires_join = False


class ErrorInMainloop(LeadsToErrorTestCase):
    exc = mpc.Exception("mainloop")
    expected = mpc.WorkerRuntimeError(exc)

    def mainloop(self):
        raise self.exc


class ErrorInSetup(LeadsToErrorTestCase):
    exc = mpc.Exception("setup")
    expected = mpc.WorkerRuntimeError(exc)

    def setup(self):
        raise self.exc


class ErrorInTeardown(LeadsToErrorTestCase):
    exc = mpc.Exception("teardown")
    expected = mpc.WorkerRuntimeError(exc)
    requires_join = True

    def teardown(self):
        raise self.exc


class ErrorInTeardownAfterErrorInMainloop(LeadsToErrorTestCase):
    exc = mpc.Exception("mainloop")
    expected = mpc.WorkerRuntimeError(exc)

    def mainloop(self):
        raise self.exc

    def teardown(self):
        raise mpc.Exception("teardown")


@pytest.mark.parametrize(
    "worker_under_test", LeadsToErrorTestCase.__subclasses__()
)
def test_worker_runtime_errors_automatic_communication(worker_under_test):
    worker = worker_under_test()

    @exception_soon(worker.expected)
    def cause():
        worker.start()

        if worker.requires_join:
            worker.join()


@pytest.mark.parametrize(
    "worker_under_test", LeadsToErrorTestCase.__subclasses__()
)
def test_worker_runtime_errors_manual_communication(worker_under_test):
    worker = worker_under_test()

    worker.start(auto=False)

    @exception_soon_repeat(worker.expected)
    def exception_occurs():
        if worker.requires_join:
            worker.join()
        else:
            worker.recv()


@pytest.mark.parametrize("auto", (True, False))
def test_scheduled_function_should_be_called_multiple_times(auto):
    class ScheduleWorker(mpc.Worker):
        called = 0

        @mpc.schedule.main(VERY_FAST_TIMEOUT / 100)
        def should_be_called_multiple_times(self):
            self.called += 1

    worker = ScheduleWorker.spawn(auto=auto)

    @happens_soon
    def scheduled_task_is_executed_multiple_times():
        if not auto:
            worker.recv()
        assert worker.called >= 2


@pytest.mark.parametrize("auto", (True, False))
def test_scheduled_function_should_be_called_only_once(auto):
    class ScheduleWorker(mpc.Worker):
        called = 0

        @mpc.schedule.main(10 * FAST_TIMEOUT)
        def should_be_called_multiple_times(self):
            self.called += 1

    worker = ScheduleWorker.spawn(auto=auto)

    @doesnt_happen
    def scheduled_task_is_executed_multiple_times():
        if not auto:
            worker.recv()
        assert worker.called >= 2
