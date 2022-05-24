import multiprocessing as mp
import time

import pytest

from .conftest import happens_soon
from .conftest import exception_soon

import mpcontroller as mpc


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
def test_worker_runtime_errors(worker_under_test):
    worker = worker_under_test()

    @exception_soon(worker.expected)
    def cause():
        worker.start()

        if worker.requires_join:
            worker.join()
