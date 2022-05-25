import pytest

from .conftest import (
    happens_soon,
    VERY_FAST_TIMEOUT,
    FAST_TIMEOUT,
    doesnt_happen,
)

import mpcontroller as mpc
import multiprocessing as mp


class ScheduledWorker(mpc.Worker):
    def __init__(self):
        self.called = mp.Value("i", 0)
        super().__init__()


class FastScheduleInMain(ScheduledWorker):
    @mpc.schedule.main(VERY_FAST_TIMEOUT / 100)
    def record(self):
        with self.called.get_lock():
            self.called.value += 1


class SlowScheduleInMain(ScheduledWorker):
    @mpc.schedule.main(10 * FAST_TIMEOUT)
    def record(self):
        with self.called.get_lock():
            self.called.value += 1


class FastScheduleInWorker(ScheduledWorker):
    @mpc.schedule.worker(VERY_FAST_TIMEOUT / 100)
    def record(self):
        with self.called.get_lock():
            self.called.value += 1


class SlowScheduleInWorker(ScheduledWorker):
    @mpc.schedule.worker(10 * FAST_TIMEOUT)
    def record(self):
        with self.called.get_lock():
            self.called.value += 1


@pytest.mark.parametrize("auto", (True, False))
@pytest.mark.parametrize(
    "worker_type", (FastScheduleInMain, FastScheduleInWorker)
)
def test_scheduled_function_should_be_called_multiple_times(worker_type, auto):
    worker = worker_type.spawn(auto=auto)

    @happens_soon
    def scheduled_task_is_executed_multiple_times():
        if not auto:
            worker.recv()
        assert worker.called.value >= 2


@pytest.mark.parametrize("auto", (True, False))
@pytest.mark.parametrize(
    "worker_type", (SlowScheduleInMain, SlowScheduleInWorker)
)
def test_scheduled_function_should_be_called_only_once(worker_type, auto):
    worker = worker_type.spawn(auto=auto)

    @doesnt_happen
    def scheduled_task_is_executed_multiple_times():
        if not auto:
            worker.recv()
        assert worker.called.value >= 2
