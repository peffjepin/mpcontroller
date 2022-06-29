import time
import typing
import os
import collections

import pytest

import mpcontroller as mpc

from mpcontroller import ipc
from mpcontroller import config


FAST_TIMEOUT = 3
if os.environ.get("CI", None):
    FAST_TIMEOUT = 15

VERY_FAST_TIMEOUT = FAST_TIMEOUT / 100
FAST_POLL = FAST_TIMEOUT / 10_000
config.local_context.poll_interval = FAST_POLL


def dummy_fn(*args, **kwargs):
    pass


def _raise(exc):
    raise exc


ipc.MainThreadInterruption.handler = dummy_fn


class _MainThreadInterruptionCapture:
    def __init__(self):
        self.exception = None

    def begin_capture(self):
        ipc.MainThreadInterruption.handler = _raise

    def end_capture(self):
        ipc.MainThreadInterruption.handler = dummy_fn

    def capture_exception(self, exc):
        self.exception = exc


@pytest.fixture(autouse=True, scope="function")
def _testing_environment():
    exccap = _MainThreadInterruptionCapture()
    exccap.begin_capture()
    yield
    exccap.end_capture()
    CommunicationManager.cleanup()
    mpc.kill_all()


class ExampleTask(mpc.Task):
    content: typing.Any = "testing"


class ExampleEvent(mpc.Event):
    content: typing.Any = "testing"


class ExampleSignal(mpc.Signal):
    pass


example_task = ExampleTask("testing")
example_event = ExampleEvent("testing")


class CommunicationManager(ipc.CommunicationManager):
    _created = []

    def __init__(self, *args, **kwds):
        CommunicationManager._created.append(self)
        super().__init__(*args, **kwds)

    @classmethod
    def cleanup(cls):
        for cm in cls._created:
            cm.kill()
        cls._created.clear()


class Worker(mpc.Worker):
    def join(self, timeout=None):
        super().join(timeout or FAST_TIMEOUT)


class BlankWorker(Worker):
    pass


@pytest.fixture(autouse=True, scope="session")
def _patch_test_environment():
    mpc.Worker = Worker
    ipc.CommunicationManager = CommunicationManager


def _succeeds_before_timeout(fn, timeout):
    deadline = time.time() + timeout

    while True:
        try:
            fn()
        except mpc.WorkerRuntimeError as exc:
            raise exc
        except Exception as exc:
            if time.time() < deadline:
                continue
            else:
                raise exc
        else:
            break


def _doesnt_succeed_before_timeout(fn, timeout):
    deadline = time.time() + timeout

    while True:
        try:
            fn()
        except mpc.WorkerRuntimeError as exc:
            raise exc
        except Exception:
            if time.time() < deadline:
                continue
            else:
                return
        else:
            raise AssertionError(f"{fn!r} succeeded")


def happens_before(timeout):
    def wrap(fn):
        _succeeds_before_timeout(fn, timeout)

    return wrap


def happens_soon(fn):
    _succeeds_before_timeout(fn, FAST_TIMEOUT)


def doesnt_happen(fn):
    _doesnt_succeed_before_timeout(fn, VERY_FAST_TIMEOUT)


def exception_soon(expected_exception):
    def inner(fn):
        deadline = time.time() + FAST_TIMEOUT
        try:
            fn()
            while time.time() < deadline:
                pass
            raise TimeoutError(f"{expected_exception!r} never raised")
        except Exception as exc:
            if exc == expected_exception:
                return
            else:
                print("Got unexpected exception")
                raise exc

    return inner


def exception_soon_repeat(expected_exception):
    def inner(fn, deadline=None):
        deadline = deadline or time.time() + FAST_TIMEOUT
        if time.time() > deadline:
            raise TimeoutError(f"{expected_exception!r} never raised")
        try:
            fn()
        except Exception as exc:
            if exc == expected_exception:
                return
            else:
                print("Got unexpected exception")
                raise exc
        else:
            inner(fn, deadline)

    return inner


class RecordedCallback:
    def __init__(self):
        self._n = -1
        self._args = collections.deque()
        self._kwargs = collections.deque()

    def __call__(self, *args, **kwargs):
        self._args.append(args)
        self._kwargs.append(kwargs)

    def nth(self, value):
        self._n = value
        return self

    @property
    def called(self):
        return len(self._args)

    @property
    def args(self):
        return self._args[self._n]

    @property
    def kwargs(self):
        return self._kwargs[self._n]

    def assert_called_with(self, *args, **kwargs):
        assert self.args == args and self.kwargs == kwargs


@pytest.fixture
def recorded_callback():
    return RecordedCallback()


example_exception = mpc.Exception("testing")
