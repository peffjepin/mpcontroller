import time
import typing
import os
import collections

import pytest

import mpcontroller as mpc

from mpcontroller import ipc
from mpcontroller import global_state
from mpcontroller import util


FAST_TIMEOUT = 3
if os.environ.get("CI", None):
    FAST_TIMEOUT = 15

VERY_FAST_TIMEOUT = FAST_TIMEOUT / 100
FAST_POLL = FAST_TIMEOUT / 10_000
global_state.config.poll_interval = FAST_POLL
global_state.clock = util.Clock(FAST_POLL)


class _MainThreadInterruption:
    _exception = None
    expecting = False

    @classmethod
    def handler(cls, exc):
        # if we're not expecting a mainthread interrupt raise the exception
        if not cls.expecting:
            raise exc
        # else just set it as the exception value for use elsewhere
        cls.set_exception(exc)

    @classmethod
    def consume_exception(cls):
        if cls._exception is None:
            return
        else:
            exc = cls._exception
            cls._exception = None
            return exc

    @classmethod
    def set_exception(cls, exc):
        if cls._exception is None:
            cls._exception = exc
        else:
            raise Exception(
                "main thread was interrupted again before a previous"
                f"interruption could be handled...\nprev: {cls._exception!r}"
                f"\nnew: {exc!r}"
            )

    @classmethod
    def clear(cls):
        cls._exception = None
        cls.expecting = None


class ExampleTask(typing.NamedTuple):
    content: typing.Any = "testing"


class ExampleSignal(mpc.Signal):
    pass


example_task = ExampleTask("testing")


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
    ipc.MainThreadInterruption.handler = _MainThreadInterruption.handler


@pytest.fixture(autouse=True, scope="function")
def _per_test_cleanup():
    yield

    _MainThreadInterruption.clear()
    CommunicationManager.cleanup()
    mpc.kill_all()


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
    # expecting an exception to interrupt the main thread from a daemon
    # expected_exception will be compared with __eq__
    def inner(fn):
        try:
            _MainThreadInterruption.expecting = True
            fn()
            deadline = time.time() + FAST_TIMEOUT

            while time.time() < deadline:
                exc = _MainThreadInterruption.consume_exception()
                if exc:
                    # some debug output in case of failure
                    print("caught an exception: ", repr(exc))
                if exc == expected_exception:
                    return
                elif exc is not None:
                    print(f"expected: {expected_exception!r}")
                    raise exc
            raise AssertionError(f"never caught {expected_exception!r}")
        finally:
            _MainThreadInterruption.clear()

    return inner


def exception_soon_repeat(exc):
    def inner(fn):
        # like exception soon but this exception will be raised directly
        # in the main process soon as a result of calling this function
        deadline = time.time() + FAST_TIMEOUT
        while time.time() < deadline:
            try:
                fn()
            except Exception as actual:
                assert exc == actual
                return
        raise AssertionError(f"never caught {exc!r}")

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
