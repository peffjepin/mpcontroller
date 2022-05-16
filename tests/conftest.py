import time
import typing
import os
import collections

import pytest

import mpcontroller as mpc

from mpcontroller import worker
from mpcontroller import ipc


FAST_TIMEOUT = 0.5
if os.environ.get("CI", None):
    FAST_TIMEOUT = 30

VERY_FAST_TIMEOUT = FAST_TIMEOUT / 10
FAST_POLL = FAST_TIMEOUT / 10_000

_processes = list()
_pipe_readers = list()


class ExampleMessage(typing.NamedTuple):
    content: str


example_message = ExampleMessage("testing")
example_signal = mpc.Signal()


class PipeReader(mpc.PipeReader):
    POLL_RATE = FAST_POLL

    def __init__(self, *args, **kwds):
        _pipe_readers.append(self)
        super().__init__(*args, **kwds)


class Worker(mpc.Worker):
    POLL_RATE = FAST_POLL

    def __init__(self, *args, **kwds):
        _processes.append(self)
        super().__init__(*args, **kwds)


class BlankWorker(Worker):
    pass


class Controller(mpc.Controller):
    POLL_RATE = FAST_POLL


class RecordedController(Controller):
    def __init__(self, *args, **kwargs):
        self.msg_cb = RecordedCallback()
        super().__init__(*args, **kwargs)

    @mpc.message_handler(ExampleMessage)
    def handler(self, msg):
        self.msg_cb(msg)


@pytest.fixture(autouse=True, scope="session")
def _patch_test_environment():
    mpc.Controller = Controller
    mpc.Worker = Worker
    mpc.PipeReader = PipeReader

    worker.Controller = Controller
    worker.Worker = Worker
    ipc.PipeReader = PipeReader


@pytest.fixture(autouse=True, scope="function")
def _auto_cleanup_long_running_resources():
    yield
    _kill_processes_and_threads()


def _kill_processes_and_threads():
    while _processes:
        try:
            _processes.pop().kill()
        except Exception:
            pass
    while _pipe_readers:
        try:
            _pipe_readers.pop().kill()
        except Exception:
            pass


def _succeeds_before_timeout(fn, timeout):
    deadline = time.time() + timeout

    while True:
        try:
            fn()
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
        except Exception:
            if time.time() < deadline:
                continue
            else:
                break
        else:
            raise AssertionError("it happened")


def happens_before(timeout):
    def wrap(fn):
        _succeeds_before_timeout(fn, timeout)

    return wrap


def happens_soon(fn):
    _succeeds_before_timeout(fn, FAST_TIMEOUT)


def doesnt_happen(fn):
    _doesnt_succeed_before_timeout(fn, VERY_FAST_TIMEOUT)


def exception_soon(expected_exception):
    # expected_exception will be compared with __eq__
    def inner(fn):
        try:
            fn()
            time.sleep(FAST_TIMEOUT)
        except Exception as exc:
            if exc == expected_exception:
                return
            else:
                raise 
        else:
            raise AssertionError(f"didn't catch {expected_exception!r}")

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
        return len(self.args)

    @property
    def args(self):
        return self._args[self._n]

    @property
    def kwargs(self):
        return self._kwargs[self._n]

    def called_with(self, *args, **kwargs):
        return self.args == args and self.kwargs == kwargs


@pytest.fixture
def recorded_callback():
    return RecordedCallback()


class EqualityException(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(msg)

    def __eq__(self, other):
        return isinstance(other, EqualityException) and self.msg == other.msg

    def __reduce__(self):
        return (EqualityException, (self.msg,))


example_exception = EqualityException("testing")
