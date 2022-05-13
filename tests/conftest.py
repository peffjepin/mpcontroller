import time
import os
import collections

import pytest

import mpcontroller as mpc

from mpcontroller import worker
from mpcontroller import ipc


try:
    FAST_TIMEOUT = float(os.environ["MPC_FAST_TIMEOUT"])
except Exception:
    FAST_TIMEOUT = 0.5

VERY_FAST_TIMEOUT = FAST_TIMEOUT / 10
FAST_POLL = FAST_TIMEOUT / 10_000

_processes = list()
_pipe_readers = list()


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


class Controller(mpc.Controller):
    POLL_RATE = FAST_POLL


@pytest.fixture(autouse=True, scope="session")
def _patch_test_environment():
    mpc.Controller = Controller
    mpc.Worker = Worker
    mpc.PipeReader = PipeReader

    worker.Controller = Controller
    worker.Worker = Worker
    ipc.PipeReader = PipeReader


@pytest.fixture(autouse=True, scope="function")
def _kill_remaining_resources():
    yield
    while _processes:
        try:
            _processes.pop().kill()
        except Exception:
            pass
    while _pipe_readers:
        try:
            _pipe_readers.pop().join(1)
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
    def arg(self):
        nargs = len(self.args)
        nkwargs = len(self.kwargs)
        if nargs + nkwargs > 1:
            raise ValueError("total args given > 1.. which arg is ambiguous")
        if nargs:
            return self.args[0]
        elif nkwargs:
            return next(self.kwargs.values())

    @property
    def kwargs(self):
        return self._kwargs[self._n]


@pytest.fixture
def recorded_callback():
    return RecordedCallback()


class EqualityException(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(msg)

    def __eq__(self, other):
        return isinstance(other, EqualityException) and self.msg == other.msg


@pytest.fixture
def exception():
    return EqualityException("testing exception")
