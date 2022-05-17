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

_pipe_readers = list()


class _MainThreadInterruption:
    _exception = None
    expecting = False

    @classmethod
    def handler(cls, exc):
        # if we're not expecting a mainthread interrupt raise the exception
        if not cls.expecting:
            raise _UnexpectedInterruption(
                f"main thread interrupted unexpectedly:\n {exc!r}"
            )
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
            raise _UnexpectedInterruption(
                "main thread was interrupted again before a previous"
                f"interruption could be handled...\nprev: {cls._exception!r}"
                f"\nnew: {exc!r}"
            )

    @classmethod
    def clear(cls):
        cls._exception = None
        cls.expecting = None


class _UnexpectedInterruption(Exception):
    pass


class ExampleMessage(typing.NamedTuple):
    content: str


class ExampleSignal(mpc.Signal):
    pass


example_message = ExampleMessage("testing")


class PipeReader(ipc.PipeReader):
    POLL_INTERVAL = FAST_POLL

    def __init__(self, *args, **kwds):
        _pipe_readers.append(self)
        super().__init__(*args, **kwds)


class Worker(mpc.Worker):
    POLL_INTERVAL = FAST_POLL


class BlankWorker(Worker):
    pass


class Controller(mpc.Controller):
    POLL_INTERVAL = FAST_POLL

    def join(self, timeout=None):
        super().join(timeout or FAST_TIMEOUT)


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
    ipc.MainThreadInterruption.handler = _MainThreadInterruption.handler


@pytest.fixture(autouse=True, scope="function")
def _per_test_cleanup():
    yield

    _MainThreadInterruption.clear()
    mpc.kill_all()
    _kill_pipe_readers()
    worker._registry.clear()


def _kill_pipe_readers():
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
            if isinstance(exc, _UnexpectedInterruption):
                raise exc
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
        except Exception as exc:
            if isinstance(exc, _UnexpectedInterruption):
                raise exc
            if time.time() < deadline:
                continue
            else:
                break
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
                if exc == expected_exception:
                    return
                elif exc is not None:
                    print(f"expected: {expected_exception!r}")
                    raise exc
            raise AssertionError(f"never caught {expected_exception!r}")
        finally:
            _MainThreadInterruption.clear()

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


class EqualityException(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(msg)

    def __eq__(self, other):
        return isinstance(other, EqualityException) and self.msg == other.msg

    def __reduce__(self):
        return (EqualityException, (self.msg,))


example_exception = EqualityException("testing")
