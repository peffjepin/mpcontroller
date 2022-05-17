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
_interrupting_exception = None
_expected_exception = None


class _DontIgnoreException(Exception):
    pass


def _interrupting_exception_hook(exc):
    global _interrupting_exception

    if _expected_exception is None:
        raise _DontIgnoreException(
            f"a thread or process died unexpectedly\n: {exc!r}"
        )

    _interrupting_exception = exc


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

    def __init__(self, *args, **kwds):
        _processes.append(self)
        super().__init__(*args, **kwds)


class BlankWorker(Worker):
    pass


class Controller(mpc.Controller):
    POLL_INTERVAL = FAST_POLL


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
    ipc.set_thread_exception_handler(_interrupting_exception_hook)


@pytest.fixture(autouse=True, scope="function")
def _per_test_cleanup():
    yield
    _kill_processes_and_threads()

    global _interrupting_exception
    _interrupting_exception = None

    global _expected_exception
    _expected_exception = None

    worker._registry.clear()


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
            if isinstance(exc, _DontIgnoreException):
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
            if isinstance(exc, _DontIgnoreException):
                raise exc
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
        global _expected_exception
        _expected_exception = expected_exception

        try:
            fn()
            deadline = time.time() + FAST_TIMEOUT
            while time.time() < deadline:
                exc = _interrupting_exception
                if exc == expected_exception:
                    return
                elif exc is not None:
                    print(f"expected: {expected_exception!r}")
                    raise _interrupting_exception
            raise AssertionError(f"never caught {expected_exception!r}")
        finally:
            _expected_exception = None

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
