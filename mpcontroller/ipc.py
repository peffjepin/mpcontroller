import _thread
import time
import signal
import threading
import traceback
import sys


def _mainthread_exception_handler(exc):
    raise exc


def _handle_interrupt(*args):
    if PipeReader.exception is not None:
        _mainthread_exception_handler(PipeReader.exception)
    else:
        print(traceback.format_exc(), file=sys.stderr)


def set_exception_handler(fn):
    global _mainthread_exception_handler
    _mainthread_exception_handler = fn


signal.signal(signal.SIGINT, _handle_interrupt)


def _id_generator():
    id = 0
    while True:
        yield id


class Signal:
    _idgen = _id_generator()

    def __init__(self, *, _id=None):
        self._id = _id or next(self._idgen)

    def __eq__(self, other):
        return isinstance(other, Signal) and other.id == self._id

    def __hash__(self):
        return hash(self._id)

    def __reduce__(self):
        return (Signal, (), {"_id": self._id})

    @property
    def id(self):
        return self._id


SHUTDOWN = Signal()


class PipeReader:
    POLL_RATE = 0.05

    exception = None

    def __init__(self, conn, message_handler=None, *, exception_handler=None):
        self._exc_cb = exception_handler or self._defaultexception_handler
        self._msg_cb = message_handler
        self._conn = conn
        self._running = True
        self._thread = None

    def start(self):
        self._running = True
        self._thread = threading.Thread(target=self._mainloop, daemon=True)
        self._thread.start()

    def join(self, timeout=5):
        if self._thread and self._thread.is_alive():
            self._running = False
            self._thread.join(timeout)
            self._thread = None

    def kill(self):
        # let the thread die on it's own time
        self._running = False
        self._thread = False

    def _defaultexception_handler(self, exc):
        PipeReader.exception = exc
        _thread.interrupt_main()

    def _mainloop(self):
        while self._running:
            while self._conn.poll(0):
                msg = self._conn.recv()
                if isinstance(msg, Exception):
                    self._exc_cb(msg)
                else:
                    self._msg_cb(msg)
            time.sleep(self.POLL_RATE)
