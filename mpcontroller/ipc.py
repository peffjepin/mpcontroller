import typing
import time
import threading
import enum


class Signal(enum.Enum):
    HEALTH_CHECK = enum.auto()
    REQUEST_WEB_JOB = enum.auto()
    SHUTDOWN = enum.auto()


class WebJob(typing.NamedTuple):
    url: str
    endpoint: str


class StatusUpdate(typing.NamedTuple):
    status: str


class PipeReader:

    POLL_RATE = 0.05

    def __init__(self, conn, exception_handler, message_handler):
        self._exc_cb = exception_handler
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

    def _mainloop(self):
        while self._running:
            while self._conn.poll(0):
                msg = self._conn.recv()
                if isinstance(msg, Exception):
                    self._exc_cb(msg)
                else:
                    self._msg_cb(msg)
            time.sleep(self.POLL_RATE)
