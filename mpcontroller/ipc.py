import _thread
import time
import signal
import threading

from collections import defaultdict


def _mainthread_exception_handler(exc):
    raise exc


def _handle_interrupt(*args):
    if PipeReader.exception is not None:
        _mainthread_exception_handler(PipeReader.exception)
    else:
        raise KeyboardInterrupt()


def set_exception_handler(fn):
    global _mainthread_exception_handler
    _mainthread_exception_handler = fn


signal.signal(signal.SIGINT, _handle_interrupt)


class Signal:
    SET = 1
    CLEAR = 0

    def __init__(self, shared_int):
        assert type(self) != Signal, "Signal should be subclassed"
        self._shared = shared_int
        self._shared.value = Signal.CLEAR

    def set(self, value=None):
        self._shared.value = value or Signal.SET

    def clear(self):
        self._shared.value = Signal.CLEAR

    @property
    def is_set(self):
        return self._shared.value != Signal.CLEAR


class PipeReader:
    POLL_INTERVAL = 0.05

    exception = None

    def __init__(self, conn, message_handler=None, *, exception_handler=None):
        self._exc_cb = exception_handler or self._default_exception_handler
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
        self._exc_cb = self._dead_exception_handler
        self._running = False
        self._thread = False

    def _default_exception_handler(self, exc):
        PipeReader.exception = exc
        _thread.interrupt_main()

    def _dead_exception_handler(self, *args):
        pass

    def _mainloop(self):
        try:
            while self._running:
                while self._conn.poll(0):
                    msg = self._conn.recv()
                    if isinstance(msg, Exception):
                        self._exc_cb(msg)
                    else:
                        self._msg_cb(msg)
                time.sleep(self.POLL_INTERVAL)
        except Exception as exc:
            self._exc_cb(exc)


class IpcHandler:
    _message_registry = defaultdict(lambda: defaultdict(list))
    _signal_registry = defaultdict(lambda: defaultdict(list))

    def __init__(self, key, *, _issignal=False):
        self._issignal = _issignal
        self._key = key

    def __call__(self, fn):
        self._fn = fn
        return self

    def __set_name__(self, cls, name):
        setattr(cls, name, self._fn)
        self._register(name, cls)

    def _register(self, name, cls):
        if self._issignal:
            registry = self._signal_registry
        else:
            registry = self._message_registry
        registry[cls][self._key].append(name)

    @classmethod
    def get_message_callback_table(cls, object):
        return cls._make_callback_table(object, cls._message_registry)

    @classmethod
    def get_signal_callback_table(cls, object):
        return cls._make_callback_table(object, cls._signal_registry)

    @classmethod
    def _make_callback_table(cls, object, registry):
        table = defaultdict(list)
        for key, names in registry[type(object)].items():
            for name in names:
                bound_method = getattr(object, name)
                table[key].append(bound_method)
        return table


def message_handler(msg_type):
    return IpcHandler(msg_type)


def signal_handler(sig_type):
    return IpcHandler(sig_type, _issignal=True)
