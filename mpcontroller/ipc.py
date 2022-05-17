import _thread
import time
import signal
import threading

from collections import defaultdict


class MainThreadInterruption:
    # set handler to try and handle exceptions rather than raise them
    handler = None
    # exception should be set in daemon threads before interrupting main
    exception = None

    @staticmethod
    def _default_thread_interrupt_exception_handler(exc):
        # handler for the _thread.interrupt_main() call, effectively
        # redirecting the error from daemon thread into the main thread
        raise exc

    @classmethod
    def _handle_interrupt(cls, *args):
        exc = cls._consume_exception()

        # if not exception has been set then this must be a KeyboardInterrupt
        if not exc:
            raise KeyboardInterrupt()

        # otherwise a daemon thread interrupted us in main because of an
        # unhandled exception so we will handle (or raise) it in main
        handler = cls._get_handler()
        handler(exc)

    @classmethod
    def _consume_exception(cls):
        exc = cls.exception
        cls.exception = None
        return exc

    @classmethod
    def _get_handler(cls):
        return cls.handler or cls._default_thread_interrupt_exception_handler


signal.signal(signal.SIGINT, MainThreadInterruption._handle_interrupt)


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

    def __init__(self, conn, message_handler=None, *, exception_handler=None):
        self._exc_cb = exception_handler or self._default_exception_handler
        self._msg_cb = message_handler
        self._conn = conn
        self.running = False
        self._thread = None

    def start(self):
        self.running = True
        self._thread = threading.Thread(target=self._mainloop, daemon=True)
        self._thread.start()

    def join(self, timeout=5):
        if self._thread and self._thread.is_alive():
            self.running = False
            self._thread.join(timeout)
            self._thread = None
            self._process_messages()

    def kill(self):
        # let the thread die on it's own time
        self._exc_cb = self._silenced_exception_handler
        self.running = False
        self._thread = None

    def _default_exception_handler(self, exc):
        MainThreadInterruption.exception = exc
        _thread.interrupt_main()

    def _silenced_exception_handler(self, *args):
        # don't let exceptions bubble up after thread has been "killed"
        pass

    def _process_messages(self):
        try:
            while self._conn.poll(0):
                msg = self._conn.recv()
                if isinstance(msg, Exception):
                    self._exc_cb(msg)
                else:
                    self._msg_cb(msg)
        except EOFError:
            if not self.running:
                return
            raise

    def _mainloop(self):
        try:
            while self.running:
                self._process_messages()
                time.sleep(self.POLL_INTERVAL)
        except EOFError as exc:
            if not self.running:
                return
            self._exc_cb(exc)
        except Exception as exc:
            self._exc_cb(exc)


class CallbackMarker:
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
    return CallbackMarker(msg_type)


def signal_handler(sig_type):
    return CallbackMarker(sig_type, _issignal=True)
