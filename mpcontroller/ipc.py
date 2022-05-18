import _thread
import time
import signal
import threading
import multiprocessing as mp

from collections import defaultdict


class MainThreadInterruption:
    # set handler to try to handle exceptions rather than raise them
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
    def __init__(self, callbacks):
        assert type(self) != Signal, "Signal should be subclassed"
        self._shared = mp.Value("i", 0)
        self._shared.value = 0
        self._callbacks = callbacks

    def activate(self):
        with self._shared.get_lock():
            self._shared.value += 1

    def handle(self):
        was_set = False

        with self._shared.get_lock():
            current = self._shared.value
            if current > 0:
                was_set = True
                self._shared.value = current - 1

        if was_set:
            for callback in self._callbacks:
                callback()

    @property
    def is_active(self):
        return self._shared.value != 0


class Terminate(Signal):
    pass


class CommunicationManager:
    POLL_INTERVAL = 0.05

    def __init__(self, conn, signals, message_handler=None, *, exception_handler=None):
        self.running = False

        self._exc_cb = exception_handler or self._default_exception_handler
        self._msg_cb = message_handler
        self._conn = conn
        self._signals = signals
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
        except BrokenPipeError:
            if not self.running:
                return
            raise

    def _handle_signals(self):
        for sig in self._signals.values():
            sig.handle()

    def _mainloop(self):
        try:
            while self.running:
                self._process_messages()
                self._handle_signals()
                time.sleep(self.POLL_INTERVAL)
        except EOFError as exc:
            if not self.running:
                return
            self._exc_cb(exc)
        except BrokenPipeError as exc:
            if not self.running:
                return
            self._exc_cb(exc)
        except Exception as exc:
            self._exc_cb(exc)


class MethodMarker:
    key_function = None

    _registry = defaultdict(lambda: defaultdict(list))

    def __init_subclass__(cls):
        cls._registry = defaultdict(lambda: defaultdict(list))

    def __init__(self, key):
        if self.key_function:
            self._key = self.key_function(key)
        else:
            self._key = key

    def __call__(self, fn):
        self._fn = fn
        return self

    def __set_name__(self, cls, name):
        setattr(cls, name, self._fn)
        self._register(name, cls)

    def _register(self, name, cls):
        self._registry[cls][self._key].append(name)

    @classmethod
    def get_registered_keys(cls, type):
        return cls._registry[type].keys()

    @classmethod
    def make_callback_table(cls, object):
        classes_to_check = type(object).__mro__[:-1]
        callbacks_seen = set()

        table = defaultdict(list)

        for c in classes_to_check:
            for key, names in cls._registry[c].items():
                for name in names:
                    if name in callbacks_seen:
                        continue
                    callbacks_seen.add(name)
                    bound_method = getattr(object, name)
                    table[key].append(bound_method)

        return table


class SignalMarker(MethodMarker):
    @classmethod
    def make_signals(cls, obj):
        return {
            sig_type: sig_type(callbacks)
            for sig_type, callbacks in cls.make_callback_table(obj).items()
        }


def message_handler(msg_type):
    return MethodMarker(msg_type)


def signal_handler(sig_type):
    return SignalMarker(sig_type)
