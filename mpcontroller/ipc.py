import _thread
import time
import signal
import traceback
import threading
import collections
import multiprocessing as mp

from collections import defaultdict

from . import exceptions
from . import global_state


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

    @classmethod
    def interrupt_main(cls, exc):
        cls.exception = exc
        _thread.interrupt_main()


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
        with self._shared.get_lock():
            n_activations = self._shared.value
            if n_activations > 0:
                self._shared.value = 0

        for _ in range(n_activations):
            for handler in self._callbacks:
                handler()

    @property
    def is_active(self):
        return self._shared.value != 0


class Terminate(Signal):
    pass


class CommunicationManager:
    def __init__(
        self,
        worker_tasks=None,
        worker_signals=None,
        main_tasks=None,
        main_signals=None,
    ):
        self._worker_tasks = worker_tasks or dict()
        self._main_tasks = main_tasks or dict()
        self._worker_signals = self._init_signals(worker_signals or dict())
        self._main_signals = self._init_signals(main_signals or dict())
        self._auto = False
        self._in_child_process = False
        self._main_conn, self._worker_conn = mp.Pipe()
        self._pipe_thread = None
        self._signal_thread = None

    def start(self, *, auto=False):
        self._auto = auto
        self.taskq = collections.deque()
        self._in_child_process = mp.current_process().name != "MainProcess"

        if self._in_child_process:
            MainThreadInterruption.handler = self._worker_interrupt_handler
        if auto:
            self._start_workthreads()

    def join(self, timeout=None):
        if self._auto:
            self._pipe_thread.join(timeout)
            self._signal_thread.join(timeout)

    def kill(self):
        if self._auto:
            self._pipe_thread.kill()
            self._signal_thread.kill()

    def recv(self):
        if self._auto:
            return
        else:
            _process_signals(
                self._local_signals.values(), self._on_runtime_exception
            )
            _process_messages(
                self._local_conn,
                self._on_task_recieved,
                self._on_exception_recieved,
            )

    def send(self, msg):
        if _is_signal(msg):
            self._send_signal(msg)
        elif isinstance(msg, Exception):
            if not self._in_child_process:
                raise msg
            self._local_conn.send(msg)
        else:
            self._send_task(msg)

    def _init_signals(self, callback_lookup):
        initialized = {
            sigtype: sigtype(callbacks)
            for sigtype, callbacks in callback_lookup.items()
        }
        return initialized

    def _send_task(self, task):
        if type(task) in self._foreign_tasks:
            self._local_conn.send(task)
        else:
            self._handle_unknown_message(task)

    def _handle_unknown_message(self, msg):
        recipient = (
            "MainProcess" if self._in_child_process else "WorkerProcess"
        )
        exception = exceptions.UnknownMessageError(msg, recipient)

        if self._in_child_process:
            self.send(exception)

        raise exception

    def _send_signal(self, signal_type):
        sigobj = self._foreign_signals.get(signal_type, None)

        if not sigobj:
            return self._handle_unknown_message(signal_type)

        sigobj.activate()

    def _start_workthreads(self):
        self._pipe_thread = ConnectionPollingThread(
            self._local_conn,
            self._on_task_recieved,
            self._on_exception_recieved,
        )
        self._signal_thread = SignalProcessingThread(
            self._local_signals.values(), self._on_runtime_exception
        )
        self._pipe_thread.start()
        self._signal_thread.start()

    def _worker_interrupt_handler(self, exc):
        # this is executed in the child process when the main thread is
        # interrupted from a daemon thread.

        exception = exceptions.WorkerRuntimeError(exc, traceback.format_exc())
        self.send(exception)
        raise exc

    @property
    def _on_task_recieved(self):
        # TODO: Multiple handlers
        def main_implementation(task):
            handlers = self._local_tasks[type(task)]
            _complete_task(task, handlers, self._on_runtime_exception)

        def worker_implementation(task):
            self.taskq.append(task)

        return (
            worker_implementation
            if self._in_child_process
            else main_implementation
        )

    @property
    def _on_exception_recieved(self):
        def main_implementation(exc):
            if self._auto:
                MainThreadInterruption.interrupt_main(exc)
            else:
                raise exc

        def worker_implementation(exc):
            MainThreadInterruption.interrupt_main(exc)

        return (
            worker_implementation
            if self._in_child_process
            else main_implementation
        )

    @property
    def _on_runtime_exception(self):
        def main_implementation(exc):
            exception = exceptions.WorkerRuntimeError(
                exc, traceback.format_exc()
            )
            if self._auto:
                MainThreadInterruption.interrupt_main(exception)
            else:
                raise exception

        def worker_implementation(exc):
            MainThreadInterruption.interrupt_main(exc)

        return (
            worker_implementation
            if self._in_child_process
            else main_implementation
        )

    @property
    def _local_conn(self):
        return self._worker_conn if self._in_child_process else self._main_conn

    @property
    def _local_tasks(self):
        return (
            self._worker_tasks if self._in_child_process else self._main_tasks
        )

    @property
    def _local_signals(self):
        return (
            self._worker_signals
            if self._in_child_process
            else self._main_signals
        )

    @property
    def _foreign_tasks(self):
        return (
            self._main_tasks if self._in_child_process else self._worker_tasks
        )

    @property
    def _foreign_signals(self):
        return (
            self._main_signals
            if self._in_child_process
            else self._worker_signals
        )


class MainloopThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        self._running = False
        super().__init__(*args, target=self._main, daemon=True, **kwargs)

    def _main(self):
        self._running = True
        while self._running:
            self.mainloop()

    def join(self, timeout=None):
        self._running = False
        super().join(timeout)

    def kill(self):
        self._running = False

    def mainloop(self):
        raise NotImplementedError()


class SignalProcessingThread(MainloopThread):
    def __init__(self, signals_to_watch, on_exception):
        self._signals = signals_to_watch
        self._on_exception = on_exception
        super().__init__()

    def mainloop(self):
        _process_signals(self._signals, self._on_exception)
        time.sleep(global_state.config.poll_interval)


class ConnectionPollingThread(MainloopThread):
    def __init__(self, conn, on_message, on_exception):
        self._conn = conn
        self._on_message = on_message
        self._on_exception = on_exception
        super().__init__()

    def mainloop(self):
        try:
            self._process_messages()
            time.sleep(global_state.config.poll_interval)
        except Exception as exc:
            MainThreadInterruption.interrupt_main(exc)
        finally:
            self._safe_flush_pipe()

    def _process_messages(self):
        try:
            _process_messages(self._conn, self._on_message, self._on_exception)
        except EOFError:
            if self._running:
                raise
        except BrokenPipeError:
            if self._running:
                raise

    def _safe_flush_pipe(self):
        try:
            self._process_messages()
        except Exception:
            pass


class MethodMarker:
    _registry = defaultdict(lambda: defaultdict(list))

    def __init_subclass__(cls):
        cls._registry = defaultdict(lambda: defaultdict(list))

    def __init__(self, key):
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


def _is_signal(msg):
    try:
        return issubclass(msg, Signal)
    except TypeError:
        return False


def _process_messages(conn, on_task, on_exception):
    while conn.poll(0):
        msg = conn.recv()
        if isinstance(msg, Exception):
            on_exception(msg)
        else:
            on_task(msg)


def _process_signals(signals, on_exception):
    try:
        for sig in signals:
            sig.handle()
    except Exception as exc:
        on_exception(exc)


def _complete_task(task, handlers, on_exception):
    try:
        for handler in handlers:
            handler(task)
    except Exception as exc:
        on_exception(exc)
