import _thread
import time
import signal
import traceback
import collections
import multiprocessing as mp

from . import exceptions
from . import global_state
from . import util


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


class TaskHandler:
    def __init__(self, task, callbacks):
        self._task = task
        self._callbacks = callbacks

    def __call__(self):
        for cb in self._callbacks:
            cb(self._task)


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
        self._auto = True
        self._in_child_process = False
        self._main_conn, self._worker_conn = mp.Pipe()
        self._pipe_thread = None
        self._signal_thread = None

    def start(self, *, auto=True):
        self.taskq = collections.deque()

        self._auto = auto
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
        if self._auto and self._pipe_thread:
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
                self._on_runtime_exception,
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

    def flush_inbound_communication(self):
        def on_runtime_exception(exc):
            if isinstance(exc, EOFError):
                return
            if isinstance(exc, BrokenPipeError):
                return
            self._on_runtime_exception(exc)

        _process_signals(self._local_signals.values(), on_runtime_exception)
        _process_messages(
            self._local_conn,
            self._on_task_recieved,
            self._on_exception_recieved,
            on_runtime_exception,
        )

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
        exception = exceptions.UnknownMessageError(msg)

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
        def main_implementation(task):
            handlers = self._local_tasks[type(task)]
            _complete_task(task, handlers, self._on_runtime_exception)

        def worker_implementation(task):
            handlers = self._local_tasks[type(task)]
            self.taskq.append(TaskHandler(task, handlers))

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
            if isinstance(exc, exceptions.WorkerRuntimeError) or isinstance(
                exc, exceptions.UnknownMessageError
            ):
                exception = exc
            else:
                exception = exceptions.WorkerRuntimeError(exc)
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


class SignalProcessingThread(util.MainloopThread):
    def __init__(self, signals_to_watch, on_exception):
        self._signals = signals_to_watch
        self._on_exception = on_exception
        super().__init__()

    def mainloop(self):
        _process_signals(self._signals, self._on_exception)
        time.sleep(global_state.config.poll_interval)


class ConnectionPollingThread(util.MainloopThread):
    def __init__(self, conn, on_task, on_exception):
        self._conn = conn
        self._on_task = on_task
        self._on_exception = on_exception
        super().__init__()

    def mainloop(self):
        try:
            _process_messages(
                self._conn,
                self._on_task,
                self._on_exception,
                self._on_runtime_exception,
            )
            time.sleep(global_state.config.poll_interval)
        except Exception as exc:
            MainThreadInterruption.interrupt_main(exc)
        finally:
            self._safe_flush_pipe()

    def _on_runtime_exception(self, exc):
        if not self._running:
            if isinstance(exc, EOFError):
                return
            if isinstance(exc, BrokenPipeError):
                return
        raise exc

    def _safe_flush_pipe(self):
        try:
            _process_messages(self._conn, self._on_task, self._on_exception)
        except Exception:
            pass


def _is_signal(msg):
    try:
        return issubclass(msg, Signal)
    except TypeError:
        return False


def _raise(exc):
    raise exc


def _process_messages(
    conn, on_task, on_exception_recieved=_raise, on_runtime_exception=_raise
):
    try:
        while conn.poll(0):
            msg = conn.recv()
            if isinstance(msg, Exception):
                on_exception_recieved(msg)
            else:
                on_task(msg)
    except Exception as exc:
        on_runtime_exception(exc)


def _process_signals(signals, on_runtime_exception=_raise):
    try:
        for sig in signals:
            sig.handle()
    except Exception as exc:
        on_runtime_exception(exc)


def _complete_task(task, handlers, on_runtime_exception=_raise):
    try:
        for handler in handlers:
            handler(task)
    except Exception as exc:
        on_runtime_exception(exc)
