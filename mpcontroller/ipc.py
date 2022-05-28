import _thread
import typing
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


def _sequential_ids():
    id = 0
    while True:
        id += 1
        yield id


_typecode_gen = _sequential_ids()


class Message:
    _class_lookup = dict()

    typecodes: set
    _idgen: typing.Generator

    def __init_subclass__(cls):
        cls.typecodes = set()
        cls._idgen = _sequential_ids()

    def __new__(cls, name="", fields=""):
        typecode = next(_typecode_gen)
        modified_fields = "typecode id " + fields
        modified_name = name or cls.__name__ + f"{typecode}"
        base_factory = collections.namedtuple(modified_name, modified_fields)
        cls._class_lookup[typecode] = base_factory
        cls.typecodes.add(typecode)

        original_new = base_factory.__new__

        def __new__(nt_cls, *args, **kwargs):
            nfields = len(nt_cls._fields)

            # when called with all params just construct normally
            # this happens when deserializing a dumped message
            if len(args) == nfields or len(kwargs) == nfields:
                return original_new(nt_cls, *args, **kwargs)

            # otherwise we have to inject the additional fields that we have
            # added onto the class
            return original_new(
                nt_cls,
                nt_cls._typecode,
                next(cls._idgen),
                *args,
                **kwargs,
            )

        __new__.__doc__ = original_new.__doc__
        __new__.__qualname__ = original_new.__qualname__

        original_repr = base_factory.__repr__

        def __repr__(self):
            return original_repr(self).replace(
                f"typecode={self.typecode}, ", ""
            )

        __repr__.__doc__ = original_repr.__doc__
        __repr__.__qualname__ = original_repr.__qualname__

        base_factory.__new__ = __new__
        base_factory.__repr__ = __repr__
        base_factory._typecode = typecode

        return base_factory

    @classmethod
    def dump(cls, msg, fmt):
        if fmt is tuple:
            return tuple(msg)
        elif fmt is list:
            return list(msg)
        elif fmt is dict:
            return msg._asdict()
        else:
            raise ValueError(
                f"Expected fmt={fmt} to be in (tuple, list, dict)"
            )

    @classmethod
    def load(cls, dump):
        if isinstance(dump, dict):
            namedtup = cls._class_lookup[dump["typecode"]]
            return namedtup(**dump)
        else:
            namedtup = cls._class_lookup[dump[0]]
            return namedtup(*dump)


class Event(Message):
    pass


class Task(Message):
    pass


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
        self._auto = auto
        self._in_child_process = mp.current_process().name != "MainProcess"

        if self._in_child_process:
            self.taskq = collections.deque()
            MainThreadInterruption.handler = self._worker_interrupt_handler
        if auto:
            self._start_communication_thread()

    def join(self, timeout=None):
        if self._auto:
            self._communication_thread.join(timeout)

    def kill(self):
        if self._auto and self._pipe_thread:
            self._pipe_thread.kill()
            self._signal_thread.kill()

    def recv(self):
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

    def _start_communication_thread(self):
        self._communication_thread = CommunicationPollingThread(self)
        self._communication_thread.start()

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


class CommunicationPollingThread(util.MainloopThread):
    def __init__(self, manager):
        self._manager = manager
        super().__init__()

    def mainloop(self):
        self._manager.recv()


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
        while conn.poll(global_state.config.poll_timeout):
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
