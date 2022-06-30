import _thread
import sys
import typing
import signal
import collections
import multiprocessing as mp

from . import exceptions
from . import config
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


class AnnotatedMessageMeta(type):
    typecodes: set
    namedtuple_lookup = dict()

    def __new__(cls, typename, bases, ns):
        typecode = next(_typecode_gen)
        cls.typecodes.add(typecode)
        annotations = ns.get("__annotations__", dict())
        fields = ["id", "typecode"]
        defaults = []
        for k, _ in annotations.items():
            assert k != "id", "'id' field is reserved"
            assert k != "typecode", "'typecode' field is reserved"
            fields.append(k)
            try:
                default = ns.get(k)
                defaults.append(default)
            except KeyError:
                if defaults:
                    raise ValueError(
                        "Arguments without defaults should be listed "
                        "before those with defaults"
                    )
        namedtuple = collections.namedtuple(
            typename, fields, module=ns.get("__module__"), defaults=defaults
        )
        cls.namedtuple_lookup[typecode] = namedtuple
        cls._patch_namedtuple(namedtuple, typecode)
        return namedtuple

    def __instancecheck__(cls, obj):
        try:
            return isinstance(obj, tuple) and obj.typecode in cls.typecodes
        except AttributeError:
            return False

    @classmethod
    def _patch_namedtuple(cls, namedtuple, typecode):
        original_new = namedtuple.__new__
        original_repr = namedtuple.__repr__

        def __new__(patched_cls, *args, **kwargs):
            nfields = len(patched_cls._fields)
            # when called with all params just construct normally
            # this happens when reconstructing a dumped message
            if len(args) == nfields or len(kwargs) == nfields:
                return original_new(patched_cls, *args, **kwargs)
            # otherwise we have to inject the additional fields that we have
            # added onto the class
            return original_new(
                patched_cls,
                next(patched_cls._idgen),
                typecode,
                *args,
                **kwargs,
            )

        def __repr__(self):
            return original_repr(self).replace(
                f"typecode={self.typecode}, ", ""
            )

        __new__.__doc__ = original_new.__doc__
        __new__.__qualname__ = original_new.__qualname__
        __repr__.__doc__ = original_repr.__doc__
        __repr__.__qualname__ = original_repr.__qualname__
        namedtuple.__new__ = __new__
        namedtuple.__repr__ = __repr__
        namedtuple._typecode = typecode
        namedtuple._idgen = config.SharedCounter(f"Message_{typecode}")


class EventMeta(AnnotatedMessageMeta):
    typecodes = set()


class TaskMeta(AnnotatedMessageMeta):
    typecodes = set()


Event = type.__new__(EventMeta, "Event", (), {})
Task = type.__new__(TaskMeta, "Task", (), {})


def event(typename, fields="", defaults=()):
    module = sys._getframe(1).f_globals.get("__name__", "__main__")
    return _functional_namedtuple_like_api(
        msg_type=Event,
        typename=typename,
        fields=fields,
        defaults=defaults,
        module=module,
    )


def task(typename, fields="", defaults=()):
    module = sys._getframe(1).f_globals.get("__name__", "__main__")
    return _functional_namedtuple_like_api(
        msg_type=Task,
        typename=typename,
        fields=fields,
        defaults=defaults,
        module=module,
    )


def _functional_namedtuple_like_api(
    msg_type, typename, fields, defaults, module
):
    if isinstance(fields, str):
        fields = [field.strip(",") for field in fields.split()]
    annotations = {field: typing.Any for field in fields}
    n_positional_only = len(fields) - len(defaults)
    default_mapping = {
        fields[n_positional_only + i]: default_value
        for i, default_value in enumerate(defaults)
    }
    namespace = {
        "__annotations__": annotations,
        "__module__": module,
        **default_mapping,
    }
    return type(typename, (msg_type,), namespace)


def dump_message(msg, fmt):
    if fmt is tuple:
        return msg
    elif fmt is list:
        return list(msg)
    elif fmt is dict:
        return msg._asdict()
    else:
        raise ValueError(f"Expected fmt={fmt} to be in (tuple, list, dict)")


def load_message(dump):
    if isinstance(dump, dict):
        namedtuple = AnnotatedMessageMeta.namedtuple_lookup[dump["typecode"]]
        return namedtuple(**dump)
    elif isinstance(dump, typing.Sequence):
        namedtuple = AnnotatedMessageMeta.namedtuple_lookup[dump[1]]
        return namedtuple(*dump)
    else:
        raise TypeError(f"dumped message type not recognized: {type(dump)!r}")


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
        worker_messages=None,
        worker_signals=None,
        main_messages=None,
        main_signals=None,
    ):
        self._main_messages = main_messages or dict()
        self._main_signals = self._init_signals(main_signals or dict())
        self._worker_messages = worker_messages or dict()
        self._worker_signals = self._init_signals(worker_signals or dict())
        self._auto = True
        self._running = False
        self._in_child_process = False
        self._main_conn, self._worker_conn = mp.Pipe()
        self._pipe_thread = None
        self._signal_thread = None

    def start(self, *, auto=True):
        self._auto = auto
        self._running = True
        self._in_child_process = (
            mp.current_process().name != config.MAIN_PROCESS
        )

        if self._in_child_process:
            self.taskq = collections.deque()
        if auto:
            self._start_communication_thread()

    def join(self, timeout=None):
        if self._auto:
            self._communication_thread.join(timeout)
        self._running = False

    def kill(self):
        if self._auto and self._pipe_thread:
            self._pipe_thread.kill()
            self._signal_thread.kill()
        self._running = False

    def recv(self):
        _process_signals(
            self._local_signals.values(), self._on_runtime_exception
        )
        _process_messages(
            conn=self._local_conn,
            on_task=self._on_task_recieved,
            on_event=self._on_event_recieved,
            on_exception_recieved=self._on_exception_recieved,
            on_runtime_exception=self._on_runtime_exception,
        )

    def send(self, msg):
        if _is_signal(msg):
            self._send_signal(msg)
        elif isinstance(msg, Exception):
            if not self._in_child_process:
                raise msg
            self._local_conn.send(msg)
        else:
            self._send_message(msg)

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

    def _send_message(self, message):
        if type(message) in self._foreign_messages:
            self._local_conn.send(message)
        else:
            self._handle_unknown_message(message)

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

    @property
    def _on_task_recieved(self):
        def main_implementation(task):
            handlers = self._local_messages[type(task)]
            _handle_message(task, handlers, self._on_runtime_exception)

        def worker_implementation(task):
            handlers = self._local_messages[type(task)]
            self.taskq.append(TaskHandler(task, handlers))

        return (
            worker_implementation
            if self._in_child_process
            else main_implementation
        )

    def _on_event_recieved(self, event):
        handlers = self._local_messages[type(event)]
        _handle_message(event, handlers, self._on_runtime_exception)

    @property
    def _on_exception_recieved(self):
        def main_implementation(exc):
            if self._auto and self._running:
                MainThreadInterruption.interrupt_main(exc)
            else:
                raise exc

        def worker_implementation(exc):
            if self._running:
                MainThreadInterruption.interrupt_main(exc)

        return (
            worker_implementation
            if self._in_child_process
            else main_implementation
        )

    @property
    def _on_runtime_exception(self):
        def main_implementation(exc):
            if self._auto and self._running:
                MainThreadInterruption.interrupt_main(exc)
            else:
                raise exc

        def worker_implementation(exc):
            if self._running:
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
    def _local_messages(self):
        return (
            self._worker_messages
            if self._in_child_process
            else self._main_messages
        )

    @property
    def _local_signals(self):
        return (
            self._worker_signals
            if self._in_child_process
            else self._main_signals
        )

    @property
    def _foreign_messages(self):
        return (
            self._main_messages
            if self._in_child_process
            else self._worker_messages
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
    conn,
    on_task,
    on_event,
    on_exception_recieved=_raise,
    on_runtime_exception=_raise,
):
    try:
        while conn.poll(config.local_context.poll_timeout):
            msg = conn.recv()
            if isinstance(msg, Exception):
                on_exception_recieved(msg)
            elif isinstance(msg, Task):
                on_task(msg)
            elif isinstance(msg, Event):
                on_event(msg)
            else:
                exc = TypeError("Unrecognized message: {msg!r}")
                on_runtime_exception(exc)
    except Exception as exc:
        on_runtime_exception(exc)


def _process_signals(signals, on_runtime_exception=_raise):
    try:
        for sig in signals:
            sig.handle()
    except Exception as exc:
        on_runtime_exception(exc)


def _handle_message(msg, handlers, on_runtime_exception=_raise):
    try:
        for handler in handlers:
            handler(msg)
    except Exception as exc:
        on_runtime_exception(exc)
