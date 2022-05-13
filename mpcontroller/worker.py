import time
import threading
import collections
import itertools
import multiprocessing as mp

from . import ipc
from . import exceptions


_MESSAGE_HANDLER_INJECTION = "_pyscrape_handlers_"


def message_handler(msg_type):
    def inner(fn):
        setattr(fn, _MESSAGE_HANDLER_INJECTION, msg_type)
        return fn

    return inner


def _create_callback_registry(obj):
    callbacks = collections.defaultdict(list)

    for key in dir(obj):
        method = getattr(obj, key, None)
        fn = getattr(method, "__func__", None)

        if not fn:
            continue

        msg_type = getattr(fn, _MESSAGE_HANDLER_INJECTION, None)

        if not msg_type:
            continue

        callbacks[msg_type].append(method)

    return callbacks


class Controller:

    POLL_RATE = 0.05
    _ID_COUNTER = itertools.count(1)

    def __init__(self, worker_type):
        self._status = Worker.DEAD
        self._worker_type = worker_type
        self._worker = None
        self._reader = None
        self._id = next(self._ID_COUNTER)

        self.last_health_check = -1
        self.messages = collections.deque()
        self.exceptions = collections.deque()

    def spawn(self):
        if self._worker:
            raise exceptions.WorkerExistsError(self._worker)

        self._status = Worker.INITIALIZING
        self._worker = self._worker_type(self.id)
        self._reader = ipc.PipeReader(
            self._worker.conn, self._recv_exception_message, self._recv_message
        )
        self._worker.start()
        self._reader.start()

    def kill(self):
        if self._worker:
            self.send_message(ipc.Signal.SHUTDOWN)
            self._reader.join()
            self._reader = None
            self._worker.kill()
            self._worker = None
            self._status = Worker.DEAD

    def join(self, timeout=5):
        if self._worker:
            self.send_message(ipc.Signal.SHUTDOWN)
            self._reader.join(timeout)
            self._reader = None
            self._worker.join(timeout)
            self._worker = None
            self._status = Worker.DEAD

    def send_message(self, message):
        self._worker.conn.send(message)

    @property
    def id(self):
        return self._id

    @property
    def status(self):
        if (
            self._status in Worker.EXIT_STATES
            and self._worker.exitcode is not None
        ):
            self.join()
        return self._status

    @property
    def worker(self):
        if self._worker is None:
            return None
        else:
            # dont leak the worker api, use repr instead
            return repr(self._worker)

    @property
    def pid(self):
        return self._worker.pid if self._worker else None

    def _recv_exception_message(self, exc):
        self.exceptions.append(exc)

    def _recv_message(self, msg):
        if isinstance(msg, ipc.StatusUpdate):
            self._status = msg.status
            self.last_health_check = time.time()
        else:
            self.messages.append(msg)


class Worker(mp.Process):

    DEAD = "dead"
    IDLE = "idle"
    BUSY = "busy"
    UNRESPONSIVE = "unresponsive"
    INITIALIZING = "initializing"
    TERMINATING = "terminating"
    ERROR = "panic"
    EXIT_STATES = {TERMINATING, ERROR}

    POLL_RATE = 0.05
    ERROR_TOLERANCE = 1

    @classmethod
    def controller(cls):
        return Controller(cls)

    @classmethod
    def spawn(cls):
        controller = Controller(cls)
        controller.spawn()
        return controller

    def __init__(self, id):
        self._conn, self.conn = mp.Pipe()
        self._id = id
        self._workthread = None
        self._error_streak = 0
        self._jobqueue = None
        self.__status = Worker.INITIALIZING
        self._message_callbacks = _create_callback_registry(self)
        super().__init__(target=self._mainloop)

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self._id}>"

    def setup(self):
        """Optional stub for subclasses."""

    def main(self):
        if not self._jobqueue:
            self.status = Worker.IDLE
            time.sleep(self.POLL_RATE)
        else:
            self.status = Worker.BUSY
            job = self._jobqueue.popleft()
            job.do()

    @property
    def status(self):
        return self.__status

    @status.setter
    def status(self, value):
        if self.__status in self.EXIT_STATES:
            # don't change status once terminating has been signaled
            return
        if value != self.__status:
            self.__status = value
            self.send_message(ipc.StatusUpdate(self.status))

    def send_message(self, message):
        self._conn.send(message)

    def report_exception(self, exc):
        self._error_streak += 1
        self.send_message(exc)
        if self._error_streak >= self.ERROR_TOLERANCE:
            self.status = Worker.ERROR

    def _setup(self):
        try:
            self.setup()
        except Exception as exc:
            self.report_exception(exc)

    def _mainloop(self):
        self._running = True
        self._jobqueue = collections.deque()
        self._setup()
        self._workthread = threading.Thread(target=self._workthread_target)
        self._workthread.start()

        while self.status not in self.EXIT_STATES:
            self._process_messages()
            time.sleep(self.POLL_RATE)

        self._workthread.join(timeout=1)

        if self.status == Worker.ERROR:
            self.send_message(exceptions.WorkerExitError(self._id))
            return 1

        return 0

    def _process_messages(self):
        while self._conn.poll(0):
            msg = self._conn.recv()
            job = self._message_to_job(msg)

            if not job:
                continue

            if job.urgent:
                job.do()

            else:
                self._jobqueue.append(job)

    def _message_to_job(self, msg):
        if isinstance(msg, ipc.Signal):
            key = msg
        else:
            key = type(msg)

        callbacks = self._message_callbacks[key]
        if not callbacks:
            return self._handle_unknown_message(msg)

        return Job(callbacks, msg, self)

    def _workthread_target(self):
        while self.status not in self.EXIT_STATES:
            try:
                self.main()
                self._error_streak = 0
            except Exception as exc:
                self.report_exception(exc)

    def _handle_unknown_message(self, msg):
        exc = exceptions.UnknownMessageError(msg, repr(self))
        self.send_message(exc)

    @message_handler(ipc.Signal.SHUTDOWN)
    def _internal_shutdown_handler(self):
        self.status = Worker.TERMINATING

    @message_handler(ipc.Signal.HEALTH_CHECK)
    def _internal_health_check_handler(self):
        self.send_message(self.status)


class Job:
    _URGENT_TYPES = {ipc.Signal.SHUTDOWN, ipc.Signal.HEALTH_CHECK}

    def __init__(self, callbacks, msg, worker):
        self._callbacks = callbacks
        self._msg = msg
        self._worker = worker

    @property
    def args(self):
        msg = self._msg
        if isinstance(msg, ipc.Signal):
            return ()
        return (msg,)

    @property
    def urgent(self):
        return self._msg in self._URGENT_TYPES

    def do(self):
        args = self.args
        for cb in self._callbacks:
            try:
                cb(*args)
            except Exception as exc:
                # TODO: consider new exception type
                self._worker.report_exception(exc)
