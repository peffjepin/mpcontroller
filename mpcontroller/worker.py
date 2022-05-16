import time
import enum
import threading
import collections
import itertools
import multiprocessing as mp

from . import ipc
from . import exceptions


_registry = collections.defaultdict(list)


class WorkerStatus(enum.Enum):
    DEAD = 0
    INIT = 1
    IDLE = 2
    BUSY = 3
    TERM = 4


def message_all(msg, type=None):
    for controller_type, controller_list in _registry.items():
        if type and type != controller_type:
            continue
        for controller in controller_list.copy():
            controller.send_message(msg)


def kill_all(type=None):
    for controller_type, controller_list in _registry.items():
        if type and type != controller_type:
            continue
        for controller in controller_list.copy():
            controller.kill()


def join_all(type=None):
    for controller_type, controller_list in _registry.items():
        if type and type != controller_type:
            continue
        for controller in controller_list.copy():
            controller.join()


class Controller:

    POLL_INTERVAL = 0.05
    _ID_COUNTER = itertools.count(1)

    def __init__(self, worker_type):
        self._worker_type = worker_type
        self._worker = None
        self._reader = None
        self._id = next(self._ID_COUNTER)
        self.message_callbacks = ipc.IpcHandler.get_message_callback_table(
            self
        )

    def __repr__(self):
        worker = f"Worker={self._worker_type.__name__}"
        return f"<{self.__class__.__name__}: {worker}, id={self.id}>"

    def spawn(self):
        if self._worker:
            raise exceptions.WorkerExistsError(self._worker)

        self._worker = self._worker_type(self.id)
        self._worker.status = WorkerStatus.INIT
        self._reader = ipc.PipeReader(self._worker.conn, self._recv_message)
        self._worker.start()
        self._reader.start()
        _registry[self._worker_type].append(self)

    def kill(self):
        if self._worker:
            self._worker.status = WorkerStatus.TERM
            self._reader.kill()
            self._reader = None
            self._worker.kill()
            self._worker = None
            _registry[self._worker_type].remove(self)

    def join(self, timeout=5):
        deadline = time.time() + timeout
        if self._worker:
            self._worker.status = WorkerStatus.TERM
            self._worker.join(deadline - time.time())
            self._reader.join(deadline - time.time())
            self._worker = None
            self._reader = None
            _registry[self._worker_type].remove(self)

    def send_message(self, message):
        if not isinstance(message, type):
            if type(message) not in self._worker.message_callbacks:
                raise exceptions.UnknownMessageError(message, self.worker)
            self._worker.conn.send(message)
        else:
            self._send_signal(message)

    def _send_signal(self, signal_type):
        if signal_type not in self._worker.signal_callbacks:
            raise exceptions.UnknownMessageError(signal_type, self.worker)
        else:
            self._worker.signals[signal_type].set()

    @property
    def id(self):
        return self._id

    @property
    def status(self):
        return self._worker.status if self._worker else WorkerStatus.DEAD

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
        key = type(msg)

        if key not in self.message_callbacks:
            raise exceptions.UnknownMessageError(msg, repr(self))

        for cb in self.message_callbacks[key]:
            cb(msg)


class Worker(mp.Process):
    CONTROLLER = Controller
    POLL_INTERVAL = 0.05

    @classmethod
    def controller(cls):
        return cls.CONTROLLER(cls)

    @classmethod
    def spawn(cls):
        controller = cls.CONTROLLER(cls)
        controller.spawn()
        return controller

    def __init__(self, id):
        self._conn, self.conn = mp.Pipe()
        self._id = id
        self._workthread = None
        self._workthread_error = None
        self._jobqueue = None
        self.message_callbacks = ipc.IpcHandler.get_message_callback_table(
            self
        )
        self.signal_callbacks = ipc.IpcHandler.get_signal_callback_table(self)
        self.signals = {
            sig_type: sig_type(mp.Value("i"))
            for sig_type in self.signal_callbacks
        }
        self.__status = mp.Value("i", WorkerStatus.DEAD.value)
        super().__init__(target=self._mainloop)

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self._id}>"

    def setup(self):
        """Optional stub for subclasses."""

    def main(self):
        """Optional stub for subclasses."""

    def teardown(self):
        """Optional stub for subclasses."""

    def _main(self):
        if not self._jobqueue:
            self.status = WorkerStatus.IDLE
            time.sleep(self.POLL_INTERVAL)
        else:
            self.status = WorkerStatus.BUSY
            job = self._jobqueue.popleft()
            job.do()

    @property
    def status(self):
        with self.__status.get_lock():
            return WorkerStatus(self.__status.value)

    @status.setter
    def status(self, new_status: WorkerStatus):
        status = self.status
        if status == WorkerStatus.TERM:
            # don't change status once terminating has been signaled
            return
        if new_status != status:
            with self.__status.get_lock():
                self.__status.value = new_status.value

    def send_message(self, message):
        self._conn.send(message)

    def _mainloop(self):
        try:
            exitcode = 0
            self._jobqueue = collections.deque()
            self.setup()
            self._workthread = threading.Thread(target=self._workthread_target)
            self._workthread.start()

            while self.status != WorkerStatus.TERM:
                self._process_messages()
                self._clear_signals()
                time.sleep(self.POLL_INTERVAL)

            self._workthread.join(timeout=1)

            if self._workthread_error is not None:
                self.send_message(self._workthread_error)
                exitcode = 1

        except Exception as exc:
            self.send_message(exc)
            exitcode = 1

        try:
            self.teardown()
        except Exception as exc:
            if exitcode == 0:
                self.send_message(exc)
                exitcode = 1

        self.status = WorkerStatus.DEAD
        return exitcode

    def _process_messages(self):
        while self._conn.poll(0):
            msg = self._conn.recv()
            job = self._message_to_job(msg)

            if not job:
                continue

            self._jobqueue.append(job)

    def _clear_signals(self):
        for type, sig in self.signals.items():
            if not sig.is_set:
                continue

            for cb in self.signal_callbacks[type]:
                cb()

            sig.clear()

    def _message_to_job(self, msg):
        if isinstance(msg, ipc.Signal):
            key = msg
        else:
            key = type(msg)

        callbacks = self.message_callbacks[key]
        if not callbacks:
            raise exceptions.UnknownMessageError(msg, repr(self))

        return _Job(callbacks, msg, self)

    def _workthread_target(self):
        while self.status != WorkerStatus.TERM:
            try:
                self._main()
                self.main()
            except Exception as exc:
                self._workthread_error = exc
                self.status = WorkerStatus.TERM
                break


class _Job:
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

    def do(self):
        args = self.args
        for cb in self._callbacks:
            cb(*args)
