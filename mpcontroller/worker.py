import time
import enum
import threading
import collections
import multiprocessing as mp

from . import ipc
from . import exceptions


def _sequential_ids():
    id = 0
    while True:
        yield id
        id += 1


class _Registry(collections.defaultdict):
    def __init__(self):
        self._idgen = _sequential_ids()
        self._idmap = dict()
        super().__init__(list)

    def __getitem__(self, key):
        if key is None:
            return sum(self.values(), [])
        return super().__getitem__(key)

    def spawn_worker(self, controller):
        worker = controller.worker_type(controller.id)
        worker.status = WorkerStatus.INIT
        worker.start()
        self[type(worker)].append(controller)
        return worker

    def kill_worker(self, worker):
        controller = self._idmap[worker.id]
        worker.kill()
        self[type(worker)].remove(controller)

    def join_worker(self, worker, timeout):
        controller = self._idmap[worker.id]
        worker.status = WorkerStatus.TERM
        worker.join(timeout)
        self[type(worker)].remove(controller)

    def controller(self, controller_type, worker_type):
        id = next(self._idgen)
        controller = controller_type(worker_type, id)
        self._idmap[id] = controller
        return controller

    def clear(self):
        super().clear()
        self._idmap.clear()
        self._idgen = _sequential_ids()


_registry = _Registry()


def message_all(message, type=None):
    for controller in _registry[type].copy():
        controller.send(message)


def kill_all(type=None):
    for controller in _registry[type].copy():
        controller.kill()


def join_all(type=None):
    for controller in _registry[type].copy():
        controller.join()


class WorkerStatus(enum.Enum):
    DEAD = 0
    INIT = 1
    IDLE = 2
    BUSY = 3
    TERM = 4


class Controller:
    POLL_INTERVAL = 0.05

    def spawn(self):
        if self._worker:
            raise exceptions.WorkerExistsError(self._worker)

        self._worker = _registry.spawn_worker(self)
        self._reader = ipc.PipeReader(self._worker.conn, self._recv_message)
        self._reader.start()
        return self

    def kill(self):
        if self._worker:
            _registry.kill_worker(self._worker)
            self._worker = None
            self._reader.kill()
            self._reader = None

    def join(self, timeout=None):
        if self._worker:
            _registry.join_worker(self._worker, timeout)
            self._worker = None
            self._reader.join(timeout)
            self._reader = None

    def send(self, message_or_signal):
        if isinstance(message_or_signal, type) and issubclass(
            message_or_signal, ipc.Signal
        ):
            self._send_signal(message_or_signal)
        else:
            self._send(message_or_signal)

    @property
    def id(self):
        return self._id

    @property
    def status(self):
        return self._worker.status if self._worker else WorkerStatus.DEAD

    @property
    def pid(self):
        return self._worker.pid if self._worker else None

    def __init__(self, worker_type, id):
        self.worker_type = worker_type
        self.message_callbacks = ipc.IpcHandler.get_message_callback_table(
            self
        )
        self._worker = None
        self._reader = None
        self._id = id

    def __repr__(self):
        worker = f"Worker={self.worker_type.__name__}"
        return f"<{self.__class__.__name__}: {worker}, id={self.id}>"

    def __str__(self):
        return repr(self)

    def _send_signal(self, signal_type):
        if signal_type not in self._worker.signal_callbacks:
            raise exceptions.UnknownMessageError(signal_type, self._worker)
        else:
            self._worker.signals[signal_type].set()

    def _send(self, message):
        if type(message) not in self._worker.message_callbacks:
            raise exceptions.UnknownMessageError(message, self._worker)
        self._worker.conn.send(message)

    def _recv_message(self, message):
        key = type(message)

        if key not in self.message_callbacks:
            raise exceptions.UnknownMessageError(message, self)

        for callback in self.message_callbacks[key]:
            callback(message)


class Worker(mp.Process):
    CONTROLLER = Controller
    POLL_INTERVAL = 0.05

    def setup(self):
        """Optional stub for subclasses."""

    def main(self):
        """Optional stub for subclasses."""

    def teardown(self):
        """Optional stub for subclasses."""

    @classmethod
    def controller(cls):
        return _registry.controller(cls.CONTROLLER, cls)

    @classmethod
    def spawn(cls):
        controller = cls.controller()
        return controller.spawn()

    @property
    def id(self):
        return self._id

    @property
    def status(self):
        return WorkerStatus(self.__status.value)

    @status.setter
    def status(self, new_status: WorkerStatus):
        status = self.status
        if status == WorkerStatus.TERM:
            # don't change status once terminating has been signaled
            return
        if new_status != status:
            self.__status.value = new_status.value

    def send(self, message):
        self._conn.send(message)

    def __init__(self, id):
        self.message_callbacks = ipc.IpcHandler.get_message_callback_table(
            self
        )
        self.signal_callbacks = ipc.IpcHandler.get_signal_callback_table(self)
        self.signals = {
            sig_type: sig_type(mp.Value("i"))
            for sig_type in self.signal_callbacks
        }

        self._conn, self.conn = mp.Pipe()
        self._id = id
        self._workthread = None
        self._workthread_exception = None
        self._jobqueue = None
        self.__status = mp.Value("i", WorkerStatus.DEAD.value)
        super().__init__(target=self._mainloop)

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self._id}>"

    def __str__(self):
        return repr(self)

    def _mainloop(self):
        try:
            exitcode = 0
            self._jobqueue = collections.deque()
            self.setup()
            self._workthread = threading.Thread(
                target=self._workthread_mainloop
            )
            self._workthread.start()

            while self.status != WorkerStatus.TERM:
                self._process_messages()
                self._clear_signals()
                time.sleep(self.POLL_INTERVAL)

            self._workthread.join(timeout=1)

            if self._workthread_exception is not None:
                self.send(self._workthread_exception)
                exitcode = 1

        except Exception as exc:
            self.send(exc)
            exitcode = 1

        try:
            self.teardown()
        except Exception as exc:
            if exitcode == 0:
                self.send(exc)
                exitcode = 1

        self.status = WorkerStatus.DEAD
        return exitcode

    def _main(self):
        if not self._jobqueue:
            self.status = WorkerStatus.IDLE
            time.sleep(self.POLL_INTERVAL)
        else:
            self.status = WorkerStatus.BUSY
            job = self._jobqueue.popleft()
            job.do()
        self.main()

    def _process_messages(self):
        while self._conn.poll(0):
            message = self._conn.recv()
            job = self._message_to_job(message)
            self._jobqueue.append(job)

    def _clear_signals(self):
        for type, sig in self.signals.items():
            if not sig.is_set:
                continue

            for callback in self.signal_callbacks[type]:
                callback()

            sig.clear()

    def _message_to_job(self, message):
        key = type(message)
        callbacks = self.message_callbacks[key]

        if not callbacks:
            raise exceptions.UnknownMessageError(message, self)

        return _Job(callbacks, message)

    def _workthread_mainloop(self):
        try:
            while self.status != WorkerStatus.TERM:
                self._main()
        except Exception as exc:
            self._workthread_exception = exc
            self.status = WorkerStatus.TERM


class _Job:
    def __init__(self, callbacks, message):
        self._callbacks = callbacks
        self._args = (message,)

    def do(self):
        for cb in self._callbacks:
            cb(*self._args)
