import time
import atexit
import traceback
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


class _CentralCommand(collections.defaultdict):
    def __init__(self):
        self._idgen = _sequential_ids()
        self._idmap = dict()
        super().__init__(list)

    def __getitem__(self, key):
        if key is None:
            return sum(self.values(), [])
        return super().__getitem__(key)

    def spawn_worker(self, controller):
        worker = controller.worker_type(controller)
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
        controller.send(ipc.Terminate)
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


_central_command = _CentralCommand()


def message_all(message, type=None):
    for controller in _central_command[type].copy():
        controller.send(message)


def kill_all(type=None):
    for controller in _central_command[type].copy():
        controller.kill()


def join_all(type=None, timeout=None):
    for controller in _central_command[type].copy():
        controller.join(timeout)


atexit.register(kill_all)


class WorkerStatus(enum.Enum):
    DEAD = 0
    INIT = 1
    IDLE = 2
    BUSY = 3


class Controller:
    POLL_INTERVAL = 0.05

    def spawn(self):
        if self._worker:
            raise exceptions.WorkerExistsError(self._worker)

        self._worker = _central_command.spawn_worker(self)
        self._reader = ipc.CommunicationManager(
            self._worker.conn, self.signals, self._recv_message
        )
        self._reader.start()
        return self

    def kill(self):
        if self._worker:
            _central_command.kill_worker(self._worker)
            self._reader.running = False
            self._worker = None
            self._reader.kill()
            self._reader = None

    def join(self, timeout=None):
        if self._worker:
            _central_command.join_worker(self._worker, timeout)
            self._reader.running = False
            self._worker = None
            self._reader.join(timeout)
            self._reader = None

    def send(self, message_or_signal):
        if _message_is_signal(message_or_signal):
            self._send_signal(message_or_signal)
        else:
            self._send_message(message_or_signal)

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
        self.message_callbacks = ipc.MethodMarker.make_callback_table(self)
        self.signals = ipc.SignalMarker.make_signals(self)
        self._worker = None
        self._reader = None
        self._id = id

    def __repr__(self):
        worker = f"Worker={self.worker_type.__name__}"
        return f"<{self.__class__.__name__}: {worker}, id={self.id}>"

    def __str__(self):
        return repr(self)

    def _send_message(self, message):
        if type(message) not in self._worker.message_callbacks:
            raise exceptions.UnknownMessageError(message, self._worker)
        self._worker.conn.send(message)

    def _send_signal(self, signal_type):
        if not _send_signal(signal_type, self._worker.signals):
            raise exceptions.UnknownMessageError(signal_type, self._worker)

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
        return _central_command.controller(cls.CONTROLLER, cls)

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
        self.__status.value = new_status.value

    def send(self, message_or_signal):
        if _message_is_signal(message_or_signal):
            if not _send_signal(message_or_signal, self._controller_signals):
                repr = self._controller_repr
                exc = exceptions.UnknownMessageError(message_or_signal, repr)
                raise exc
        else:
            self._conn.send(message_or_signal)

    def __init__(self, controller):
        self.message_callbacks = ipc.MethodMarker.make_callback_table(self)
        self.signals = ipc.SignalMarker.make_signals(self)

        self._conn, self.conn = mp.Pipe()
        self._controller_signals = controller.signals
        self._controller_repr = repr(controller)
        self._id = controller.id
        self._workthread = None
        self._workthread_exception = None
        self._jobqueue = None
        self.__status = mp.Value("i", WorkerStatus.DEAD.value, lock=False)
        self._running = False
        super().__init__(target=self._mainloop)

    def __repr__(self):
        return f"<{self.__class__.__name__}: {self._id}>"

    def __str__(self):
        return repr(self)

    def _mainloop(self):
        try:
            exitcode = 0
            self._running = True
            self._jobqueue = collections.deque()
            self.setup()
            self._workthread = threading.Thread(
                target=self._workthread_mainloop
            )
            self._workthread.start()

            while self._running:
                self._process_messages()
                self._handle_signals()
                time.sleep(self.POLL_INTERVAL)

            self._workthread.join()

            # one final poll for communication
            self._process_messages()
            self._handle_signals()
            self._flush_jobqueue()

            if self._workthread_exception is not None:
                self.send(self._workthread_exception)
                exitcode = 1

        except Exception as exc:
            self.send(self._wrap_exception(exc))
            exitcode = 1

        try:
            self.teardown()
        except Exception as exc:
            if exitcode == 0:
                self.send(self._wrap_exception(exc))
                exitcode = 1

        self.__status.value = WorkerStatus.DEAD.value
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

    def _handle_signals(self):
        for sig in self.signals.values():
            sig.handle()

    def _message_to_job(self, message):
        key = type(message)
        callbacks = self.message_callbacks[key]

        if not callbacks:
            raise exceptions.UnknownMessageError(message, self)

        return _Job(callbacks, message)

    def _flush_jobqueue(self):
        for job in self._jobqueue:
            job.do()

    def _workthread_mainloop(self):
        try:
            while self._running:
                self._main()
        except Exception as exc:
            self._workthread_exception = self._wrap_exception(exc)
            self._running = False

    def _wrap_exception(self, exc):
        tb = traceback.format_exc()
        wrapped_exc = exceptions.UnhandledWorkerError(exc, tb)
        return wrapped_exc

    @ipc.signal_handler(ipc.Terminate)
    def _handle_termination_signal(self):
        self._running = False


class _Job:
    def __init__(self, callbacks, message):
        self._callbacks = callbacks
        self._args = (message,)

    def do(self):
        for cb in self._callbacks:
            cb(*self._args)


def _send_signal(type, signals):
    if type not in signals:
        return False
    signals[type].activate()
    return True


def _message_is_signal(message):
    return isinstance(message, type) and issubclass(message, ipc.Signal)
