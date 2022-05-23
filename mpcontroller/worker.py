import time
import enum
import collections
import multiprocessing as mp

from . import ipc
from . import util
from . import exceptions
from . import global_state


def _sequential_ids():
    id = 0
    while True:
        yield id
        id += 1


class WorkerTaskMarker(util.MethodMarker):
    pass


class WorkerSignalMarker(util.MethodMarker):
    pass


class MainTaskMarker(util.MethodMarker):
    pass


class MainSignalMarker(util.MethodMarker):
    pass


class HandlerNamespace:
    _help_message = (
        "You must specify which process the handler is meant to execute in:\n"
        "# INCORRECT:\n"
        "class MyWorker(mpc.Worker):\n"
        "    @mpc.handler(MyTask)\n"
        "    def my_handler_that_doesnt_know_where_to_execute(self, task):\n"
        "        ...\n\n"
        "# CORRECT:\n"
        "class MyWorker(mpc.Worker):\n"
        "    @mpc.handler.main(MyTask)\n"
        "    def my_handler_that_executes_in_the_main_process(self, task):\n"
        "        ...\n\n"
        "    @mpc.handler.worker(MyTask)\n"
        "    def my_handler_that_executes_in_the_worker_process(self, task):\n"
        "        ..."
    )

    def worker(self, key):
        try:
            if issubclass(key, ipc.Signal):
                return WorkerSignalMarker(key)
        except Exception:
            return WorkerTaskMarker(key)
        else:
            return WorkerTaskMarker(key)

    def main(self, key):
        try:
            if issubclass(key, ipc.Signal):
                return MainSignalMarker(key)
        except Exception:
            return MainTaskMarker(key)
        else:
            return MainTaskMarker(key)

    def __call__(self, *args, **kwargs):
        raise RuntimeError(self._help_message)


class WorkerStatus(enum.Enum):
    DEAD = 0
    INIT = 1
    IDLE = 2
    BUSY = 3


class ActiveWorkers:
    _worker_lookup = collections.defaultdict(list)

    @classmethod
    def get_by_type(cls, type):
        if type is None:
            return sum(cls._worker_lookup.values(), [])
        return cls._worker_lookup[type]

    @classmethod
    def send_all(cls, communication, type=None):
        for worker in cls.get_by_type(type).copy():
            worker.send(communication)

    @classmethod
    def kill_all(cls, type=None):
        for worker in cls.get_by_type(type).copy():
            worker.kill()

    @classmethod
    def join_all(cls, type=None, timeout=None):
        for worker in cls.get_by_type(type).copy():
            worker.join(timeout)

    @classmethod
    def notify_worker_terminated(cls, worker):
        try:
            cls._worker_lookup[type(worker)].remove(worker)
        except ValueError:
            pass

    @classmethod
    def notify_worker_started(cls, worker):
        cls._worker_lookup[type(worker)].append(worker)


class Worker(mp.Process):
    def __init__(self):
        self._manager = ipc.CommunicationManager(
            main_tasks=MainTaskMarker.make_callback_table(self),
            worker_tasks=WorkerTaskMarker.make_callback_table(self),
            main_signals=MainSignalMarker.make_callback_table(self),
            worker_signals=WorkerSignalMarker.make_callback_table(self),
        )
        self.__status = mp.Value("i", WorkerStatus.DEAD.value, lock=False)
        self._running = False
        super().__init__(target=self._main, args=(global_state.config,))

    def setup(self):
        """Optional stub for subclasses."""

    def mainloop(self):
        """Optional stub for subclasses."""

    def teardown(self):
        """Optional stub for subclasses."""

    @classmethod
    def spawn(cls):
        worker = cls()
        worker.start()
        return worker

    @property
    def pid(self):
        if self._running:
            return super().pid
        return None

    @property
    def status(self):
        return WorkerStatus(self.__status.value)

    @status.setter
    def status(self, new_status: WorkerStatus):
        self.__status.value = new_status.value

    def send(self, communication):
        self._manager.send(communication)

    def start(self):
        self._running = True
        super().start()
        ActiveWorkers.notify_worker_started(self)
        self._manager.start()

    def kill(self):
        self._manager.kill()
        super().kill()
        ActiveWorkers.notify_worker_terminated(self)
        self._running = False

    def join(self, timeout=None):
        self.send(ipc.Terminate)
        super().join(timeout)
        ActiveWorkers.notify_worker_terminated(self)
        self._running = False
        self._manager.join()

    def _main(self, config):
        global_state.config = config
        global_state.config.context = repr(self)

        try:
            self._setup()

            while self._running:
                self._mainloop()

        except Exception as exc:
            self.send(exceptions.WorkerRuntimeError(exc))
            self._exitcode = 1

        finally:
            self._teardown()

        self.__status.value = WorkerStatus.DEAD.value
        raise SystemExit(self._exitcode)

    def _setup(self):
        self._running = True
        self._exitcode = 0
        self._manager.start()
        self._taskthread = _TaskThread(
            taskq=self._manager.taskq,
            on_idle=self._set_idle,
            on_busy=self._set_busy,
        )
        self._taskthread.start()
        self.setup()

    def _mainloop(self):
        self.mainloop()
        time.sleep(global_state.config.poll_interval)

    def _teardown(self):
        try:
            self._manager.join()
            self._taskthread.join()
            self._flush_incoming_communication()
        except Exception as exc:
            if self._exitcode == 0:
                self.send(exceptions.WorkerRuntimeError(exc))
                self._exitcode = 3

        try:
            self.teardown()
        except Exception as exc:
            if self._exitcode == 0:
                self.send(exceptions.WorkerRuntimeError(exc))
                self._exitcode = 4

    @WorkerSignalMarker(ipc.Terminate)
    def _handle_termination_signal(self):
        self._running = False

    def _flush_incoming_communication(self):
        self._manager.flush_inbound_communication()
        for handler in self._manager.taskq.copy():
            handler()

    def _set_idle(self):
        self.status = WorkerStatus.IDLE

    def _set_busy(self):
        self.status = WorkerStatus.BUSY


class _TaskThread(util.MainloopThread):
    def __init__(self, taskq, on_idle, on_busy):
        self.taskq = taskq
        self.on_idle = on_idle
        self.on_busy = on_busy
        self._idle = True
        self.on_idle()
        super().__init__()

    @property
    def idle(self):
        return self._idle

    @idle.setter
    def idle(self, value):
        if self._idle and not value:
            self._idle = False
            self.on_idle()
        elif not self._idle and value:
            self._idle = True
            self.on_busy()

    def mainloop(self):
        if not self.taskq:
            self.idle = True
            time.sleep(global_state.config.poll_interval)

        else:
            self.idle = False

            try:
                handler = self.taskq.popleft()
                handler()
            except Exception as exc:
                ipc.MainThreadInterruption.interrupt_main(exc)
