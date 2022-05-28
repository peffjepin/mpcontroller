import enum
import collections
import multiprocessing as mp

from . import ipc
from . import util
from . import exceptions
from . import global_state


class WorkerTaskMarker(util.MethodMarker):
    pass


class WorkerSignalMarker(util.MethodMarker):
    pass


class WorkerScheduleMarker(util.MethodMarker):
    pass


class MainTaskMarker(util.MethodMarker):
    pass


class MainSignalMarker(util.MethodMarker):
    pass


class MainScheduleMarker(util.MethodMarker):
    pass


class _DecoratorNamespace:
    _help_message = (
        "You must specify which process the decorated function is meant to execute in:\n"
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
        raise NotImplementedError()

    def main(self, key):
        raise NotImplementedError()

    def __call__(self, *args, **kwargs):
        raise RuntimeError(self._help_message)


class ScheduleNamespace(_DecoratorNamespace):
    def worker(self, interval):
        return WorkerScheduleMarker(interval)

    def main(self, interval):
        return MainScheduleMarker(interval)


class HandlerNamespace(_DecoratorNamespace):
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


class _WorkerProcess(mp.Process):
    def __init__(self, worker):
        self.worker = worker
        super().__init__(target=self._main, args=(global_state.config,))

    def _main(self, cfg):
        self.worker.main(cfg)


class Worker:
    def __init__(self):
        self._process = _WorkerProcess(self)
        self._manager = ipc.CommunicationManager(
            main_tasks=MainTaskMarker.make_callback_table(self),
            worker_tasks=WorkerTaskMarker.make_callback_table(self),
            main_signals=MainSignalMarker.make_callback_table(self),
            worker_signals=WorkerSignalMarker.make_callback_table(self),
        )
        self._init_schedule(MainScheduleMarker)
        self.__status = mp.Value("i", WorkerStatus.DEAD.value, lock=False)
        self._running = False

    def setup(self):
        """Optional stub for subclasses."""

    def mainloop(self):
        """Optional stub for subclasses."""

    def teardown(self):
        """Optional stub for subclasses."""

    @classmethod
    def spawn(cls, auto=True):
        worker = cls()
        worker.start(auto=auto)
        return worker

    @property
    def pid(self):
        if self._running:
            return self._process.pid
        return None

    @property
    def status(self):
        return WorkerStatus(self.__status.value)

    @status.setter
    def status(self, new_status: WorkerStatus):
        self.__status.value = new_status.value

    def send(self, communication):
        self._manager.send(communication)

    def recv(self):
        self._manager.recv()
        if self._schedule:
            self._schedule.update()

    def start(self, auto=True):
        self._running = True
        self._process.start()
        self._manager.start(auto=auto)

        if auto and self._schedule:
            self._schedule_thread = _ScheduleThread(self._schedule)
            self._schedule_thread.start()

        ActiveWorkers.notify_worker_started(self)

    def kill(self):
        if self._schedule_thread:
            self._schedule_thread.kill()
        self._manager.kill()
        self._process.kill()
        self._running = False
        self.status = WorkerStatus.DEAD
        ActiveWorkers.notify_worker_terminated(self)

    def join(self, timeout=None):
        if self._schedule_thread:
            self._schedule_thread.join()
        self.send(ipc.Terminate)
        self._process.join(timeout)
        self._manager.join()
        self._manager.flush_inbound_communication()
        self._running = False
        self.status = WorkerStatus.DEAD
        ActiveWorkers.notify_worker_terminated(self)

    def main(self, config):
        global_state.config = config
        global_state.clock = util.Clock(config.poll_interval)
        global_state.config.context = self

        try:
            self._setup()

            while self._running:
                self._mainloop()

        except Exception as exc:
            self.send(exceptions.WorkerRuntimeError(exc))
            self._exitcode = 1

        finally:
            self._teardown()

        raise SystemExit(self._exitcode)

    def _init_schedule(self, markers):
        scheduled_tasks = markers.make_callback_table(self)
        if scheduled_tasks:
            schedule = util.Schedule(scheduled_tasks.items())
        else:
            schedule = None

        self._schedule_thread = None
        self._schedule = schedule

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
        self._init_schedule(WorkerScheduleMarker)
        self.setup()

    def _mainloop(self):
        self.mainloop()
        if self._schedule:
            self._schedule.update()
        global_state.clock.tick(self)

    def _teardown(self):
        self._running = False

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

        self.status = WorkerStatus.DEAD

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
            global_state.clock.tick(self)

        else:
            self.idle = False

            try:
                handler = self.taskq.popleft()
                handler()
            except Exception as exc:
                ipc.MainThreadInterruption.interrupt_main(exc)


class _ScheduleThread(util.MainloopThread):
    def __init__(self, schedule):
        self.schedule = schedule
        super().__init__()

    def mainloop(self):
        self.schedule.update()
