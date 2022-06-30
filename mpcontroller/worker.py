import enum
import collections
import multiprocessing as mp

from . import ipc
from . import util
from . import exceptions
from . import config


main_message_marker = util.MethodMarker()
main_signal_marker = util.MethodMarker()
main_schedule_marker = util.MethodMarker()

worker_message_marker = util.MethodMarker()
worker_signal_marker = util.MethodMarker()
worker_schedule_marker = util.MethodMarker()


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


class _ScheduleNamespace(_DecoratorNamespace):
    def worker(self, interval):
        return worker_schedule_marker.mark(interval)

    def main(self, interval):
        return main_schedule_marker.mark(interval)


class _HandlerNamespace(_DecoratorNamespace):
    def worker(self, key):
        try:
            if issubclass(key, ipc.Signal):
                return worker_signal_marker.mark(key)
        except Exception:
            return worker_message_marker.mark(key)
        else:
            return worker_message_marker.mark(key)

    def main(self, key):
        try:
            if issubclass(key, ipc.Signal):
                return main_signal_marker.mark(key)
        except Exception:
            return main_message_marker.mark(key)
        else:
            return main_message_marker.mark(key)


schedule_namespace = _ScheduleNamespace()
handler_namespace = _HandlerNamespace()


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
        self._exitcode = 0
        self._parent_context = config.local_context
        super().__init__(target=self._main)

    def _main(self):
        config.local_context = self._parent_context
        config.local_context.name = str(self.worker)
        try:
            self.worker.child_runtime_setup()
            self.worker.child_runtime_main()
        except Exception as exc:
            self._exitcode = 1
            self.worker.send(exceptions.WorkerRuntimeError(exc))
        finally:
            self._teardown()

    def _teardown(self):
        try:
            self.worker.child_runtime_teardown()
        except Exception as exc:
            if self._exitcode == 0:
                self.worker.send(exceptions.WorkerRuntimeError(exc))
            self._exitcode = 2
        finally:
            raise SystemExit(self._exitcode)


class Worker:
    def __init__(self):
        self._process = _WorkerProcess(self)
        self._manager = ipc.CommunicationManager(
            main_messages=main_message_marker.make_callback_table(self),
            worker_messages=worker_message_marker.make_callback_table(self),
            main_signals=main_signal_marker.make_callback_table(self),
            worker_signals=worker_signal_marker.make_callback_table(self),
        )
        self._init_schedule(main_schedule_marker)
        self._status = mp.Value("i", WorkerStatus.DEAD.value, lock=False)
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
        return WorkerStatus(self._status.value)

    @status.setter
    def status(self, new_status: WorkerStatus):
        self._status.value = new_status.value

    def send(self, communication):
        self._manager.send(communication)

    def recv(self):
        self._manager.recv()
        if self._schedule:
            self._schedule.update()

    def start(self, auto=True):
        self._running = True
        self.status = WorkerStatus.INIT
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

    def child_runtime_setup(self):
        self._running = True
        self._manager.start()
        self._taskthread = _TaskThread(
            taskq=self._manager.taskq,
            on_idle=self._set_idle,
            on_busy=self._set_busy,
        )
        self._taskthread.start()
        self._init_schedule(worker_schedule_marker)
        self.setup()

    def child_runtime_main(self):
        while self._running:
            self.mainloop()
            if self._schedule:
                self._schedule.update()
            util.clock.tick(self)

    def child_runtime_teardown(self, exitcode=0):
        self._running = False
        self._manager.join()
        self._taskthread.join()
        self._flush_incoming_communication()
        self.teardown()
        self.status = WorkerStatus.DEAD

    def _init_schedule(self, markers):
        scheduled_tasks = markers.make_callback_table(self)
        if scheduled_tasks:
            schedule = util.Schedule(scheduled_tasks.items())
        else:
            schedule = None

        self._schedule_thread = None
        self._schedule = schedule

    @handler_namespace.worker(ipc.Terminate)
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
            util.clock.tick(self)

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
