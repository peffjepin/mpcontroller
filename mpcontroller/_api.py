import multiprocessing as mp
import atexit

from . import exceptions
from . import config
from . import ipc
from . import util
from . import worker

from .config import MAIN_PROCESS
from .exceptions import PicklableException as Exception
from .exceptions import UnknownMessageError
from .exceptions import WorkerRuntimeError
from .ipc import Event
from .ipc import Signal
from .ipc import Task
from .ipc import event
from .ipc import task
from .worker import Worker

__all__ = (
    "BUSY",
    "DEAD",
    "Event",
    "Exception",
    "IDLE",
    "MAIN_PROCESS",
    "Signal",
    "Task",
    "UnknownMessageError",
    "Worker",
    "WorkerRuntimeError",
    "config",
    "cpu_count",
    "event",
    "handler",
    "join_all",
    "kill_all",
    "schedule",
    "send_all",
    "task",
)

BUSY = worker.WorkerStatus.BUSY
DEAD = worker.WorkerStatus.DEAD
IDLE = worker.WorkerStatus.IDLE
cpu_count = mp.cpu_count()
handler = worker.handler_namespace
join_all = worker.ActiveWorkers.join_all
kill_all = worker.ActiveWorkers.kill_all
schedule = worker.schedule_namespace
send_all = worker.ActiveWorkers.send_all

atexit.register(worker.ActiveWorkers.kill_all)
