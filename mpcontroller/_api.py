import multiprocessing as mp
import atexit

from . import exceptions
from . import config
from . import ipc
from . import util
from . import worker

from .exceptions import PicklableException as Exception
from .exceptions import UnknownMessageError
from .exceptions import WorkerRuntimeError
from .ipc import Signal
from .worker import Worker

__all__ = (
    "BUSY",
    "DEAD",
    "Exception",
    "IDLE",
    "Signal",
    "UnknownMessageError",
    "Worker",
    "WorkerRuntimeError",
    "config",
    "cpu_count",
    "handler",
    "join_all",
    "kill_all",
    "schedule",
    "send_all",
)

BUSY = worker.WorkerStatus.BUSY
DEAD = worker.WorkerStatus.DEAD
IDLE = worker.WorkerStatus.IDLE
cpu_count = mp.cpu_count()
handler = worker.HandlerNamespace()
join_all = worker.ActiveWorkers.join_all
kill_all = worker.ActiveWorkers.kill_all
schedule = worker.ScheduleNamespace()
send_all = worker.ActiveWorkers.send_all

atexit.register(worker.ActiveWorkers.kill_all)
