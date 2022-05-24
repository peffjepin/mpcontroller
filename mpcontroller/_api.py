import multiprocessing as mp
import atexit

from . import exceptions
from . import global_state
from . import ipc
from . import worker

from .exceptions import PicklableException as Exception
from .exceptions import UnknownMessageError
from .exceptions import WorkerRuntimeError
from .global_state import config
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
    "send_all",
)

BUSY = worker.WorkerStatus.BUSY
DEAD = worker.WorkerStatus.DEAD
IDLE = worker.WorkerStatus.IDLE
handler = worker.HandlerNamespace()
join_all = worker.ActiveWorkers.join_all
kill_all = worker.ActiveWorkers.kill_all
send_all = worker.ActiveWorkers.send_all
cpu_count = mp.cpu_count()
atexit.register(worker.ActiveWorkers.kill_all)
