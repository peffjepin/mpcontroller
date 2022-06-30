import multiprocessing as mp
from dataclasses import dataclass, field


MAIN_PROCESS = "MainProcess"


@dataclass
class Context:
    name: str = MAIN_PROCESS
    poll_interval: float = 1e-3
    poll_timeout: float = 1e-3
    shared_values: dict = field(default_factory=dict)


class SharedCounter:
    def __init__(self, key):
        self.key = key
        self._shared_value = None

    def __next__(self):
        with self._shared.get_lock():
            value = self._shared.value
            self._shared.value = value + 1
        return value

    @property
    def _shared(self):
        # lazily evalulate what shared value object to use so that a worker
        # process has time to setup the local_context variable
        if self._shared_value is not None:
            return self._shared_value
        if self.key in local_context.shared_values:
            self._shared_value = local_context.shared_values[self.key]
            return self._shared_value
        else:
            self._shared_value = mp.Value("i", 1)
            local_context.shared_values[self.key] = self._shared_value
            return self._shared_value


local_context = Context()
