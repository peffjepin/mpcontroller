from dataclasses import dataclass


@dataclass
class Context:
    name: str = "MainProcess"
    poll_interval: float = 1e-3
    poll_timeout: float = 1e-3


local_context = Context()
