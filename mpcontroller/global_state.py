from dataclasses import dataclass

from . import util


@dataclass
class Config:
    poll_interval: float = 1e-3
    poll_timeout: float = 1e-3
    context: str = "MainProcess"


config = Config()
clock = util.Clock(config.poll_interval)
