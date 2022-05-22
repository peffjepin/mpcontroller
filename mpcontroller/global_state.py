from dataclasses import dataclass


@dataclass
class Config:
    poll_interval: float = 1e-3


config = Config()
