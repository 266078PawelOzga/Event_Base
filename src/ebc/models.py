from pydantic import BaseModel
from datetime import datetime, timedelta

class SimulationConfig(BaseModel):
    t0: datetime = datetime.fromtimestamp(0)
    dt: timedelta = timedelta(minutes=1)
    tickrate: float = 1
    tickrate_resolution: float = 0.1

class SimulationStatus(BaseModel):
    reset: bool = True
    running: bool = False
    time: datetime = datetime.fromtimestamp(0)
