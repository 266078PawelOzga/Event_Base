from pydantic import BaseModel
from datetime import datetime, timedelta
from enum import Enum, auto

class SimulationConfig(BaseModel):
    t0: datetime = datetime.fromtimestamp(0)
    dt: timedelta = timedelta(minutes=1)
    tickrate: float = 1
    tickrate_resolution: float = 0.1

class SimulationStatus(BaseModel):
    reset: bool = True
    running: bool = False
    time: datetime = datetime.fromtimestamp(0)

class TripState(str, Enum):
    CREATED = "CREATED"
    PLANNED = "PLANNED"
    IN_PROGRESS = "IN_PROGRESS"
    FINISHED = "FINISHED"

class Trip(BaseModel):
    state: TripState = TripState.CREATED
    origin: str = "origin"
    origin_coord: tuple[float, float] | None = None
    destination: str = "destination"
    destination_coord: tuple[float, float] | None = None
