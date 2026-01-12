from pydantic import BaseModel
from datetime import datetime, timedelta
from enum import Enum, unique
from typing import NamedTuple
import math

# Trip
# ----------------------------------------------------------

@unique
class JourneyState(str, Enum):
    CREATED     = "CREATED"
    IN_PROGRESS = "IN_PROGRESS"
    FINISHED    = "FINISHED"

@unique
class ModeOfTransport(str, Enum):
    WALK = "WALK"
    BUS  = "BUS"

class Coordinates(BaseModel):
    lat: float
    lon: float
    def norm(self) -> float:
        return (self.lat**2 + self.lon**2)**0.5
    def __add__(self, other: 'Coordinates') -> 'Coordinates':
        return Coordinates(lat = self.lat + other.lat, lon = self.lon + other.lon)
    def __sub__(self, other: 'Coordinates') -> 'Coordinates':
        return Coordinates(lat = self.lat - other.lat, lon = self.lon - other.lon)
    def __mul__(self, other: float) -> 'Coordinates':
        return Coordinates(lat = self.lat * other, lon = self.lon * other)
    def tuple(self) -> tuple[float, float]:
        return (self.lat, self.lon)


class Location(BaseModel):
    """
    Place the trip goes through
    name      : display name
    coord     : longitude and latitude
    arrival   : epected/actual time of arrival
    departure : epected/actual time of departure
    """
    name: str
    coord: Coordinates | None = None
    arrival: datetime | None = None
    departure: datetime | None = None

class Trip(BaseModel):
    """
    Part of a journey
    Can describe walking or taking a bus from one place to another
    """
    kind: ModeOfTransport
    name: str
    locations: list[Location]

class Journey(BaseModel):
    """
    How to take the user from origin to destination
    Planned and modified by the state machine
    """
    state: JourneyState = JourneyState.CREATED
    id: int | None = None
    origin: Location
    destination: Location
    current_position: Coordinates | None = None
    current_time: datetime | None = None
    trips: list[Trip] = []

    @property
    def locations(self):
        return [self.origin] + \
               [loc for trip in self.trips for loc in trip.locations ] + \
               [self.destination]

    # TODO: make it linear
    def update_position(self, time: datetime):
        next_location = None
        for location in self.locations:
            if location.arrival > self.current_time:
                next_location = location
                break

        if next_location:
            timestep = (time - self.current_time).total_seconds()
            time_to_next_location = (next_location.arrival - self.current_time).total_seconds()
            a = min(timestep / time_to_next_location, 1)
            self.current_position = self.current_position * (1-a) + next_location.coord * a
        else:
            self.current_position = self.locations[-1].coord

        self.current_time = time


# Simulation
# ----------------------------------------------------------

class SimulationConfig(BaseModel):
    t0: datetime = datetime.fromtimestamp(0)
    dt: timedelta = timedelta(minutes=1)
    tickrate: float = 1
    tickrate_resolution: float = 0.1

class SimulationStatus(BaseModel):
    reset: bool = True
    running: bool = False
    time: datetime = datetime.fromtimestamp(0)
    journeys: list[Journey] = []
