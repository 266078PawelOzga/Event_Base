from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator
from datetime import datetime, timedelta
from enum import Enum, unique
from typing import NamedTuple
import math
from .student_automata import StudentAutomata

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
    visited: bool = False

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
    events: list[str] = []
    trips: list[Trip] = []

    @property
    def locations(self):
        return [self.origin] + \
               [loc for trip in self.trips for loc in trip.locations ] + \
               [self.destination]

    def update_position_arrival_time(self, time: datetime):
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
        
    def update_position_travel_speed(self, time: datetime, walking_speed_m_per_s: float,
                                     public_transportation_speed_m_per_s: float):
        from .target_stop import get_distance_from_lat_lon_in_m
        next_location = None
        for location in self.locations:
            if location.visited == False:
                next_location = location
                break
        
        # Determine which trip this segment belongs to
        speed_m_per_s = walking_speed_m_per_s  # default fallback

        for trip in self.trips:
            if next_location in trip.locations:
                if trip.kind == ModeOfTransport.WALK:
                    speed_m_per_s = walking_speed_m_per_s
                elif trip.kind == ModeOfTransport.BUS:
                    speed_m_per_s = public_transportation_speed_m_per_s
                break

        if next_location:
            distance_to_next_location_m = get_distance_from_lat_lon_in_m(self.current_position.lat, self.current_position.lon,
                                        next_location.coord.lat, next_location.coord.lon)
            timestep = (time - self.current_time).total_seconds()
            time_to_next_location = distance_to_next_location_m/speed_m_per_s
            if time_to_next_location > 0.1:
                a = min(timestep / time_to_next_location, 1)
                self.current_position = self.current_position * (1-a) + next_location.coord * a
            else:
                self.current_position = next_location.coord
                next_location.visited = True
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
    students_automatas: list[StudentAutomata] = Field(default_factory=list)

    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    @field_validator("students_automatas", mode="before")
    @classmethod
    def parse_students(cls, v):
        # v is the raw JSON value (list of dicts)
        if v is None:
            return []

        students = []
        for s in v:
            automata = StudentAutomata(
                student={"student_id": s["student_id"]}
            )
            automata.state = s["state"]
            students.append(automata)

        return students
    
    @field_serializer("students_automatas")
    def serialize_students(self, students):
        return [s.to_dict() for s in students]
