from datetime import datetime, timedelta
from .models import *
from .journey_fsm import journey_fsm_simple
import threading
import time
from .AutomatFSM import StopFinderFSM

class Simulation:
    def __init__(self, config: SimulationConfig, run: bool = False):
        self.config = config
        self.reset()
        if run:
            self.resume()
        self.terminate = threading.Event()
        self.thread = threading.Thread(target=self._simulation_loop, daemon=True)
        self.thread.start()

    def pause(self):
        """Stop advancing the simulation"""
        self.status.running = False

    def resume(self):
        """Start advancing the simulation"""
        self.status.reset = False
        self.status.running = True

    def reset(self):
        """Reset the simulation to initial conditions"""
        self._journey_counter = counter()
        self.status = SimulationStatus(
            reset = True,
            running = False,
            time = self.config.t0,
            journeys = [],
            students_automatas = []
        )

    def tick(self):
        """Advance the simulation by one tick"""
        self.status.time += self.config.dt

    def add_journey_and_automata(self, journey: Journey):
        """Add journey to simulation"""
        fsm = StopFinderFSM(verbose=True,
                            number_of_students=1,
                            current_time=self.status.time,
                            journey=journey)
        fsm.on_event("user_request")
        stops_finding_results = fsm.results
        for student in stops_finding_results:
            # Create automata (this already builds student.path)
            automata = StudentAutomata(student)
            self.status.students_automatas.append(automata)

            # ----------------------------
            # WALK trip (origin → first stop)
            # ----------------------------
            d, stop_id, stop_name, stop_lat, stop_lon = student['nearest_stops'][0]

            walk_loc = Location(
                name=stop_name,
                coord=Coordinates(lat=stop_lat, lon=stop_lon),
                arrival=self.status.time,
                departure=self.status.time
            )

            initial_trip = Trip(
                kind=ModeOfTransport.WALK,
                name="Walk to stop",
                locations=[walk_loc]
            )

            journey.trips.append(initial_trip)

            # ----------------------------
            # BUS trip (full stop path)
            # ----------------------------
            bus_locations = []

            for stop in automata.path:
                loc = Location(
                    name=stop["stop_name"],
                    coord=Coordinates(
                        lat=stop["stop_lat"],
                        lon=stop["stop_lon"]
                    ),
                    arrival=_parse_gtfs_time(self.status.time, stop["arrival_time"]),
                    departure=_parse_gtfs_time(self.status.time, stop["departure_time"])
                )
                bus_locations.append(loc)

            if bus_locations:
                bus_trip = Trip(
                    kind=ModeOfTransport.BUS,
                    name=f"Bus trip {automata.path[0]['stop_name']} → {automata.path[-1]['stop_name']}",
                    locations=bus_locations
                )
                journey.trips.append(bus_trip)
        journey.id = next(self._journey_counter)
        self.status.journeys.append(journey)

    def _simulation_loop(self):
        """Simulation thread execution loop"""
        while not self.terminate.is_set():
            if self.config.tickrate <= 0:
                time.sleep(0.1)
                continue

            time.sleep(1.0/self.config.tickrate)

            if self.status.running:
                for journey in self.status.journeys:
                    journey_fsm_simple(journey)
                    journey.update_position_travel_speed(self.status.time,
                                                         walking_speed_m_per_s=1.4,
                                                         public_transportation_speed_m_per_s=5)
                self.tick()

    def __del__(self):
        """Terminate the thread when the object is deleted"""
        self.terminate.set()
        self.thread.join()

def _parse_gtfs_time(sim_time: datetime, time_str: str) -> datetime:
    """
    Converts GTFS HH:MM:SS into a datetime aligned with simulation date.
    Handles times past midnight (e.g. 25:10:00).
    """
    if not time_str:
        return sim_time

    h, m, s = map(int, time_str.split(":"))

    day_offset = h // 24
    h = h % 24

    base = sim_time.replace(hour=0, minute=0, second=0, microsecond=0)
    return base + timedelta(days=day_offset, hours=h, minutes=m, seconds=s)

def counter() -> int:
    n: int = 1
    while True:
        yield n
        n += 1
