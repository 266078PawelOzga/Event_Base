from datetime import datetime, timedelta
from .models import *
from .journey_fsm import journey_fsm_simple
import threading
import time
from .AutomatFSM import StopFinderFSM
from .target_stop import get_distance_from_lat_lon_in_m

WYSPA_SLODOWA = Location(
    name="Wyspa Słodowa",
    coord=Coordinates(lat=51.1159, lon=17.0374)
)

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
            students_automatas = [],
            crashes = []
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
            automata.journey = journey
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
                automata.on_event('stop_found')
            else:
                automata.on_event('no_bus_available')
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
                for journey in list(self.status.journeys):
                    if journey.state == JourneyState.CRASHED:
                        if journey.restart_after is None:
                            continue

                        if self.status.time < journey.restart_after:
                            continue  # still waiting

                        # ⏱ delay expired → restart
                        self._handle_crash(journey)
                        continue

                    old_state = journey.state
                    journey_fsm_simple(journey)
                    crash_detected = False

                    for event in journey.events:
                        if event == 'Delay':
                            journey.remaining_delay_s += 300
                        elif event == 'Crash':
                            crash_detected = True
                            journey.state = JourneyState.CRASHED
                            self.status.crashes.append((journey.current_position, journey.origin.name, journey.destination.name))
                            journey.restart_after = self.status.time + timedelta(minutes=10)
                            for automata in self.status.students_automatas:
                                if automata.journey is journey:
                                    automata.on_event("crash")
                        elif event == 'classes_canceled':
                            for automata in self.status.students_automatas:
                                if automata.journey is journey:
                                    automata.on_event("classes_canceled")

                            self._handle_classes_canceled(journey)
                            # journey.events.clear()
                            break  # IMPORTANT: journey is gone after this

                    #NOTE: assumes all the events were handled 
                    journey.events.clear()
                    journey.update_position_travel_speed(time = self.status.time,
                                                    walking_speed_m_per_s=1.4,
                                                    public_transportation_speed_m_per_s=5)
                    # Checking if the goal was reached
                    if old_state != journey.state and journey.state == "FINISHED":
                        for automata in self.status.students_automatas:
                            if automata.journey is journey:
                                if automata.state != "TERMINAL_STATE":
                                    automata.on_event("goal_reached")
                    # Checking if the stop was reached
                    for automata in self.status.students_automatas:
                        if automata.journey is not journey:
                            continue
                        if automata.state == "WALK_TO_STOP":
                            walk_trip = journey.trips[0]
                            stop_location = walk_trip.locations[0]
                            current_pos = journey.current_position  # <-- adjust name if needed

                            if self._is_at_location(current_pos, stop_location.coord):
                                automata.on_event("stop_reached")
                        elif automata.state == "WAITING_FOR_TRANSPORTATION":
                            # NOTE: this will just assume, the bus is there in the moment student
                            # arrives to the stop
                            automata.on_event("available_bus_arrived")
                        elif automata.state == "TRAVELING_BY_TRANSPORTATION":
                            bus_trip = journey.trips[1]   # WALK=0, BUS=1
                            final_stop = bus_trip.locations[-1]

                            if self._is_at_location(journey.current_position, final_stop.coord):
                                automata.on_event("final_stop_reached")

                self.tick()

    def _is_at_location(self, pos: Coordinates,
                target: Coordinates,
                threshold_m: float = 5.0) -> bool:
        """
        Returns True if pos is within threshold meters of target.
        """
        if pos is None or target is None:
            return False

        return get_distance_from_lat_lon_in_m(pos.lat, pos.lon,
                                              target.lat,
                                              target.lon) <= threshold_m

    def _handle_crash(self, journey: Journey):
        """
        Terminates the given journey and spawns a new one
        from the current position to the original destination.
        """

        # 1. Capture state
        current_pos = journey.current_position
        destination = journey.destination

        if current_pos is None:
            return  # nothing sensible to do

        # 2. Create a new origin location
        new_origin = Location(
            name="Crash location",
            coord=current_pos,
            arrival=self.status.time,
            departure=self.status.time
        )

        # 3. Create a new journey
        new_journey = Journey(
            origin=new_origin,
            destination=destination,
            current_time=self.status.time
        )

        new_journey.current_position = new_origin.coord
        new_journey.id = next(self._journey_counter)

        # 4. Rebind automata
        for automata in self.status.students_automatas:
            if automata.journey is journey:
                automata.journey = new_journey
                automata.on_event("journey_restarted")  # optional FSM hook

        # 5. Remove old journey and automata
        self.status.journeys.remove(journey)

        self.status.students_automatas = [
            a for a in self.status.students_automatas
            if a.journey is not journey
        ]

        # 6. Initialize routing for the new journey
        self.add_journey_and_automata(new_journey)

    def _handle_classes_canceled(self, journey: Journey):
        journey.events.clear()
        current_pos = journey.current_position
        if current_pos is None:
            return

        new_origin = Location(
            name="Current location",
            coord=current_pos,
            arrival=self.status.time,
            departure=self.status.time
        )

        new_destination = WYSPA_SLODOWA

        new_journey = Journey(
            origin=new_origin,
            destination=new_destination,
            current_time=self.status.time
        )
        new_journey.current_position = current_pos
        new_journey.log_message("Classes canceled — walking to Wyspa Słodowa")

        # 🚶 WALK-ONLY trip
        walk_trip = Trip(
            kind=ModeOfTransport.WALK,
            name="Walk to Wyspa Słodowa",
            locations=[new_destination]
        )
        new_journey.trips.append(walk_trip)

        # Rebind automata
        for automata in self.status.students_automatas:
            if automata.journey is journey:
                automata.journey = new_journey
                automata.on_event("classes_canceled")

        self.status.journeys.remove(journey)
        self.status.journeys.append(new_journey)


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
