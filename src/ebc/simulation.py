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
        journey.id = next(self._journey_counter)
        self.status.journeys.append(journey)
        fsm = StopFinderFSM(verbose=True,
                            number_of_students=1,
                            current_time=self.status.time,
                            journey=journey)
        fsm.on_event("user_request")
        stops_finding_results = fsm.results
        for student in stops_finding_results:
            self.status.students_automatas.append(StudentAutomata(student))

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
                                                         speed_m_per_s=1.4)
                self.tick()

    def __del__(self):
        """Terminate the thread when the object is deleted"""
        self.terminate.set()
        self.thread.join()

def counter() -> int:
    n: int = 1
    while True:
        yield n
        n += 1
