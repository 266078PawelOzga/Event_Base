from datetime import datetime, timedelta
from .models import *
import threading
import time

class Simulation:
    def __init__(self, config: SimulationConfig, run: bool = False):
        self.config = config
        self.reset()
        if run:
            self.resume()
        self.terminate = threading.Event()
        self.thread = threading.Thread(target=self._simulation_loop, daemon=True)
        self.thread.start()

    def pause(self) -> None:
        """Stop advancing the simulation"""
        self.status.running = False

    def resume(self) -> None:
        """Start advancing the simulation"""
        self.status.reset = False
        self.status.running = True

    def reset(self) -> None:
        """Reset the simulation to initial conditions"""
        self.trips = []
        self.status = SimulationStatus(
            reset = True,
            running = False,
            time = self.config.t0
        )

    def tick(self) -> None:
        """Advance the simulation by one tick"""
        self.status.time += self.config.dt

    def get_status(self) -> SimulationStatus:
        """Get current simulation status"""
        return self.status

    def get_config(self) -> SimulationConfig:
        """Get current simulation config"""
        return self.config

    def get_trips(self) -> list[Trip]:
        """Get all trips in simulation"""
        return self.trips

    def add_trip(self, trip: Trip):
        """Add trip to simulation"""
        self.trips.append(trip)

    def _simulation_loop(self):
        """Simulation thread execution loop"""
        while not self.terminate.is_set():
            if self.config.tickrate <= 0:
                time.sleep(0.1)
                continue

            time.sleep(1.0/self.config.tickrate)

            if not self.status.running:
                continue

            self.tick()

    def __del__(self):
        """Terminate the thread when the object is deleted"""
        self.terminate.set()
        self.thread.join()
