from datetime import datetime, timedelta

class Simulation:
    def __init__(self, t0: datetime = datetime.now(), dt: timedelta = timedelta(seconds=30), activate: bool = False):
        self.t0 = t0
        self.t = self.t0
        self.dt = dt
        self.active = activate

    def start(self) -> None:
        """Start advancing the simulation"""
        self.active = True

    def stop(self) -> None:
        """Stop advancing the simulation"""
        self.active = False

    def reset(self) -> None:
        """Reset the simulation to initial conditions"""
        self.stop()
        self.t = self.t0

    def tick(self) -> None:
        """Advance the simulation by one tick"""
        if not self.active:
            return
        self.t += self.dt

    def get_time(self) -> datetime:
        """Get current time in simulation"""
        return self.t
