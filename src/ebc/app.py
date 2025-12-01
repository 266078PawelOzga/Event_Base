from .gui import run_gui
from .simulation_server import run_server
import threading
from .events_simulator import EventSimulator

def run_app():
    sim_thread = threading.Thread(target=run_server, daemon=True)
    sim_thread.start()
    events_simulator = EventSimulator(verbose=True)
    event_sim_thread = threading.Thread(
        target=events_simulator.event_simulation_loop, daemon=True)
    event_sim_thread.start()
    run_gui()


if __name__ == '__main__':
    run_app()
