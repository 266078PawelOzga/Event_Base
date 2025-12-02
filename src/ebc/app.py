from .gui import run_gui
from .simulation_server import run_server
import threading
from .events_simulator import EventSimulator
import datetime

def run_app():
    sim_thread = threading.Thread(target=run_server, daemon=True)
    sim_thread.start()
    #temporary for testing purpouses
    current_time=datetime.datetime.combine(
        datetime.date.today(), datetime.time(15, 24, 00))
    
    events_simulator = EventSimulator(verbose=True, number_of_students=4,
                                      current_time=current_time)
    event_sim_thread = threading.Thread(
        target=events_simulator.event_simulation_loop, daemon=True)
    event_sim_thread.start()
    run_gui()


if __name__ == '__main__':
    run_app()
