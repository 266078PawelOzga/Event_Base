from .gui import run_gui
from .simulation_server import run_server
import threading

def run_app():
    sim_thread = threading.Thread(target=run_server, daemon=True)
    sim_thread.start()
    run_gui()

if __name__ == '__main__':
    run_app()
