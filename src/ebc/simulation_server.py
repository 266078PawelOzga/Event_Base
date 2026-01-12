from .simulation import Simulation
import threading
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn
import time

app = FastAPI()
sim = Simulation(activate=True)
tickrate = 1.0

def simulation_loop():
    """Advance the simulation in a variable frequency loop"""
    global tickrate
    while True:
        if tickrate > 0:
            sim.tick()
            time.sleep(1.0/tickrate)

def run_server():
    """Run the simulation thread and then start the API server"""
    sim_thread = threading.Thread(target=simulation_loop, daemon=True)
    sim_thread.start()
    uvicorn.run(app, host="0.0.0.0", port=8000,
                access_log=False,
                )

@app.get("/tickrate")
def get_tickrate():
    global tickrate
    return {"tickrate": tickrate}

@app.post("/tickrate")
def set_tickrate(value: float):
    global tickrate
    tickrate = value
    return get_tickrate()

@app.post("/start")
def start_simulation():
    sim.start()
    return {"status": "Simulation started"}

@app.post("/stop")
def stop_simulation():
    sim.stop()
    return {"status": "Simulation stopped"}

@app.post("/reset")
def reset_simulation():
    sim.reset()
    return {"status": "Simulation reset"}

@app.post("/tick")
def tick_simulation():
    sim.tick()
    return {"status": "Simulation ticked"}

@app.get("/time")
def get_simulation_time():
    return {"current_time": sim.get_time()}

@app.get("/map", response_class=HTMLResponse)
def get_simulation_map():
    map_html = sim.generate_map().get_root().render()
    return map_html
