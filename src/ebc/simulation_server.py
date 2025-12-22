from .simulation import Simulation
from .models import SimulationStatus, SimulationConfig
from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn
import time
import folium

app = FastAPI()
sim = Simulation(config=SimulationConfig(), run=False)

def run_server():
    """Start the API server"""
    uvicorn.run(app, host="0.0.0.0", port=8000)

@app.get("/status")
def get_simulation_status() -> SimulationStatus:
    return sim.get_status()

@app.get("/config")
def get_simulation_config() -> SimulationConfig:
    return sim.get_config()

@app.post("/config")
def set_simulation_config(config: SimulationConfig):
    sim.config = config

@app.post("/tickrate")
def set_tickrate(value: float):
    sim.config.tickrate = value
    return {"tickrate": sim.config.tickrate}

@app.post("/resume")
def resume_simulation():
    sim.resume()
    return {"status": "Simulation resumed"}

@app.post("/pause")
def pause_simulation():
    sim.pause()
    return {"status": "Simulation paused"}

@app.post("/reset")
def reset_simulation():
    sim.reset()
    return {"status": "Simulation reset"}

@app.post("/tick")
def tick_simulation():
    sim.tick()
    return {"status": "Simulation ticked"}

@app.get("/map", response_class=HTMLResponse)
def get_simulation_map():
    "Generate a folium map displaying current simulation status"
    m = folium.Map(location=[51, 17], zoom_start=10)
    return m.get_root().render()
