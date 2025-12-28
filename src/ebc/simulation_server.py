from .simulation import Simulation
from .models import *
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
import uvicorn
import time
import folium
from folium.plugins import AntPath

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

@app.get("/trips")
def get_trips() -> list[Trip]:
    return sim.get_trips()

@app.post("/trip")
def add_trip(trip: Trip):
    import geocoder

    if trip.origin_coord is None:
        o = geocoder.arcgis(trip.origin + "Wrocław")
        if o.ok:
            trip.origin_coord = tuple(o.latlng)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Can't locate origin"
            )

    if trip.destination_coord is None:
        d = geocoder.arcgis(trip.destination + "Wrocław")
        if d.ok:
            trip.destination_coord = tuple(d.latlng)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Can't locate destination"
            )

    sim.add_trip(trip)

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
    m = folium.Map(location=[51.107778, 17.038611], zoom_start=10)

    for trip in sim.get_trips():
        folium.Marker(
            location=trip.origin_coord,
            popup=folium.Popup(f"Origin: {trip.origin}", parse_html=True, max_width=100),
        ).add_to(m)
        folium.Marker(
            location=trip.destination_coord,
            popup=folium.Popup(f"Destination: {trip.destination}", parse_html=True, max_width=100),
        ).add_to(m)
        AntPath(
            locations=[trip.origin_coord, trip.destination_coord]
        ).add_to(m)

    return m.get_root().render()
