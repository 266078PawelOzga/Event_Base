from .simulation import Simulation
from .models import *
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, status
from fastapi import BackgroundTasks
from fastapi.responses import HTMLResponse
import uvicorn
import time
import folium
from folium.plugins import AntPath

app = FastAPI()
sim = Simulation(config=SimulationConfig(), run=False)

def run_server():
    """Start the API server"""
    uvicorn.run(app, host="0.0.0.0", port=8000, access_log = False)

@app.get("/status")
def get_simulation_status() -> SimulationStatus:
    return sim.status

@app.get("/config")
def get_simulation_config() -> SimulationConfig:
    return sim.config

@app.post("/config")
def set_simulation_config(config: SimulationConfig):
    sim.config = config

@app.post("/tickrate")
def set_tickrate(value: float):
    sim.config.tickrate = value
    return {"tickrate": sim.config.tickrate}

@app.post("/event")
def set_tickrate(journey_id: int, event: str):
    for journey in sim.status.journeys:
        if journey.id == journey_id:
            journey.events.append(event)
            journey.log_message(event)

@app.post("/journey")
def add_journey(journey: Journey, background_tasks: BackgroundTasks):
    import geocoder
    journey.model_validate(journey)
    print(journey)

    if journey.origin.coord is None:
        o = geocoder.arcgis(journey.origin.name + " Wrocław")
        if o.ok:
            journey.origin.coord = Coordinates(lat=o.latlng[0], lon=o.latlng[1])
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Can't locate origin"
            )

    if journey.origin.arrival is None:
        journey.origin.arrival = sim.status.time

    if journey.destination.coord is None:
        d = geocoder.arcgis(journey.destination.name + " Wrocław")
        if d.ok:
            journey.destination.coord = Coordinates(lat=d.latlng[0], lon=d.latlng[1])
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Can't locate destination"
            )

    if journey.current_position is None:
        journey.current_position = journey.origin.coord

    if journey.current_time is None:
        journey.current_time = sim.status.time

    # Asynchronious adding - the trip is not visible instantly, only after adding
    # the next trip
    # background_tasks.add_task(sim.add_journey_and_automata, journey)
    # Sychronious adding - the trip is visible at the right time,
    # but the app and simulation are frozen for the time of adding
    sim.add_journey_and_automata(journey)

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

    position_markers = {}

    for journey in sim.status.journeys:
        folium.Marker(
            location=journey.origin.coord.tuple(),
            popup=folium.Popup(f"Origin: {journey.origin.name}", parse_html=True, max_width=100),
        ).add_to(m)
        folium.Marker(
            location=journey.destination.coord.tuple(),
            popup=folium.Popup(f"Destination: {journey.destination.name}", parse_html=True, max_width=100),
        ).add_to(m)

        AntPath(
            locations=[loc.coord.tuple() for loc in journey.locations]
        ).add_to(m)

        position_marker = folium.CircleMarker(location = journey.current_position.tuple(),
                            radius = 5,
                            color = 'blue',
                            fill = True
                            )

        position_markers[journey.id] = position_marker
        position_marker.add_to(m)


    kw = {"prefix": "fa", "color": "red", "icon": "burst"}
    for crash in sim.status.crashes:
        folium.Marker(location = crash[0].tuple(),
                      icon=folium.Icon(**kw),
                      popup=folium.Popup(f"Crash of journey from {crash[1]} to {crash[2]}")
                      ).add_to(m)

    marker_script = "var position_markers = {};\n"
    for key, marker in position_markers.items():
        marker_script += f"position_markers['journey_{key}'] = '{marker.get_name()}';\n"

    m.get_root().html.add_child(folium.Element(f"<script>{marker_script}</script>"))

    return m.get_root().render()
