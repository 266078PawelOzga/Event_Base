import folium
import webbrowser
import logging

logger = logging.getLogger(__name__)

def create_map_with_students_and_stops(stops, students, start_stop=None, end_stop=None,
                                       start_coords=None, end_coords=None):
    """
    Create map with students and stops.
    
    Args:
        stops: list of (name, lon, lat) for bus stops
        students: list of (lat, lon) for student positions
        start_stop: name of start stop
        end_stop: name of end stop
        start_coords: (lat, lon) for start stop
        end_coords: (lat, lon) for end stop
    """
    m = folium.Map(location=students[0], zoom_start=13)
    
    logger.debug(f"Map: stops={len(stops)}, start='{start_stop}', end='{end_stop}'")
    logger.debug(f"Map: start_coords={start_coords}, end_coords={end_coords}")
    
    # Add START stop marker (if coordinates provided)
    if start_coords and start_stop:
        lat, lon = start_coords
        logger.debug(f"Adding START marker '{start_stop}' at ({lat}, {lon})")
        folium.Marker(
            location=[lat, lon],
            popup=f"<b>📍 START: {start_stop}</b>",
            icon=folium.Icon(color="green", icon="play", prefix="fa", icon_color="white")
        ).add_to(m)
    
    # Add END stop marker (if coordinates provided)
    if end_coords and end_stop:
        lat, lon = end_coords
        logger.debug(f"Adding END marker '{end_stop}' at ({lat}, {lon})")
        folium.Marker(
            location=[lat, lon],
            popup=f"<b>🎯 END: {end_stop}</b>",
            icon=folium.Icon(color="red", icon="stop", prefix="fa", icon_color="white")
        ).add_to(m)
    
    # Add reachable bus stops - orange
    for name, lon, lat in stops:
        folium.Marker(
            location=[lat, lon],
            popup=f"🚌 Stop: {name}",
            icon=folium.Icon(color="orange", icon="bus", prefix="fa")
        ).add_to(m)

    # Add students - blue markers
    for i, (lat, lon) in enumerate(students):
        folium.Marker(
            location=[lat, lon],
            popup=f"👤 Student {i+1}",
            icon=folium.Icon(color="blue", icon="user", prefix="fa")
        ).add_to(m)
    
    # Draw lines connecting students to stops (optional - shows connectivity)
    for lat, lon in students:
        for name, stop_lon, stop_lat in stops:
            # Draw light line from student to each reachable stop
            folium.PolyLine(
                locations=[[lat, lon], [stop_lat, stop_lon]],
                color="gray",
                weight=0.5,
                opacity=0.3
            ).add_to(m)
    
    return m

def get_map_html(stops, students, start_stop=None, end_stop=None,
                  start_coords=None, end_coords=None):
    """Get map as HTML string."""
    m = create_map_with_students_and_stops(stops, students, start_stop, end_stop,
                                           start_coords, end_coords)
    return m._repr_html_()  # folium map in HTML format