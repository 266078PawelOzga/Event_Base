import folium
import webbrowser

def create_map_with_students_and_stops(stops, students):
    m = folium.Map(location=students[0], zoom_start=13)
    
    for name, lon, lat in stops:
        folium.Marker(
            location=[lat, lon],
            popup=f"Stop: {name}",
            icon=folium.Icon(color="red")
        ).add_to(m)

    for i, (lat, lon) in enumerate(students):
        folium.Marker(
            location=[lat, lon],
            popup=f"Student {i+1}",
            icon=folium.Icon(color="blue", icon="user")
        ).add_to(m)
    return m

def get_map_html(stops, students):
    """Get map as HTML string."""
    m = create_map_with_students_and_stops(stops, students)
    return m._repr_html_()  # folium map in HTML format