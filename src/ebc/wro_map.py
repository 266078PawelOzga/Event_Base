import folium
import webbrowser

def show_map_with_students_and_stops(stops, students):
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

    m.save("students_stops_map.html")
    webbrowser.open("students_stops_map.html")