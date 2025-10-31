import sqlite3
import random
import math
"""
The purpose of this function is to drop the 'target_stop' table from the database. Done
Generate the position (latitude and longitude) of student.
Find the nearest stop_id to the student position.
Check if any bus route (trip_id) serves that stop_id. if not find the next nearest stop_id.
"""
"""
    1 Find all bus stop with name containing eg. "Dworzec Główny"
"""
conn = sqlite3.connect('.cache/mpk.db')
cursor = conn.cursor()

target = "Dworzec Główny"
cursor.execute("SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops WHERE stop_name LIKE ?", (f"%{target}%",))
rows = cursor.fetchall() # if not, fetchall returns empty list
for row in rows:
    print(row)
conn.close()
"""
poetry run python src/ebc/target_stop.py
OUTPUT:
    (5428, 'Dworzec Główny (MDK)', 51.10071395, 17.03607078)
    (5437, 'Dworzec Główny (MDK)', 51.10072906, 17.03607078)
"""

"""
    2: Generate Student position
"""
student_lat = random.uniform(51.05, 51.15)
student_lon = random.uniform(16.85, 17.05)
print("Student position in Wroclaw (random):", (student_lat, student_lon))

"""
    3: Find nearest stop_id to student position
"""
def distance(lat1, lon1, lat2, lon2):
    return math.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2)   # Euclidean distance
conn = sqlite3.connect('.cache/mpk.db')
cursor = conn.cursor()
cursor.execute("SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops")
rows = cursor.fetchall()

closest_stop = None
min_dist = float('inf')

for stop_id, stop_name ,stop_lat, stop_lon in rows:
    d = distance(student_lat, student_lon, stop_lat, stop_lon)
    if d < min_dist:
        min_dist = d
        closest_stop = (stop_id, stop_name, stop_lat, stop_lon)
print("The nearest bus stop:", closest_stop)
conn.close()
"""
OUTPUT: 
    The nearest bus stop: (4485, 'Smolec - Wiśniowa', 51.088342, 16.908163)
"""