import sqlite3
import random
import math
from datetime import timedelta
"""
Raw Data in .txt:
- Stop: stop_id,stop_code,stop_name,stop_lat,stop_lon
- Trips: route_id,service_id,trip_id,trip_headsign,direction_id,shape_id,brigade_id,vehicle_id,variant_id
- stop_times: trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type
- stop_times: trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type

The purpose ...
of this function is to drop the 'target_stop' table from the database. Done
Generate the position (latitude and longitude) of student.
Find the nearest stop_id to the student position.
Check if any bus route (trip_id) serves that stop_id. if not find the next nearest stop_id.

Input: Student localization & data.time
Output: stop_id, trip_id, departure_time
"""

"""
    1 Find all bus stop with name containing eg. "Dworzec Główny"
"""
conn = sqlite3.connect('.cache/mpk.db')
cursor = conn.cursor()
# CHECK IF TARGET EXISTS !
target = "Dworzec Główny" # ! the name of bus_stop without knowledge about its stop_id !
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


stops_close_to_student = []
for stop_id, stop_name, stop_lat, stop_lon in rows:
    d = distance(student_lat, student_lon, stop_lat, stop_lon)
    stops_close_to_student.append((d, stop_id, stop_name, stop_lat, stop_lon))

stops_close_to_student.sort(key=lambda x: x[0])
nearest_stops = stops_close_to_student[:30]  # 3 nearest stops

for d, stop_id, stop_name, stop_lat, stop_lon in nearest_stops:
    print(f"Stop: {stop_name}, ID: {stop_id}, Distance: {d:.6f}")

conn.close()
"""
OUTPUT example: 
    Stop: Dworska, ID: 4066, Distance: 0.000472
    Stop: Dworska, ID: 4065, Distance: 0.000553
    Stop: Dworska, ID: 113, Distance: 0.000623
"""
# conn = sqlite3.connect('.cache/mpk.db')   # debugg table info
# cursor = conn.cursor()
# cursor.execute("PRAGMA table_info(stop_times)")
# for col in cursor.fetchall():
#     print(col)
# conn.close()
"""
     4: Check if any bus route (trip_id) serves that stop_id. if not find the next nearest stop_id.
"""
conn = sqlite3.connect('.cache/mpk.db')
cursor = conn.cursor()
reachable_stops = []
count_reachable_stops = 0
cursor.execute("SELECT stop_id FROM stops WHERE stop_name LIKE ?", (f"%{target}%",))
target_stops = [row[0] for row in cursor.fetchall()]

for d, stop_id, stop_name, stop_lat, stop_lon in nearest_stops:
    lines_to_target = set()
    for target_stop_id in target_stops:
        
        cursor.execute("SELECT trip_id, stop_sequence FROM stop_times WHERE stop_id = ?", (stop_id,))
        start_trips = cursor.fetchall()
        start_dict = {trip: seq for trip, seq in start_trips}
        # Eg. 3_1568416 : 5, # line 3, trip 15684168, this bus_stop is 5-th in the route
        cursor.execute("SELECT trip_id, stop_sequence FROM stop_times WHERE stop_id = ?", (target_stop_id,))
        end_trips = cursor.fetchall()
        # check if any trip_id serves both stops in correct order
        valid_trip_ids = set()
        for trip_id, end_seq in end_trips:
            if trip_id in start_dict and start_dict[trip_id] < end_seq:
                valid_trip_ids.add(trip_id)
            #^-- This checks whether a given trip (trip_id) passes through the starting stop!
            #^-- And whether the starting stop appears earlier on the route than the destination stop!
        # v--check which route_id serves valid_trip_ids
        if valid_trip_ids:
            cursor.execute(
                "SELECT DISTINCT route_id FROM trips WHERE trip_id IN ({seq})".format(
                    seq=','.join('?'*len(valid_trip_ids))
                ), tuple(valid_trip_ids)
            )
            for row in cursor.fetchall():
                lines_to_target.add(row[0]) # which route_id serves this trip_id, eg. route 3, 10, 14, etc.

    if lines_to_target:
        reachable_stops.append({
            "stop_id": stop_id,
            "stop_name": stop_name,
            "stop_lat": stop_lat,
            "stop_lon": stop_lon,
            "routes_to_target": list(lines_to_target)
        })
        count_reachable_stops += 1

    if count_reachable_stops >= 3:
        break  

for stop in reachable_stops:
    print(f"Stop: {stop['stop_name']}, ID: {stop['stop_id']}, Routes to target: {stop['routes_to_target']}")
conn.close()
"""
    5: Find the timetable for the selected stop_id and trip_id
"""
def parse_gtfs_time(s):
    h, m, sec = map(int, s.split(":"))
    return timedelta(hours=h, minutes=m, seconds=sec)

time_right_now = "06:05:01"  # HH:MM:SS
time_right_now_td = parse_gtfs_time(time_right_now)

conn = sqlite3.connect('.cache/mpk.db')
cursor = conn.cursor()

for stop in reachable_stops:
    stop_id = stop['stop_id']
    print(f"\nStop: {stop['stop_name']} (ID: {stop_id})")

    for route_id in stop["routes_to_target"]:
        cursor.execute(
            "SELECT trip_id FROM trips WHERE route_id = ?", (route_id,)
        )
        trip_ids = [row[0] for row in cursor.fetchall()]
        if not trip_ids:
            continue

        cursor.execute(
            "SELECT departure_time FROM stop_times WHERE stop_id = ? AND trip_id IN ({seq}) ORDER BY departure_time".format(
                seq=','.join('?'*len(trip_ids))
            ),
            (stop_id, *trip_ids)
        )
        times = [row[0] for row in cursor.fetchall()]

        # find the next departure time after 'time_right_now'
        next_time = next((t for t in times if parse_gtfs_time(t) >= time_right_now_td), None)
        print(f"Route {route_id}: Next departure at {next_time if next_time else 'No more today'}")
