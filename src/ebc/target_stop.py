import sqlite3
import sys
import random
import math
from datetime import timedelta, datetime
from students_pos import check_student_pos 
from time_operation import time_now_td, check_departure_time, display_departure_time_once

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

""" 1 Find all bus stop with name containing eg. "Dworzec Główny" """
def distance(lat1, lon1, lat2, lon2):
        return math.sqrt((lat1 - lat2)**2 + (lon1 - lon2)**2)   # Euclidean distance
    

def find_target_stops(cursor, target):
    # CHECK IF TARGET EXISTS !
    #target = "Dworzec Główny" # ! the name of bus_stop without knowledge about its stop_id !
    cursor.execute("SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops WHERE stop_name LIKE ?", (f"%{target}%",))
    rows = cursor.fetchall() # if not, fetchall returns empty list
    # for row in rows:
    #    print(row)
    if not rows:
        # No matching target stops found — raise to let caller decide how to handle
        raise ValueError(f"No target stops found matching '{target}'")
    return rows

    
def find_nearest_to_student(cursor, student_lat, student_lon, max_results=12):
    """ 3: Find nearest stop_id to student position """
    cursor.execute("SELECT stop_id, stop_name, stop_lat, stop_lon FROM stops")
    all_stops = cursor.fetchall()

    stops_close_to_student = []
    
    for stop_id, stop_name, stop_lat, stop_lon in all_stops:
        d = distance(student_lat, student_lon, stop_lat, stop_lon)
        stops_close_to_student.append((d, stop_id, stop_name, stop_lat, stop_lon))

    stops_close_to_student.sort(key=lambda x: x[0])
    nearest_stops = stops_close_to_student[:max_results]  
    return nearest_stops


    # """
    #     4: Check if any bus route (trip_id) serves that stop_id. if not find the next nearest stop_id.
    # """
def check_trip_id_for_stops(cursor, nearest_stops, target_stops, max_results=3):
    reachable_stops = []
    count_reachable_stops = 0
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

        if count_reachable_stops >= max_results:
            break  

    return reachable_stops


def find_nearest_stops( target="Dworzec Główny", students_pos=None, max_results=12):
    conn = sqlite3.connect('.cache/mpk.db')
    cursor = conn.cursor()
    # ensure target stops exist (find_target_stops will raise if none)
    try:
        target_rows = find_target_stops(cursor, target)
    except ValueError as e:
        # Abort program with explanatory message
        print(f"Error: {e}")
        conn.close()
        sys.exit(1)
    result = []
     
    # extract stop ids for the target stops
    target_stops = [row[0] for row in target_rows]
    
    for idx, (student_lat, student_lon) in enumerate(students_pos):
        nearest_stops = find_nearest_to_student(cursor, student_lat, student_lon, max_results)
        reachable_stops = check_trip_id_for_stops(cursor, nearest_stops, target_stops)
        departure_times = check_departure_time(cursor, reachable_stops)

        result.append({
            "student_id": idx +1,
            "position": (student_lat, student_lon),
            "nearest_stops": nearest_stops,
            "reachable_stops": reachable_stops,
            "departures": departure_times
        })

    conn.close()
    return result


if __name__ == "__main__":
        find_nearest_stops()