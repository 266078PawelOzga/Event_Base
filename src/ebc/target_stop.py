import os
import sqlite3
import sys
import random
import math
from math import radians, cos, sin, asin, sqrt
import logging
import time
from datetime import timedelta, datetime
from .students_pos import check_student_pos 
from .time_operation import select_fastest_departure, time_now_td, check_departure_time, display_departure_time_once
from ebc import students_pos
logger = logging.getLogger(__name__)
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
DB_PATH = os.path.join(PROJECT_ROOT, ".cache", "mpk.db")

conn = sqlite3.connect(DB_PATH)

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

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    c = 2*asin(sqrt(a))
    return R * c


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
    logger.debug(f"check_trip_id_for_stops: {len(nearest_stops)} nearest stops, {len(target_stops)} target stops")
    
    start = time.time()
    reachable_stops = []
    count_reachable_stops = 0
    
    # Pre-load all data we need - single pass through database
    logger.debug("Loading stop_times and trips data...")
    
    # Extract stop_ids from nearest_stops: (distance, stop_id, stop_name, stop_lat, stop_lon)
    all_stops = [stop_id for d, stop_id, _, _, _ in nearest_stops] + target_stops
    placeholders = ','.join('?' * len(all_stops))
    
    cursor.execute(
        f"SELECT trip_id, stop_id, stop_sequence FROM stop_times WHERE stop_id IN ({placeholders})",
        all_stops
    )
    stop_times_data = cursor.fetchall()
    logger.debug(f"Loaded {len(stop_times_data)} stop_times records")
    
    # Build maps in memory
    stop_times_map = {}  # (stop_id, trip_id) -> stop_sequence
    trips_set = set()
    for trip_id, stop_id, stop_seq in stop_times_data:
        stop_times_map[(stop_id, trip_id)] = stop_seq
        trips_set.add(trip_id)
    
    # Get all routes for these trips in one query
    if trips_set:
        placeholders_trips = ','.join('?' * len(trips_set))
        cursor.execute(
            f"SELECT trip_id, route_id FROM trips WHERE trip_id IN ({placeholders_trips})",
            list(trips_set)
        )
        trip_route_map = {trip_id: route_id for trip_id, route_id in cursor.fetchall()}
    else:
        trip_route_map = {}
    
    logger.debug(f"Loaded {len(trip_route_map)} trip->route mappings")
    
    # Now process nearest stops
    for d, stop_id, stop_name, stop_lat, stop_lon in nearest_stops:
        lines_to_target = set()
        
        # Find trips from this stop
        trips_from_start = {trip_id for (sid, trip_id), seq in stop_times_map.items() if sid == stop_id}
        
        if not trips_from_start:
            continue
        
        # Check each target stop
        for target_stop_id in target_stops:
            trips_to_target = {trip_id for (sid, trip_id), seq in stop_times_map.items() if sid == target_stop_id}
            
            # Find common trips that serve both in correct order
            for trip_id in trips_from_start & trips_to_target:
                start_seq = stop_times_map[(stop_id, trip_id)]
                end_seq = stop_times_map[(target_stop_id, trip_id)]
                
                if start_seq < end_seq and trip_id in trip_route_map:
                    lines_to_target.add(trip_route_map[trip_id])
        
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

    elapsed = time.time() - start
    logger.debug(f"check_trip_id_for_stops: found {len(reachable_stops)} reachable stops in {elapsed:.3f}s")
    return reachable_stops


def find_nearest_stops( target="Dworzec Główny", students_pos=None, max_results=12):
    logger.info(f"find_nearest_stops: target='{target}', {len(students_pos)} students")
    start_total = time.time()
    
    conn = sqlite3.connect(DB_PATH)
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
    logger.debug(f"Target stops found: {target_stops}")
    
    for idx, (student_lat, student_lon) in enumerate(students_pos):
        logger.debug(f"Processing student {idx+1}...")
        # nearest bus stops ---
        start = time.time()
        nearest_stops = find_nearest_to_student(cursor, student_lat, student_lon, max_results)
        elapsed_nearest = time.time() - start
        logger.debug(f"Student {idx+1}: found {len(nearest_stops)} nearest stops in {elapsed_nearest:.3f}s")
        # reachable stops ---
        start = time.time()
        reachable_stops = check_trip_id_for_stops(cursor, nearest_stops, target_stops)
        elapsed_reachable = time.time() - start
        logger.debug(f"Student {idx+1}: {len(reachable_stops)} reachable stops found in {elapsed_reachable:.3f}s")
        # --- get departure times ---
        start = time.time()
        departure_times = check_departure_time(cursor, reachable_stops)
        elapsed_departures = time.time() - start
        logger.debug(f"Student {idx+1}: departures checked in {elapsed_departures:.3f}s")
        # ---best choice ---
        best_option = select_fastest_departure(
            student_pos=(student_lat, student_lon),
            reachable_stops=reachable_stops,
            departures=departure_times
        )
        # --- output ---
        result.append({
            "student_id": idx + 1,
            "position": (student_lat, student_lon),
            "nearest_stops": nearest_stops,
            "reachable_stops": reachable_stops,
            "departures": departure_times,
            "best_option": best_option 
        })
    conn.close()
    elapsed_total = time.time() - start_total
    logger.info(f"find_nearest_stops: complete, {len(result)} students processed in {elapsed_total:.3f}s total")
    return result

if __name__ == "__main__":
    print(">>> AutomatFSM START <<<")
    # generujemy pozycje studentów (2 przykładowe)
    students_positions = check_student_pos(students_count=2)
    # przekazujemy je do funkcji
    find_nearest_stops(students_pos=students_positions)
    print(">>> AutomatFSM END <<<")