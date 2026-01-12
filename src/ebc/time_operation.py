from datetime import timedelta, datetime
import time
import math

def time_now_td():
    now = datetime.now() #2025-11-13 16:15:32
    return timedelta(hours=now.hour, minutes=now.minute, seconds=now.second)

def parse_gtfs_time(s):
    h, m, sec = map(int, s.split(":")) #['16', '15', '32']
    return timedelta(hours=h, minutes=m, seconds=sec)

    # """
    #     5: Find the timetable for the selected stop_id and trip_id
    # """
def check_departure_time(cursor, reachable_stops):
    """
    """
    current_time_td = time_now_td()  
    # debug/test values removed - do not print here to avoid spamming stdout
    departures = []
    for stop in reachable_stops:
        stop_id = stop['stop_id']
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
            next_time = next((t for t in times if parse_gtfs_time(t) >= current_time_td), None)
            departures.append({
                "stop_id": stop_id,
                "stop_name": stop['stop_name'],
                "route_id": route_id,
                "next_departure": next_time
            })
    return departures


def display_departure_time_once(departures, expired_seconds=5):
    now_td = time_now_td()
    now_datetime = datetime.now()
    output_lines = []
    for dep in departures:
        stop_name = dep['stop_name']
        route_id = dep['route_id']
        departure_td = parse_gtfs_time(dep['next_departure'])
        time_diff = departure_td - now_td
        total_sec = time_diff.total_seconds()
        if total_sec > 0:
            hours, remainder = divmod(total_sec, 3600)
            minutes, seconds = divmod(remainder, 60)
            line = f"{stop_name} - Route {route_id}: {int(hours):02}h {int(minutes):02}m {int(seconds):02}s left"
        elif -60 < total_sec <= 0:
            line = f"{stop_name} - Route {route_id}: already departed"
        else:
            line = f"{stop_name} - Route {route_id}: departed long ago"
        output_lines.append(line)
    for line in output_lines:
        print("     ",line.ljust(60))


def walking_time_seconds(lat1, lon1, lat2, lon2, speed_m_s=1.4):
    R = 6371000  # m
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    distance = 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return distance / speed_m_s

def select_fastest_departure(student_pos, reachable_stops, departures):
    now_td = time_now_td()
    best = None
    best_time = None
    for dep in departures:
        if not dep["next_departure"]:
            continue
        # znajdź dane przystanku
        stop = next(
            (s for s in reachable_stops if s["stop_id"] == dep["stop_id"]),
            None
        )
        if not stop:
            continue
        # czas dojścia studenta
        walk_sec = walking_time_seconds(
            student_pos[0], student_pos[1],
            stop["stop_lat"], stop["stop_lon"]
        )
        arrival_td = now_td + timedelta(seconds=walk_sec)
        dep_td = parse_gtfs_time(dep["next_departure"])
        # student nie zdąży
        if arrival_td > dep_td:
            continue
        total_wait = dep_td - now_td
        if best is None or total_wait < best_time:
            best = {
                **dep,
                "walk_seconds": walk_sec,
                "arrival_at_stop": arrival_td,
                "total_wait": total_wait
            }
            best_time = total_wait
    return best

