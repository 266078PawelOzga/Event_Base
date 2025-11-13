from datetime import timedelta, datetime

def time_now():
    now = datetime.now()
    return now

def parse_gtfs_time(s):
    h, m, sec = map(int, s.split(":"))
    return timedelta(hours=h, minutes=m, seconds=sec)

    # """
    #     5: Find the timetable for the selected stop_id and trip_id
    # """
def check_departure_time(cursor, reachable_stops):
    #time_right_now = "06:05:01"  # HH:MM:SS
    #time_right_now_td = parse_gtfs_time(time_right_now)
    current_time = time_now().strftime("%H:%M:%S")
    time_right_now_td = parse_gtfs_time(current_time)

    departures = []

    for stop in reachable_stops:
        stop_id = stop['stop_id']
        #print(f"\nStop: {stop['stop_name']} (ID: {stop_id})")

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
            #print(f"Route {route_id}: Next departure at {next_time if next_time else 'No more today'}")
            departures.append({
                "stop_id":stop_id,
                "stop_name":stop['stop_name'],
                "route_id":route_id,
                "next_departure": next_time
            })

    return departures