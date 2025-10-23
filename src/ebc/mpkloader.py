import pandas as pd

def drop_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

def table_exists(conn, table_name):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT name FROM sqlite_master
        WHERE type='table' AND name=?
    """, (table_name,))
    return cursor.fetchone() is not None


def load_stops(conn, filepath):
    """Load stops from a csv file into the db"""
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stops (
        stop_id INTEGER PRIMARY KEY,
        stop_code INTEGER,
        stop_name TEXT,
        stop_lat REAL,
        stop_lon REAL
    )
    ''')

    df = pd.read_csv(filepath,
                     sep=',',
                     header=0)

    stops = df[[
        'stop_id', 'stop_code', 'stop_name','stop_lat', 'stop_lon'
    ]].itertuples(index=False, name=None)

    cursor.executemany('''
    INSERT OR REPLACE INTO stops
        (stop_id, stop_code, stop_name, stop_lat, stop_lon)
    VALUES (?, ?, ?, ?, ?)
    ''', stops)


def load_trips(conn, filepath):
    """Load trips from a csv file into the db"""
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trips (
        trip_id TEXT PRIMARY KEY,
        trip_headsign TEXT,
        route_id TEXT
    )
    ''')

    df = pd.read_csv(filepath,
                     sep=',',
                     header=0)

    trips = df[[
        'trip_id', 'trip_headsign', 'route_id'
    ]].itertuples(index=False, name=None)

    cursor.executemany('''
    INSERT OR REPLACE INTO trips
        (trip_id, trip_headsign, route_id)
    VALUES (?, ?, ?)
    ''', trips)


def load_stop_times(conn, filepath):
    """Load stop times from a csv file into the db"""
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stop_times (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trip_id TEXT,
        arrival_time TEXT,
        departure_time TEXT,
        stop_id INTEGER
    )
    ''')

    df = pd.read_csv(filepath,
                     sep=',',
                     header=0)

    stop_times = df[[
        'trip_id','arrival_time','departure_time','stop_id'
    ]].itertuples(index=False, name=None)

    cursor.executemany('''
    INSERT INTO stop_times
        (trip_id,arrival_time,departure_time,stop_id)
    VALUES (?, ?, ?, ?)
    ''', stop_times)
