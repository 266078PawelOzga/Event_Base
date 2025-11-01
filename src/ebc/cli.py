from .mpkloader import load_stops, load_trips, load_stop_times, table_exists, drop_table
import click
import folium
import sqlite3
import os
import webbrowser
from dataclasses import dataclass
from pathlib import Path

@dataclass
class State:
    cache: Path = Path('.cache/')
    conn: sqlite3.Connection = None

@click.command()
@click.pass_context
def stops_map(ctx) -> None:
    "Show all bus stops on a map."
    cursor = ctx.obj.conn.cursor()
    cursor.execute(
        "SELECT stop_name, stop_lon, stop_lat FROM stops"
    )
    rows = cursor.fetchall()

    (_, lon, lat) = rows[0]
    m = folium.Map(location=(lat, lon))

    for (name, lon, lat) in rows:
        folium.Marker(
            location=[lat, lon],
            tooltip="Click me!",
            popup=name,
            icon=folium.Icon("red")
        ).add_to(m)

    tmpfile = ctx.obj.cache / "index.html"
    m.save(tmpfile)
    webbrowser.open('file://' + str(tmpfile.resolve()))

@click.command()
@click.pass_context
def stops(ctx) -> None:
    "Print table 'stops'"
    cursor = ctx.obj.conn.cursor()
    cursor.execute(
        "SELECT * FROM stops LIMIT 20"
    )
    for row in cursor:
        click.echo(row)

@click.command()
@click.pass_context
def trips(ctx) -> None:
    "Print table 'trips'"
    cursor = ctx.obj.conn.cursor()
    cursor.execute(
        "SELECT * FROM trips LIMIT 20"
    )
    for row in cursor:
        click.echo(row)

@click.command()
@click.pass_context
def stop_times(ctx) -> None:
    "Print table 'stop_times'"
    cursor = ctx.obj.conn.cursor()
    cursor.execute(
        "SELECT * FROM stop_times LIMIT 20"
    )
    for row in cursor:
        click.echo(row)

@click.group(
    epilog="""
You can also run: 
  poetry run python target_stop.py\n
- to see 12 bus stops with 3 different target stops and departure times after 06:05:01.
"""
)
@click.option('--dataset', type=click.Path(exists=True), help='Load MPK dataset.')
@click.pass_context
def main(ctx, dataset):
    state = State()
    if not os.path.exists(state.cache):
        os.makedirs(state.cache)

    state.conn = sqlite3.connect(state.cache / 'mpk.db')
    if dataset:
        dataset = Path(dataset)
        with state.conn as conn:
            drop_table(conn, 'stops')
            drop_table(conn, 'trips')
            drop_table(conn, 'stop_times')
            load_stops(conn, dataset / 'stops.txt')
            load_trips(conn, dataset / 'trips.txt')
            load_stop_times(conn, dataset / 'stop_times.txt')

    with state.conn as conn:
        if not (table_exists(conn, 'stops') and
                table_exists(conn, 'trips') and
                table_exists(conn, 'stop_times')):
            ctx = click.get_current_context()
            click.echo("Database tables missing. Load MPK dataset.")
            click.echo(ctx.get_help())
            ctx.exit(-1)

    ctx.obj = state
    ctx.call_on_close(state.conn.close)

main.add_command(stops_map)
main.add_command(stops)
main.add_command(trips)
main.add_command(stop_times)
