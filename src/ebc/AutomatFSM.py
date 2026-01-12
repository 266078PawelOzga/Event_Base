from .target_stop import find_nearest_stops
from .students_pos import check_student_pos
from .wro_map import create_map_with_students_and_stops
from .time_operation import display_departure_time_once, time_now_td, check_departure_time, select_fastest_departure
import threading
import time
import sqlite3
import subprocess
import os, sys
"""
Event: User requests to find nearest bus stops
the target_stop.py is implemented in run_search method

FSM states:
IDLE - waiting for user request
SEARCHING - searching for nearest stops
DISPLAYING - displaying results to user
WROMAP - showing map
"""

class StopFinderFSM:
    def __init__(self, verbose = False, show_map = False):
        self.state = 'IDLE'
        self.results = []
        self.students_position = []
        self.verbose = verbose
        self.show_map = show_map
      #  self.live_terminal_started = False 

    def _vprint(self, *args, **kwargs):
        if self.verbose:
            print(*args, **kwargs)

    def on_event(self,event):

        if self.state == 'IDLE':
            if event == 'user_request':
                self._vprint("IDLE")
                students_position = check_student_pos()
                self.students_position = students_position
                self.state = 'SEARCHING'
                self.event_happend()
                self.run_search(students_position)

        elif self.state == 'SEARCHING':
            if event =='search_done':
                self._vprint('SEARCHING done')
                self.state = 'DISPLAYING'


        elif self.state == 'DISPLAYING':
            self._vprint("\nDISPLAYING\n")
            self.display_find_nearest_stops()
            #self.display_student_pos()
            self.state = 'WROMAP'
            self.on_event("show")


        elif self.state == 'WROMAP':
            if event == 'show':
                self._vprint('Loading map...')
                self._vprint('OFF')
                if self.show_map:
                    self.draw_map()
                self.state = 'IDLE'

    def event_happend(self): # testing empty event
        self._vprint(" Event_happened: No event")
        self._vprint(" solution found: None")
        self.on_event("search_done")


    def draw_map(self):
        stops = []
        for student in self.results:
            for stop in student['reachable_stops']:
                stops.append((stop['stop_name'], stop['stop_lon'], stop['stop_lat']))
        create_map_with_students_and_stops(stops, self.students_position)

    def run_search(self, students_position):
        self.results = find_nearest_stops(students_pos=students_position) 
        self.on_event("search_done")
       
    # def display_student_pos(self):
    #     safe_self._vprint("Student position in Wroclaw (random):")
    #     for i, pos in enumerate(self.students_position, start=1):
    #         safe_self._vprint(f"  Student {i}: {pos}")


    def display_find_nearest_stops(self):
        for student in self.results:
            self._vprint(f"\nStudent {student['student_id']}: {student['position']}")

            if student['nearest_stops']:
                self._vprint("  Nearest stops:")        
                for d, stop_id, stop_name, stop_lat, stop_lon in student['nearest_stops'][:3]:
                    self._vprint(f"    Stop: {stop_name}, ID: {stop_id}, Distance: {d:.6f}")
            else:
                self._vprint("  In your area, the bus stop is far far away")

            if student['reachable_stops']:
                self._vprint("  Reachable stops (with routes to target):")
                for stop in student['reachable_stops']:
                    self._vprint(f"    Stop: {stop['stop_name']}, ID: {stop['stop_id']}, Routes to target: {stop['routes_to_target']}")
            else:
                self._vprint("  No reachable stops with routes to target from nearest stops.")
                
            if student.get('departures'):
                self._vprint('  Next departures:')
                for dep in student['departures']:
                    self._vprint(f"    Stop:{dep['stop_name']}, Route {dep['route_id']}: {dep['next_departure']}")
                display_departure_time_once(student['departures'])
            else:
                self._vprint("  No departure times available for reachable stops.")

            # --- dodatkowo wyświetlamy best_option ---
            best = student.get('best_option')
            if best:
                self._vprint("\n  Best option (fastest bus student can catch):")
                self._vprint(f"    Stop: {best['stop_name']} (ID: {best['stop_id']})")
                self._vprint(f"    Route: {best['route_id']}")
                self._vprint(f"    Next departure: {best['next_departure']}")
                self._vprint(f"    Walk time to stop: {int(best['walk_seconds'])} s")
                # total_wait to timedelta, zamieniamy na minuty i sekundy
                total_sec = best['total_wait'].total_seconds()
                minutes, seconds = divmod(total_sec, 60)
                self._vprint(f"    Time until departure: {int(minutes)}m {int(seconds)}s")
            else:
                self._vprint("  Best option: no reachable bus available.")

#Test - always run user_request first
if __name__ == '__main__':
    fsm = StopFinderFSM(verbose=True)
    fsm.on_event("user_request")

