from .target_stop import find_nearest_stops
from .students_pos import check_student_pos, real_student_pos
from .wro_map import show_map_with_students_and_stops
from .time_operation import display_departure_time_once, time_now_td, check_departure_time
import threading
import time
import sqlite3
import subprocess
import os, sys
from datetime import datetime
from .models import *
"""
Event: User requests to find nearest bus stops
the target_stop.py is implemented in run_search method

FSM states:
IDLE - waiting for user request
SEARCHING - searching for nearest stops
DISPLAYING - displaying results to user
WROMAP - showing map
"""

# def open_live_terminal():
#     subprocess.Popen([
#         "x-terminal-emulator",
#         "-e",
#         "bash -c 'python3 src/ebc/live_update_runner.py; exec bash'"
#     ])


# def print_current_time(stop_event):
#     while not stop_event.is_set():
#         now = time_now_td()
#         with print_lock:
#             # clear line then print current time (keeps it on single line)
#             self._vprint(f"\r\033[KCurrent time: {now}", end='', flush=True)
#         stop_event.wait(timeout=1)

# stop_event = threading.Event()

# # simple thread-safe print helper used in this module
# print_lock = threading.Lock()
# def safe_self._vprint(*args, **kwargs):
#     with print_lock:
#         self._vprint(*args, **kwargs)

# t = threading.Thread(target=print_current_time, args=(stop_event,) ,daemon=True)
# t.start()
# safe_self._vprint('Main dalej')
# t.join(timeout=5)
# safe_self._vprint('Main koniec')

class StopFinderFSM:
    def __init__(self, number_of_students = 2, verbose = False, show_map = False,
                 current_time = datetime.now(),
                 journey:Journey = None):
        self.state = 'IDLE'
        self.results = []
        self.students_position = []
        self.number_of_students = number_of_students
        self.verbose = verbose
        self.show_map = show_map
        self.current_time = current_time
        self.journey = journey
      #  self.live_terminal_started = False 

    def _vprint(self, *args, **kwargs):
        if self.verbose:
            print(*args, **kwargs)

    def on_event(self,event):

        if self.state == 'IDLE':
            if event == 'user_request':
                self._vprint("IDLE")
                if self.journey == None:
                    students_position = check_student_pos(students_count=self.number_of_students)
                else:
                    students_position = real_student_pos(self.journey.origin)
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
        show_map_with_students_and_stops(stops, self.students_position)

    def run_search(self, students_position):
        # TODO: add fining nearest stop to the destination of the journey
        #       (now it finds stops leading to the "Dworzec Główny")
        self._vprint("FSM current_time:", self.current_time)
        if self.journey == None:
            self.results = find_nearest_stops(students_pos=students_position,
                                          current_time=self.current_time) 
        else:
            self.results = find_nearest_stops(students_pos=students_position,
                                          current_time=self.current_time,
                                          target= self.journey.destination.name) 
        self.on_event("search_done")
       
    # def display_student_pos(self):
    #     safe_self._vprint("Student position in Wroclaw (random):")
    #     for i, pos in enumerate(self.students_position, start=1):
    #         safe_self._vprint(f"  Student {i}: {pos}")


    def display_find_nearest_stops(self):
     for student in self.results:
        self._vprint(f"\nStudents {student['student_id']}: {student['position']}")

        if student['nearest_stops']:
            self._vprint("  Nearest stops:")        
            for d, stop_id, stop_name, stop_lat, stop_lon in student['nearest_stops'][:3]:
                self._vprint(f"  Stop: {stop_name}, ID: {stop_id}, Distance: {d:.6f}")
        else:
            self._vprint(" In your area, the bus stop is far far away")

        if student['reachable_stops']:
            self._vprint("  Reachable stops (with routes to target):")
            for stop in student['reachable_stops']:
                self._vprint(f"    Stop: {stop['stop_name']}, ID: {stop['stop_id']}, Routes to target: {stop['routes_to_target']}")
        else:
            self._vprint("  No reachable stops with routes to target from nearest stops.")
            
        if student.get('departures'):
            self._vprint(' Next departures:')
            for dep in student.get('departures', []):
                self._vprint(f"    Stop:{dep['stop_name']}, Route {dep['route_id']}: {dep['next_departure']}")
            display_departure_time_once(student['departures'])
        else:
            self._vprint("FSM current_time:", self.current_time)
            self._vprint("  No departure times available for reachable stops.")
        
        

#Test - always run user_request first
if __name__ == '__main__':
    fsm = StopFinderFSM()
    fsm.on_event("user_request")

