from target_stop import find_nearest_stops
from students_pos import check_student_pos
from wro_map import show_map_with_students_and_stops
from time_operation import display_departure_time_once, time_now_td, check_departure_time
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
#             print(f"\r\033[KCurrent time: {now}", end='', flush=True)
#         stop_event.wait(timeout=1)

# stop_event = threading.Event()

# # simple thread-safe print helper used in this module
# print_lock = threading.Lock()
# def safe_print(*args, **kwargs):
#     with print_lock:
#         print(*args, **kwargs)

# t = threading.Thread(target=print_current_time, args=(stop_event,) ,daemon=True)
# t.start()
# safe_print('Main dalej')
# t.join(timeout=5)
# safe_print('Main koniec')

class StopFinderFSM:
    def __init__(self):
        self.state = 'IDLE'
        self.results = []
        self.students_position = []
      #  self.live_terminal_started = False 

    def on_event(self,event):

        if self.state == 'IDLE':
            if event == 'user_request':
                print("IDLE")
                students_position = check_student_pos()
                self.students_position = students_position
                self.state = 'SEARCHING'
                self.event_happend()
                self.run_search(students_position)

        elif self.state == 'SEARCHING':
            if event =='search_done':
                print('SEARCHING done')
                self.state = 'DISPLAYING'


        elif self.state == 'DISPLAYING':
            print("\nDISPLAYING\n")
            self.display_find_nearest_stops()
            #self.display_student_pos()
            self.state = 'WROMAP'
            self.on_event("show")



        elif self.state == 'WROMAP':
            if event == 'show':
                print('Loading map...')
                print('OFF')
                self.draw_map()
                self.state = 'IDLE'

    def event_happend(self): # testing empty event
        print(" Event_happened: No event")
        print(" solution found: None")
        self.on_event("search_done")


    def draw_map(self):
        stops = []
        for student in self.results:
            for stop in student['reachable_stops']:
                stops.append((stop['stop_name'], stop['stop_lon'], stop['stop_lat']))
        show_map_with_students_and_stops(stops, self.students_position)

    def run_search(self, students_position):
        self.results = find_nearest_stops(students_pos=students_position) 
        self.on_event("search_done")
       
    # def display_student_pos(self):
    #     safe_print("Student position in Wroclaw (random):")
    #     for i, pos in enumerate(self.students_position, start=1):
    #         safe_print(f"  Student {i}: {pos}")


    def display_find_nearest_stops(self):
     for student in self.results:
        print(f"\nStudents {student['student_id']}: {student['position']}")

        if student['nearest_stops']:
            print("  Nearest stops:")        
            for d, stop_id, stop_name, stop_lat, stop_lon in student['nearest_stops'][:3]:
                print(f"  Stop: {stop_name}, ID: {stop_id}, Distance: {d:.6f}")
        else:
            print(" In your area, the bus stop is far far away")

        if student['reachable_stops']:
            print("  Reachable stops (with routes to target):")
            for stop in student['reachable_stops']:
                print(f"    Stop: {stop['stop_name']}, ID: {stop['stop_id']}, Routes to target: {stop['routes_to_target']}")
        else:
            print("  No reachable stops with routes to target from nearest stops.")
            
        if student.get('departures'):
            print(' Next departures:')
            for dep in student.get('departures', []):
                print(f"    Stop:{dep['stop_name']}, Route {dep['route_id']}: {dep['next_departure']}")
            display_departure_time_once(student['departures'])
        else:
            print("  No departure times available for reachable stops.")
        
        

#Test - always run user_request first
fsm = StopFinderFSM()
fsm.on_event("user_request")

