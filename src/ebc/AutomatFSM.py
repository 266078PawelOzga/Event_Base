from target_stop import find_nearest_stops
from students_pos import check_student_pos
from wro_map import show_map_with_students_and_stops
from time_operation import display_departure_time_once
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
    def __init__(self):
        self.state = 'IDLE'
        self.results = []
        self.students_position = []

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
            #if event == 'user_request':
                print('DISPLAYING:')
                self.display_find_nearest_stops()
                self.state = 'WROMAP'
                self.on_event("show")


        elif self.state == 'WROMAP':
            if event == 'show':
                print('Loading map...')
                print('OFF')
                #self.draw_map()
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