from .AutomatFSM import StopFinderFSM
from .student_automata import StudentAutomata
import time
import random

class EventSimulator:
    def __init__(self, number_of_students=2, verbose=False):
        fsm = StopFinderFSM()
        fsm.on_event("user_request")
        stops_finding_results = fsm.results

        self.verbose = verbose
        self.students_automatas = []
        for student in stops_finding_results:
            self.students_automatas.append(StudentAutomata(student))
        if self.verbose:
            print("Students in event simulator:")
            for student_automata in self.students_automatas:
                student_automata.print_student_state() 

    def event_simulation_loop(self):
        while True:
            for student_automata in self.students_automatas:
                time.sleep(random.randint(2, 4))
                match student_automata.state:
                    case "START":
                        student_automata.update_state(event="stop_found")
                        if self.verbose:
                            student_automata.print_student_state()
