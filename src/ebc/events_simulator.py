from .AutomatFSM import StopFinderFSM
from .student_automata import StudentAutomata, TRANSITIONS
import time
import random
import datetime

class EventSimulator:
    def __init__(self, number_of_students=2, verbose=False,
                 current_time=datetime.datetime.now):
        self.curren_time = current_time
        self.verbose = verbose

        fsm = StopFinderFSM(verbose=True,
                            number_of_students=number_of_students,
                            current_time=self.curren_time)
        fsm.on_event("user_request")
        stops_finding_results = fsm.results

        self.students_automatas = []
        for student in stops_finding_results:
            self.students_automatas.append(StudentAutomata(student))
        if self.verbose:
            print("Students in event simulator:")
            for student_automata in self.students_automatas:
                student_automata.print_student_state() 

    def event_simulation_loop(self):
        while True:
            # pick one random student that is NOT in terminal state
            active_students = [s for s in self.students_automatas if s.state != "TERMINAL_STATE"]

            # stop if all students finished
            if not active_students:
                if self.verbose:
                    print("\nAll students reached terminal states. Simulation complete.")
                break

            student = random.choice(active_students)

            # random time before something happens to the student
            time.sleep(random.uniform(1.0, 4.0))

            # find possible events from current state
            possible_events = list(TRANSITIONS[student.state].keys())

            if not possible_events:
                continue  # safety, but should not occur

            # choose random event
            event = random.choice(possible_events)

            # apply event
            student.update_state(event)

            if self.verbose:
                print(f"\nEvent '{event}' triggered for student {student.student['student_id']}")
                student.print_student_state()