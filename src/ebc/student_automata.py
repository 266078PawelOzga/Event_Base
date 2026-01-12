TRANSITIONS = {
    'START': {
        'stop_found': 'WALK_TO_STOP',
        'classes_canceled': 'TERMINAL_STATE',
    },
    'WALK_TO_STOP': {
        'stop_reached': 'WAITING_FOR_TRANSPORTATION',
        'classes_canceled': 'TERMINAL_STATE',
    },
    'WAITING_FOR_TRANSPORTATION': {
        'available_bus_arrived':  'TRAVELING',
        'classes_canceled': 'TERMINAL_STATE',

    },
    'TRAVELING': {
        'goal_reached': 'TERMINAL_STATE',
        'classes_canceled': 'TERMINAL_STATE'
    },
    'TERMINAL_STATE': {
    }
}


class StudentAutomata:
    def __init__(self, student):
        self.state = 'START'
        self.student = student

    def on_event(self, event):
        state_transitions = TRANSITIONS.get(self.state, {})
        new_state = state_transitions.get(event)

        if new_state:
            self.state = new_state
        else:
            print(f'Event "{event}" is not supported for state "{self.state}".')

    def update_state(self, event):
        old = self.state
        self.on_event(event)
        return old != self.state
    
    def print_student_state(self):
        print(f"\nStudent {self.student['student_id']} state: {self.state}")
        
    def to_dict(self):
        return {
            "student_id": self.student.get("student_id"),
            "state": self.state,
        }
