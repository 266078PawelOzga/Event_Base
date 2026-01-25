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

        # ==========================
        # NEW: build stop-by-stop path
        # ==========================
        self.path = []
        self._build_path()
        # ==========================

    def _build_path(self):
        """
        Builds the full list of stops between the chosen start stop
        and the destination stop for this student.
        """
        try:
            reachable = self.student.get("reachable_stops", [])
            if not reachable:
                return

            chosen = reachable[0]

            trip_id = chosen.get("trip_id")
            start_stop_id = chosen.get("stop_id")
            target_stop_id = chosen.get("target_stop_id")

            if not trip_id or not start_stop_id or not target_stop_id:
                return

            import sqlite3
            from .target_stop import get_stops_between

            conn = sqlite3.connect('.cache/mpk.db')
            cursor = conn.cursor()

            self.path = get_stops_between(
                cursor,
                start_stop_id,
                target_stop_id,
                trip_id
            )

            conn.close()
            print(self.path)

        except Exception as e:
            print("StudentAutomata path build failed:", e)

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
            "path": self.path,  # optional: makes debugging & visualization easy
        }
