from target_stop import find_nearest_stops

class StopFinderFSM:
    def __init__(self):
        self.state = 'IDLE'

    def on_event(self,event):
        if self.state == 'IDLE':
            if event == 'user_request':
                print("IDLE")
                self.state = 'SEARCHING'
                self.event_happend()
                self.run_search()

        elif self.state == 'SEARCHING':
            if event =='search_done':
                print('SEARCHING done')
                self.state = 'DISPLAYING'

        elif self.state == 'DISPLAYING':
            if event == 'user_request':
                print('DISPLAYING:')
                self.state = 'IDLE'

    def event_happend(self): # testing empty event
        print("Event_happened")
        print("solution found: None")
        self.on_event("search_done")

    def run_search(self):
        reachable_stops, time_right_now = find_nearest_stops() 
        self.on_event("search_done")


#Test
fsm = StopFinderFSM()
fsm.on_event("user_request")

#     conn.close()
#     return reachable_stops, time_right_now

# if __name__ == "__main__":
#     find_nearest_stops()