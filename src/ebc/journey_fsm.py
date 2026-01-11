from .models import *

# TODO: make something with path planning and updates
def journey_fsm_simple(journey: Journey):
    """Journey State Machine - walking from origin to destination"""
    if journey.state == JourneyState.CREATED:
        journey.destination.arrival = journey.origin.arrival + timedelta(hours=1)
        journey.state = JourneyState.IN_PROGRESS
    elif journey.state == JourneyState.IN_PROGRESS:
        if journey.current_position == journey.destination.coord:
            journey.state = JourneyState.FINISHED
