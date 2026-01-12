import random
from .models import *
"""
    2: Generate Student position
"""
def check_student_pos(students_count=2, students_pos=None):
    positions = []

    if students_pos is not None:

        return students_pos
    
    for _ in range(students_count):
        student_lat = random.uniform(51.06, 51.14)
        student_lon = random.uniform(16.94, 17.13)
        positions.append((student_lat, student_lon))

    return positions

def real_student_pos(location: Location):
    # TODO: implement reading real student position from file or input
    # How to handle a many students positions?
    positions = []
    positions.append((location.coord.lat, location.coord.lon))

    return positions
