import sys
import requests
import threading
import logging
from .wro_map import create_map_with_students_and_stops
from PyQt5.QtWidgets import (
    QApplication,
    QMainWindow,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QSlider,
    QWidget
)
from PyQt5.QtCore import Qt, QTimer, QUrl, pyqtSignal, QObject
from PyQt5.QtWebEngineWidgets import QWebEngineView

# Setup logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class MapLoaderSignals(QObject):
    """Signals for map loading thread."""
    map_loaded = pyqtSignal(list, list)  # (stops, students)
    map_error = pyqtSignal(str)  # error message

class SimulationControlApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.update_time)
        self.timer.timeout.connect(self.update_tickrate_display)
        self.timer.start(1000)
        self.base_url = "http://localhost:8000"
        
        # Signals for map loading
        self.map_signals = MapLoaderSignals()
        self.map_signals.map_loaded.connect(self.on_map_loaded)
        self.map_signals.map_error.connect(self.on_map_error)
        
        self.initUI()

    def initUI(self):
        self.setWindowTitle('EBC: Wrocław MPK Navigation')
        central_widget = QWidget()
        layout = QVBoxLayout()

        # Time display
        self.time_label = QLabel('Time: 0')
        layout.addWidget(self.time_label)

        # Control Buttons
        btn_layout = QHBoxLayout()
        start_btn = QPushButton('Start')
        stop_btn = QPushButton('Stop')
        reset_btn = QPushButton('Reset')
        tick_btn = QPushButton('Tick')

        start_btn.clicked.connect(self.start_simulation)
        stop_btn.clicked.connect(self.stop_simulation)
        reset_btn.clicked.connect(self.reset_simulation)
        tick_btn.clicked.connect(self.tick_simulation)

        btn_layout.addWidget(start_btn)
        btn_layout.addWidget(stop_btn)
        btn_layout.addWidget(reset_btn)
        btn_layout.addWidget(tick_btn)
        layout.addLayout(btn_layout)

        # Tickrate Slider
        self.tickrate_slider = QSlider(Qt.Horizontal)
        self.tickrate_slider.setMinimum(0)
        self.tickrate_slider.setMaximum(100)
        self.tickrate_slider.setValue(10)

        self.tickrate_slider.valueChanged.connect(self.update_tickrate)
        self.tickrate_display = QLabel('Tickrate')
        layout.addWidget(self.tickrate_display)
        layout.addWidget(self.tickrate_slider)

        # Load Map Button
        load_map_btn = QPushButton('Load Map')
        load_map_btn.clicked.connect(self.load_and_show_map)
        layout.addWidget(load_map_btn)

        self.web_view = QWebEngineView()
        self.web_view.setMinimumHeight(400)  # Set minimum height for map visibility
        #self.web_view.setUrl(QUrl(f"{self.base_url}/map"))
        layout.addWidget(self.web_view)

        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)
        self.resize(1000, 800)  # Set initial window size

    def show_map(self, stops, students):
        """Display map with stops and students in the web view."""
        logger.info(f"show_map: stops={len(stops)}, students={len(students)}")
        try:
            if not stops or not students:
                logger.warning("show_map: No data to display")
                self.web_view.setHtml("<p>No data to display on map.</p>")
                return
            
            logger.debug("show_map: Creating map object...")
            map_obj = create_map_with_students_and_stops(stops, students)
            logger.debug("show_map: Getting map HTML...")
            map_html = map_obj._repr_html_()
            
            if not map_html:
                logger.error("show_map: map_html is empty!")
                self.web_view.setHtml("<p>Error: Map HTML is empty</p>")
                return
            
            logger.debug(f"show_map: map_html length={len(map_html)} bytes")
            
            # Wrap folium map HTML in a complete HTML document
            full_html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Bus Stop Map</title>
</head>
<body style="margin:0; padding:0;">
    {map_html}
</body>
</html>
"""
            logger.info("show_map: Setting HTML in web view...")
            self.web_view.setHtml(full_html)
            logger.info("show_map: Map displayed successfully")
        except Exception as e:
            logger.error(f"show_map error: {e}", exc_info=True)
            self.web_view.setHtml(f"<p>Error loading map: {str(e)}</p>")

    def load_and_show_map(self):
        """Load student positions and reachable stops, then display map."""
        logger.info("Load Map button clicked")
        self.web_view.setHtml("<p>Loading map data...</p>")
        
        # Run in background thread to avoid blocking UI
        thread = threading.Thread(target=self._load_map_thread, daemon=True)
        thread.start()
    
    def _load_map_thread(self):
        """Background thread to load map data."""
        try:
            logger.info("Starting map data load...")
            from .students_pos import check_student_pos
            from .target_stop import find_nearest_stops
            
            logger.info("Getting student positions...")
            students = check_student_pos()
            logger.info(f"Got {len(students)} students")
            
            logger.info("Finding nearest stops...")
            data = find_nearest_stops(students_pos=students)
            logger.info(f"Got data for {len(data)} students")
            
            # Extract stops: (name, lon, lat)
            stops = []
            for student_data in data:
                for stop in student_data.get('reachable_stops', []):
                    stops.append((
                        stop['stop_name'],
                        stop['stop_lon'],
                        stop['stop_lat']
                    ))
            
            logger.info(f"Extracted {len(stops)} stops")
            self.map_signals.map_loaded.emit(stops, students)
            
        except Exception as e:
            logger.error(f"Error loading map: {e}", exc_info=True)
            self.map_signals.map_error.emit(str(e))
    
    def on_map_loaded(self, stops, students):
        """Called when map data is loaded."""
        logger.info("Map data loaded, displaying...")
        self.show_map(stops, students)
    
    def on_map_error(self, error_msg):
        """Called when map loading fails."""
        logger.error(f"Map error: {error_msg}")
        self.web_view.setHtml(f"<p>Error loading map:</p><p>{error_msg}</p>")

    def start_simulation(self):
        requests.post(f"{self.base_url}/start")
        self.update_time()

    def stop_simulation(self):
        requests.post(f"{self.base_url}/stop")

    def reset_simulation(self):
        requests.post(f"{self.base_url}/reset")
        self.update_time()

    def tick_simulation(self):
        requests.post(f"{self.base_url}/tick")
        self.update_time()

    def update_tickrate_display(self):
        try:
            response = requests.get(f"{self.base_url}/tickrate", timeout=1)
            tickrate = response.json()['tickrate']
            self.tickrate_display.setText(f'Tickrate: {tickrate:.1f} Hz')
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            self.tickrate_display.setText('Tickrate: (server offline)')
        except Exception:
            pass

    def update_tickrate(self):
        tickrate_scale = 0.1
        value = float(self.tickrate_slider.value() * tickrate_scale)
        requests.post(f"{self.base_url}/tickrate", params={'value': value})
        self.update_tickrate_display()

    def update_time(self):
        try:
            response = requests.get(f"{self.base_url}/time", timeout=1)
            time = response.json()['current_time']
            self.time_label.setText(f'Time: {time}')
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            self.time_label.setText('Time: (server offline)')
        except Exception:
            pass

def run_gui():
    app = QApplication(sys.argv)
    ex = SimulationControlApp()
    ex.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    run_gui()
