from .models import *
import sys
import requests
from PyQt5.QtWidgets import (
    QScrollArea,
    QApplication,
    QMainWindow,
    QDialog,
    QLineEdit,
    QShortcut,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QSlider,
    QWidget,
    QSizePolicy
)
from PyQt5.QtGui import QKeySequence
from PyQt5.QtCore import Qt, QTimer, QUrl
from PyQt5.QtWebEngineWidgets import QWebEngineView

border_width = 1
border_color = '#666'
spinner_base_period = 100
base_url = "http://localhost:8000"
font_size = 12

class SimulationControlApp(QMainWindow):
    def __init__(self):
        super().__init__()

        self.resize(960, 640)

        quit_shortcut = QShortcut(QKeySequence('Ctrl+q'), self)
        quit_shortcut.activated.connect(QApplication.instance().quit)

        self.simulation_config = SimulationConfig()
        self.simulation_status = SimulationStatus()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.sync_simulation_status)
        self.timer.timeout.connect(self.sync_simulation_config)
        self.timer.start(1000)

        self.spinner_timer = QTimer(self)
        self.spinner_timer.timeout.connect(self.update_spinner)
        self.spinner_timer.start(spinner_base_period)

        self.init_ui()
        self.sync_simulation_status()
        self.sync_simulation_config()

    def init_ui(self):
        self.setWindowTitle('EBC: Wrocław MPK Navigation')
        layout_body = QHBoxLayout()
        # FIXME: currently map has to be created first so menu can reference self.web_view
        # can we make them independent?
        wmap = self.build_map()
        wmenu = self.build_menu()
        self.journeys_panel = self.build_journeys_panel()
        self.journeys_panel.hide()
        layout_body.addWidget(wmenu)
        layout_body.addWidget(self.journeys_panel)
        layout_body.addWidget(wmap)

        layout = QVBoxLayout()
        layout.addLayout(layout_body)
        layout.addWidget(self.build_modeline())

        widget = QWidget(objectName='window')
        widget.setStyleSheet(f"""
        * {{ font-size: {font_size}pt; }}
        QWidget[objectName="menu"] {{
        border: {border_width}px solid {border_color};
        }}
        QWidget[objectName="journey"] {{
        border: {border_width}px solid {border_color};
        border-radius: 6px;
        background-color: #f4f4f4;
        }}
        QWidget[objectName="journeys"] {{
        border: {border_width}px solid {border_color};
        }}
        QWidget[objectName="modeline"] {{
        border: {border_width}px solid {border_color};
        }}
        QWidget[objectName="map"] {{
        border: {border_width}px solid {border_color};
        }}
        """)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        widget.setLayout(layout)
        self.setCentralWidget(widget)

    def build_modeline(self) -> QWidget:
        modeline = QWidget(objectName='modeline')
        modeline.setFixedHeight(36)
        layout = QHBoxLayout()

        self.spinner = QLabel('⏸')
        self.spinner_progress = 0
        layout.addWidget(self.spinner)

        layout.addStretch()

        self.time_label = QLabel('Time: -')
        layout.addWidget(self.time_label)

        modeline.setLayout(layout)
        return modeline

    def build_journeys_panel(self) -> QWidget:
        journeys = QScrollArea(objectName='journeys')
        journeys.setWidgetResizable(True)
        journeys.setFixedWidth(300)

        container = QWidget()
        self.journeys_layout = QVBoxLayout(container)
        self.journeys_layout.addStretch()
        self.journeys_list = []

        journeys.setWidget(container)
        return journeys

    def toggle_journeys_panel(self):
        self.journeys_panel.setVisible(not self.journeys_panel.isVisible())

    def add_journey_to_panel(self, journey: Journey):
        """Add journey widget to the top of Journeys panel"""
        journey_widget = QWidget(objectName='journey')
        layout = QVBoxLayout(journey_widget)
        layout.addWidget(QLabel(f"O: {journey.origin.name}"))
        layout.addWidget(QLabel(f"D: {journey.destination.name}"))
        layout.addWidget(QLabel(f"State: {journey.state.value}"))

        self.journeys_layout.insertWidget(0, journey_widget)
        self.journeys_list.insert(0, journey_widget)

    def clear_journeys_panel(self):
        """Remove all journeys from the Journeys panel"""
        for journey in self.journeys_list[:]:
            self.journeys_layout.removeWidget(journey)
            journey.deleteLater()
            self.journeys_list.remove(journey)

    def build_map(self) -> QWidget:
        map_ = QWidget(objectName='map')
        layout = QVBoxLayout()
        layout.setContentsMargins(border_width, border_width, border_width, border_width)
        self.web_view = QWebEngineView()
        self.web_view.setUrl(QUrl(f"{base_url}/map"))
        layout.addWidget(self.web_view)
        map_.setLayout(layout)
        return map_

    def build_menu(self) -> QWidget:
        menu = QWidget(objectName='menu')
        menu.setFixedWidth(120)
        layout = QVBoxLayout()

        # Simulation Toggle Button
        self.sim_toggle_btn = QPushButton('Start')
        self.sim_toggle_btn.clicked.connect(self.toggle_simulation)
        layout.addWidget(self.sim_toggle_btn)

        sim_toggle_shortcut = QShortcut(QKeySequence('s'), self)
        sim_toggle_shortcut.activated.connect(self.toggle_simulation)

        self.add_route_btn = QPushButton('New journey')
        self.add_route_btn.clicked.connect(self.add_route_window)
        layout.addWidget(self.add_route_btn)

        add_route_shortcut = QShortcut(QKeySequence('a'), self)
        add_route_shortcut.activated.connect(self.add_route_window)

        self.toggle_journeys_btn = QPushButton('Journeys')
        self.toggle_journeys_btn.clicked.connect(self.toggle_journeys_panel)

        toggle_journeys_shortcut = QShortcut(QKeySequence('t'), self)
        toggle_journeys_shortcut.activated.connect(self.toggle_journeys_panel)

        layout.addWidget(self.toggle_journeys_btn)
        layout.addStretch()

        update_journeys_shortcut = QShortcut(QKeySequence('r'), self)
        update_journeys_shortcut.activated.connect(self.web_view.reload)

        # Simulation Reset Button
        sim_reset_btn = QPushButton('Reset')
        sim_reset_btn.clicked.connect(self.reset_simulation)
        layout.addWidget(sim_reset_btn)

        reset_shortcut = QShortcut(QKeySequence('Ctrl+r'), self)
        reset_shortcut.activated.connect(self.reset_simulation)

        # Tickrate Slider
        self.tickrate_slider = QSlider(Qt.Horizontal)
        self.tickrate_slider.setMinimum(0)
        self.tickrate_slider.setMaximum(100)
        self.tickrate_slider.setValue(10)
        self.tickrate_slider.valueChanged.connect(self.update_tickrate)

        self.tickrate_display = QLabel('Ticks: -')
        layout.addWidget(self.tickrate_display)
        layout.addWidget(self.tickrate_slider)

        menu.setLayout(layout)
        return menu

    def add_route_window(self):
        dialog = NewJourneyWindow(self)
        dialog.exec_()  # Modal dialog, blocks main window

    def resume_simulation(self):
        requests.post(f"{base_url}/resume")
        self.sync_simulation_status()

    def pause_simulation(self):
        requests.post(f"{base_url}/pause")
        self.sync_simulation_status()

    def get_simulation_status(self, force: bool = False):
        response = requests.get(f"{base_url}/status")
        if response.status_code == 200:
            self.simulation_status = SimulationStatus(**response.json())
        else:
            self.simulation_status = SimulationStatus()
            print(f'{base_url}/status -> response error {response.status_code}')

    def get_simulation_config(self, force: bool = False):
        response = requests.get(f"{base_url}/config")
        if response.status_code == 200:
            self.simulation_config = SimulationConfig(**response.json())
        else:
            self.simulation_config = SimulationConfig()
            print(f'{base_url}/config -> response error {response.status_code}')

    def update_spinner(self):
        spinner_chars = [' ⠋', ' ⠙', ' ⠹', ' ⠸', ' ⠼', ' ⠴', ' ⠦', ' ⠧', ' ⠇', ' ⠏']
        paused = '⏸'

        if not self.simulation_status.running:
            self.spinner.setText(paused)
            return

        if self.simulation_config.tickrate <= 0:
            return

        if self.spinner_progress >= len(spinner_chars):
            self.spinner_progress = 0

        self.spinner.setText(spinner_chars[self.spinner_progress])
        self.spinner_progress += 1

        # Wheeeeeee...
        self.spinner_timer.start(int(spinner_base_period / self.simulation_config.tickrate**0.5))

    def process_simulation_status(self):
        status = self.simulation_status

        if status.running:
            self.sim_toggle_btn.setText('Pause')
        elif status.reset:
            self.sim_toggle_btn.setText('Start')
        else:
            self.sim_toggle_btn.setText('Resume')

        time = self.simulation_status.time.strftime("%H:%M")
        self.time_label.setText(f'Time: {time}')

        # FIXME: inefficient, but simple
        self.clear_journeys_panel()
        for journey in status.journeys:
            self.update_marker_position(journey)
            self.add_journey_to_panel(journey)


    def update_marker_position(self, journey: Journey):
        """
        Update the position of the marker on the map
        without reloading the whole map
        """
        lat, lon = journey.current_position.tuple()
        js_script = f"""
        if (position_markers['journey_{journey.id}']) {{
        eval(position_markers['journey_{journey.id}']).setLatLng([{lat}, {lon}]);
        }}
        """
        self.web_view.page().runJavaScript(js_script)

    def process_simulation_config(self):
        config = self.simulation_config
        self.tickrate_display.setText(f'Ticks: {config.tickrate:.1f} Hz')

    def toggle_simulation(self) -> None:
        self.get_simulation_status()

        if self.simulation_status.running:
            self.pause_simulation()
        else:
            self.resume_simulation()

    def sync_simulation_status(self):
        self.get_simulation_status()
        self.process_simulation_status()

    def sync_simulation_config(self):
        self.get_simulation_config()
        self.process_simulation_config()

    def reset_simulation(self):
        requests.post(f"{base_url}/reset")
        self.sync_simulation_status()
        self.web_view.reload()

    def tick_simulation(self):
        requests.post(f"{base_url}/tick")
        self.sync_simulation_status()

    def update_tickrate(self):
        tickrate_resolution = self.simulation_config.tickrate_resolution
        tickrate = float(self.tickrate_slider.value() * tickrate_resolution)
        requests.post(f"{base_url}/tickrate", params={'value': tickrate})
        self.sync_simulation_config()


class NewJourneyWindow(QDialog):
    def __init__(self, mainwindow):
        super().__init__()
        self.mainwindow = mainwindow
        self.setWindowTitle("New journey")
        layout = QVBoxLayout()

        self.setStyleSheet(f"""
        * {{ font-size: {font_size}pt; }}
        """)

        self.origin = QLineEdit()
        self.origin.setPlaceholderText("")
        layout.addWidget(QLabel("Origin:"))
        layout.addWidget(self.origin)

        self.destination = QLineEdit()
        self.destination.setPlaceholderText("")
        layout.addWidget(QLabel("Destination:"))
        layout.addWidget(self.destination)

        self.error_label = QLabel("")
        layout.addWidget(self.error_label)

        self.error_label.setStyleSheet("""
        color: red;
        font-weight: bold;
        """)

        submit_btn = QPushButton("Find route")
        submit_btn.clicked.connect(self.on_submit)
        layout.addWidget(submit_btn)

        self.setLayout(layout)

    def on_submit(self):
        if self.origin.text() == "":
            self.error_label.setText("Origin missing")
            return

        if self.destination.text() == "":
            self.error_label.setText("Destination missing")
            return

        journey = Journey(
            origin = Location(name=self.origin.text()),
            destination = Location(name=self.destination.text())
        )

        response = requests.post(f"{base_url}/journey", json=journey.model_dump())
        if response.status_code == 200:
            self.mainwindow.web_view.reload()
            self.accept()
        # elif 'detail' in response.json():
        #     self.error_label.setText(response.json().get('detail'))
        else:
            self.error_label.setText(f"Server request error ({response.status_code})")

def run_gui():
    app = QApplication(sys.argv)
    ex = SimulationControlApp()
    ex.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    run_gui()
