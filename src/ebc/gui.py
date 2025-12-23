from .models import SimulationStatus, SimulationConfig
import sys
import requests
from PyQt5.QtWidgets import (
    QApplication,
    QMainWindow,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QSlider,
    QWidget,
    QSizePolicy
)
from PyQt5.QtCore import Qt, QTimer, QUrl
from PyQt5.QtWebEngineWidgets import QWebEngineView

border_width = 1
border_color = '#666'
spinner_base_period = 100

class SimulationControlApp(QMainWindow):
    def __init__(self):
        super().__init__()

        self.simulation_config = SimulationConfig()
        self.simulation_status = SimulationStatus()

        self.timer = QTimer(self)
        self.timer.timeout.connect(self.sync_simulation_status)
        self.timer.timeout.connect(self.sync_simulation_config)
        self.timer.start(1000)

        self.spinner_timer = QTimer(self)
        self.spinner_timer.timeout.connect(self.update_spinner)
        self.spinner_timer.start(spinner_base_period)

        self.base_url = "http://localhost:8000"
        self.init_ui()
        self.sync_simulation_status()
        self.sync_simulation_config()

    def init_ui(self):
        self.setWindowTitle('EBC: Wrocław MPK Navigation')
        layout_body = QHBoxLayout()
        layout_body.addWidget(self.build_menu())
        layout_body.addWidget(self.build_map())

        layout = QVBoxLayout()
        layout.addLayout(layout_body)
        layout.addWidget(self.build_modeline())

        widget = QWidget(objectName='window')
        widget.setStyleSheet(f"""
        * {{ font-size: 12pt; }}
        QWidget[objectName="menu"] {{
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

    def build_map(self) -> QWidget:
        map_ = QWidget(objectName='map')
        layout = QVBoxLayout()
        layout.setContentsMargins(border_width, border_width, border_width, border_width)
        self.web_view = QWebEngineView()
        self.web_view.setUrl(QUrl(f"{self.base_url}/map"))
        layout.addWidget(self.web_view)
        map_.setLayout(layout)
        return map_

    def build_menu(self) -> QWidget:
        menu = QWidget(objectName='menu')
        menu.setFixedWidth(140)
        layout = QVBoxLayout()

        # Simulation Toggle Button
        self.sim_toggle_btn = QPushButton('Start')
        self.sim_toggle_btn.clicked.connect(self.toggle_simulation)
        layout.addWidget(self.sim_toggle_btn)

        layout.addStretch()

        # Simulation Reset Button
        sim_reset_btn = QPushButton('Reset')
        sim_reset_btn.clicked.connect(self.reset_simulation)
        layout.addWidget(sim_reset_btn)

        # Tickrate Slider
        self.tickrate_slider = QSlider(Qt.Horizontal)
        self.tickrate_slider.setMinimum(0)
        self.tickrate_slider.setMaximum(100)
        self.tickrate_slider.setValue(10)
        self.tickrate_slider.valueChanged.connect(self.update_tickrate)

        self.tickrate_display = QLabel('Tickrate: -')
        layout.addWidget(self.tickrate_display)
        layout.addWidget(self.tickrate_slider)

        menu.setLayout(layout)
        return menu

    def resume_simulation(self):
        requests.post(f"{self.base_url}/resume")
        self.sync_simulation_status()

    def pause_simulation(self):
        requests.post(f"{self.base_url}/pause")
        self.sync_simulation_status()

    def get_simulation_status(self, force: bool = False):
        response = requests.get(f"{self.base_url}/status")
        if response.status_code == 200:
            self.simulation_status = SimulationStatus(**response.json())
        else:
            self.simulation_status = SimulationStatus()
            print(f'{self.base_url}/status -> response error {response.status_code}')

    def get_simulation_config(self, force: bool = False):
        response = requests.get(f"{self.base_url}/config")
        if response.status_code == 200:
            self.simulation_config = SimulationConfig(**response.json())
        else:
            self.simulation_config = SimulationConfig()
            print(f'{self.base_url}/config -> response error {response.status_code}')

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

    def process_simulation_config(self):
        config = self.simulation_config
        self.tickrate_display.setText(f'Tickrate: {config.tickrate:.1f} Hz')

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
        requests.post(f"{self.base_url}/reset")
        self.sync_simulation_status()

    def tick_simulation(self):
        requests.post(f"{self.base_url}/tick")
        self.sync_simulation_status()

    def update_tickrate(self):
        tickrate_resolution = self.simulation_config.tickrate_resolution
        tickrate = float(self.tickrate_slider.value() * tickrate_resolution)
        requests.post(f"{self.base_url}/tickrate", params={'value': tickrate})
        self.sync_simulation_config()

def run_gui():
    app = QApplication(sys.argv)
    ex = SimulationControlApp()
    ex.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    run_gui()
