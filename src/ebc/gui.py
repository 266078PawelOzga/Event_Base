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
    QWidget
)
from PyQt5.QtCore import Qt, QTimer, QUrl
from PyQt5.QtWebEngineWidgets import QWebEngineView

class SimulationControlApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.timer = QTimer(self)
        self.timer.timeout.connect(self.sync_simulation_status)
        self.timer.timeout.connect(self.sync_simulation_config)
        self.timer.start(1000)

        self.base_url = "http://localhost:8000"
        self.init_ui()
        self.sync_simulation_status()
        self.sync_simulation_config()

    def init_ui(self):
        self.setWindowTitle('EBC: Wrocław MPK Navigation')
        layout_body = QHBoxLayout()
        layout_body.addWidget(self.build_menu(), alignment=Qt.AlignTop)
        layout_body.addWidget(self.build_map())

        layout = QVBoxLayout()
        layout.addLayout(layout_body)
        layout.addWidget(self.build_modeline(), alignment=Qt.AlignRight)

        widget = QWidget()
        widget.setLayout(layout)
        self.setCentralWidget(widget)

    def build_modeline(self) -> QWidget:
        modeline = QWidget()
        modeline.setFixedHeight(30)
        layout = QHBoxLayout()

        self.time_label = QLabel('Time: -')
        layout.addWidget(self.time_label)

        modeline.setLayout(layout)
        return modeline

    def build_map(self) -> QWidget:
        self.web_view = QWebEngineView()
        self.web_view.setUrl(QUrl(f"{self.base_url}/map"))
        return self.web_view

    def build_menu(self) -> QWidget:
        menu = QWidget()
        menu.setFixedWidth(120)
        layout = QVBoxLayout()

        # Simulation Toggle Button
        self.sim_toggle_btn = QPushButton('Start')
        self.sim_toggle_btn.clicked.connect(self.toggle_simulation)
        layout.addWidget(self.sim_toggle_btn)

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
