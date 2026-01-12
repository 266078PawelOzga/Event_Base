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
        self.timer.timeout.connect(self.update_time)
        self.timer.timeout.connect(self.update_tickrate_display)
        self.timer.start(1000)
        self.base_url = "http://localhost:8000"
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

        self.web_view = QWebEngineView()
        self.web_view.setUrl(QUrl(f"{self.base_url}/map"))
        layout.addWidget(self.web_view)

        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)

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
            response = requests.get(f"{self.base_url}/tickrate")
            tickrate = response.json()['tickrate']
            self.tickrate_display.setText(f'Tickrate: {tickrate:.1f} Hz')
        except:
            pass

    def update_tickrate(self):
        tickrate_scale = 0.1
        value = float(self.tickrate_slider.value() * tickrate_scale)
        requests.post(f"{self.base_url}/tickrate", params={'value': value})
        self.update_tickrate_display()

    def update_time(self):
        try:
            response = requests.get(f"{self.base_url}/time")
            time = response.json()['current_time']
            self.time_label.setText(f'Time: {time}')
        except:
            pass

def run_gui():
    app = QApplication(sys.argv)
    ex = SimulationControlApp()
    ex.show()
    sys.exit(app.exec_())

if __name__ == '__main__':
    run_gui()
