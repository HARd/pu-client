from typing import Callable, Optional

from PySide6.QtCore import QObject, Qt, QUrl, Signal
from PySide6.QtGui import QKeySequence, QPixmap
from PySide6.QtMultimedia import QAudioOutput, QMediaPlayer
from PySide6.QtMultimediaWidgets import QVideoWidget
from PySide6.QtWidgets import (
    QDialog,
    QHBoxLayout,
    QLabel,
    QPushButton,
    QSlider,
    QVBoxLayout,
    QWidget,
)

class WorkerSignals(QObject):
    success = Signal(object)
    error = Signal(str)
    progress = Signal(int, str)

class PreviewDialog(QDialog):
    def __init__(self, parent: QWidget, file_name: str) -> None:
        super().__init__(parent)
        self.setWindowTitle(f"Preview: {file_name}")
        self.resize(980, 640)
        self.original_pixmap: Optional[QPixmap] = None
        self.current_file_name = file_name

        layout = QVBoxLayout(self)
        self.status_label = QLabel("Loading preview...")
        layout.addWidget(self.status_label)

        nav_row = QHBoxLayout()
        self.prev_btn = QPushButton("Previous")
        self.next_btn = QPushButton("Next")
        self.download_btn = QPushButton("Download")
        self.prev_btn.setObjectName("secondaryBtn")
        self.next_btn.setObjectName("secondaryBtn")
        self.download_btn.setObjectName("secondaryBtn")
        self.prev_btn.setMinimumHeight(28)
        self.next_btn.setMinimumHeight(28)
        self.download_btn.setMinimumHeight(28)
        nav_row.addWidget(self.prev_btn)
        nav_row.addWidget(self.next_btn)
        nav_row.addStretch(1)
        nav_row.addWidget(self.download_btn)
        layout.addLayout(nav_row)

        self.image_label = QLabel("")
        self.image_label.setAlignment(Qt.AlignCenter)
        self.image_label.setMinimumHeight(420)
        layout.addWidget(self.image_label, 1)

        self.video_widget = QVideoWidget()
        self.video_widget.setMinimumHeight(420)
        self.video_widget.setVisible(False)
        layout.addWidget(self.video_widget, 1)

        controls = QHBoxLayout()
        self.play_btn = QPushButton("Play")
        self.pause_btn = QPushButton("Pause")
        self.stop_btn = QPushButton("Stop")
        self.seek = QSlider(Qt.Horizontal)
        self.seek.setRange(0, 0)
        self.seek.setEnabled(False)
        controls.addWidget(self.play_btn)
        controls.addWidget(self.pause_btn)
        controls.addWidget(self.stop_btn)
        controls.addWidget(self.seek, 1)
        layout.addLayout(controls)

        self.audio_output = QAudioOutput()
        self.player = QMediaPlayer()
        self.player.setAudioOutput(self.audio_output)
        self.player.setVideoOutput(self.video_widget)

        self.play_btn.clicked.connect(self.player.play)
        self.pause_btn.clicked.connect(self.player.pause)
        self.stop_btn.clicked.connect(self.player.stop)
        self.seek.sliderMoved.connect(self.player.setPosition)
        self.player.positionChanged.connect(self._on_position_changed)
        self.player.durationChanged.connect(self._on_duration_changed)
        self.player.errorOccurred.connect(self._on_error)

        self.play_btn.setEnabled(False)
        self.pause_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)
        self.prev_btn.setEnabled(False)
        self.next_btn.setEnabled(False)
        self.download_btn.setEnabled(False)

    def set_navigation_handlers(
        self,
        prev_cb: Callable[[], None],
        next_cb: Callable[[], None],
        download_cb: Callable[[], None],
    ) -> None:
        self.prev_btn.clicked.connect(prev_cb)
        self.next_btn.clicked.connect(next_cb)
        self.download_btn.clicked.connect(download_cb)

    def set_navigation_state(self, has_prev: bool, has_next: bool) -> None:
        self.prev_btn.setEnabled(has_prev)
        self.next_btn.setEnabled(has_next)

    def set_download_enabled(self, enabled: bool) -> None:
        self.download_btn.setEnabled(enabled)

    def _on_position_changed(self, pos: int) -> None:
        if not self.seek.isSliderDown():
            self.seek.setValue(pos)

    def _on_duration_changed(self, duration: int) -> None:
        self.seek.setRange(0, max(0, duration))

    def _on_error(self, _error) -> None:
        self.status_label.setText(f"Playback error: {self.player.errorString() or 'Unknown error'}")

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        if self.original_pixmap and self.image_label.isVisible():
            scaled = self.original_pixmap.scaled(self.image_label.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation)
            self.image_label.setPixmap(scaled)

    def keyPressEvent(self, event) -> None:
        if event.key() == Qt.Key_Left and self.prev_btn.isEnabled():
            self.prev_btn.click()
            event.accept()
            return
        if event.key() == Qt.Key_Right and self.next_btn.isEnabled():
            self.next_btn.click()
            event.accept()
            return
        if event.matches(QKeySequence.Save) and self.download_btn.isEnabled():
            self.download_btn.click()
            event.accept()
            return
        super().keyPressEvent(event)

    def closeEvent(self, event) -> None:
        self.player.stop()
        super().closeEvent(event)

    def show_image(self, file_name: str, data: bytes) -> None:
        self.current_file_name = file_name
        self.setWindowTitle(f"Preview: {file_name}")
        pix = QPixmap()
        if not pix.loadFromData(data):
            self.status_label.setText("Failed to decode image.")
            return
        self.original_pixmap = pix
        self.video_widget.setVisible(False)
        self.image_label.setVisible(True)
        scaled = pix.scaled(self.image_label.size(), Qt.KeepAspectRatio, Qt.SmoothTransformation)
        self.image_label.setPixmap(scaled)
        self.status_label.setText(f"Image preview: {file_name}")
        self.seek.setEnabled(False)
        self.play_btn.setEnabled(False)
        self.pause_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)

    def show_media(self, file_name: str, media_url: str, is_video: bool) -> None:
        self.current_file_name = file_name
        self.setWindowTitle(f"Preview: {file_name}")
        self.original_pixmap = None
        self.image_label.clear()
        self.image_label.setVisible(False)
        self.video_widget.setVisible(is_video)
        self.seek.setEnabled(True)
        self.play_btn.setEnabled(True)
        self.pause_btn.setEnabled(True)
        self.stop_btn.setEnabled(True)
        self.player.setSource(QUrl(media_url))
        self.player.play()
        label = "Video" if is_video else "Audio"
        self.status_label.setText(f"{label} preview: {file_name}")
