import base64
import datetime as dt
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, urlencode

import requests
from PySide6.QtCore import QObject, Qt, QUrl, Signal
from PySide6.QtGui import QAction, QDesktopServices, QPixmap
from PySide6.QtGui import QKeySequence
from PySide6.QtMultimedia import QAudioOutput, QMediaPlayer
from PySide6.QtMultimediaWidgets import QVideoWidget
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QMenu,
    QInputDialog,
    QPushButton,
    QProgressBar,
    QSlider,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QToolButton,
    QVBoxLayout,
    QWidget,
    QSplitter,
)


APP_VERSION = "0.1.0"
DEFAULT_UPDATE_REPO = "Erleke/backblaze"


def format_bytes(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(max(0, num_bytes))
    unit_idx = 0
    while value >= 1024 and unit_idx < len(units) - 1:
        value /= 1024.0
        unit_idx += 1
    if unit_idx == 0:
        return f"{int(value)} {units[unit_idx]}"
    return f"{value:.2f} {units[unit_idx]}"


def parse_semver(tag: str) -> Tuple[int, int, int]:
    raw = tag.strip().lstrip("v")
    m = re.match(r"^(\d+)\.(\d+)\.(\d+)", raw)
    if not m:
        return (0, 0, 0)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


class BackblazeB2Client:
    def __init__(self) -> None:
        self.account_id = None
        self.authorization_token = None
        self.api_url = None
        self.download_url = None

    def authorize(self, key_id: str, application_key: str) -> None:
        credentials = f"{key_id}:{application_key}".encode("utf-8")
        auth_header = base64.b64encode(credentials).decode("utf-8")

        response = requests.get(
            "https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
            headers={"Authorization": f"Basic {auth_header}"},
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        self.account_id = data["accountId"]
        self.authorization_token = data["authorizationToken"]
        self.api_url = data["apiUrl"]
        self.download_url = data["downloadUrl"]

    def _require_auth(self) -> None:
        if not self.authorization_token or not self.api_url:
            raise RuntimeError("Client is not authorized.")

    def get_upload_url(self, bucket_id: str) -> dict:
        self._require_auth()
        response = requests.post(
            f"{self.api_url}/b2api/v2/b2_get_upload_url",
            headers={"Authorization": self.authorization_token},
            json={"bucketId": bucket_id},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()

    def upload_file(
        self,
        bucket_id: str,
        local_path: str,
        file_name_in_bucket: str,
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        wait_if_paused: Optional[Callable[[], None]] = None,
    ) -> dict:
        upload_info = self.get_upload_url(bucket_id)
        upload_url = upload_info["uploadUrl"]
        upload_auth_token = upload_info["authorizationToken"]

        total_size = os.path.getsize(local_path)
        sha1 = self._compute_file_sha1(local_path, total_size, progress_cb, should_stop, wait_if_paused)

        headers = {
            "Authorization": upload_auth_token,
            "X-Bz-File-Name": quote(file_name_in_bucket, safe="/"),
            "Content-Type": "b2/x-auto",
            "Content-Length": str(total_size),
            "X-Bz-Content-Sha1": sha1,
        }

        with open(local_path, "rb") as f:
            stream = UploadProgressReader(f, total_size, progress_cb, should_stop, wait_if_paused)
            response = requests.post(upload_url, headers=headers, data=stream, timeout=120)

        if response.status_code >= 400:
            try:
                details = response.json()
            except Exception:
                details = response.text
            raise RuntimeError(f"Upload failed ({response.status_code}): {details}")

        return response.json()

    def _compute_file_sha1(
        self,
        local_path: str,
        total_size: int,
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        wait_if_paused: Optional[Callable[[], None]] = None,
    ) -> str:
        hasher = hashlib.sha1()
        processed = 0
        chunk_size = 1024 * 1024

        with open(local_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                if should_stop and should_stop():
                    raise RuntimeError("Transfer stopped by user.")
                if wait_if_paused:
                    wait_if_paused()
                hasher.update(chunk)
                processed += len(chunk)
                if progress_cb:
                    progress_cb("hash", processed, total_size)

        if progress_cb:
            progress_cb("hash", total_size, total_size)

        return hasher.hexdigest()

    def list_files(self, bucket_id: str, prefix: str = "", max_count: int = 1000) -> List[Dict]:
        self._require_auth()

        payload = {
            "bucketId": bucket_id,
            "maxFileCount": max_count,
        }
        if prefix:
            payload["prefix"] = prefix

        response = requests.post(
            f"{self.api_url}/b2api/v2/b2_list_file_names",
            headers={"Authorization": self.authorization_token},
            json=payload,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("files", [])

    def list_files_all(self, bucket_id: str, prefix: str = "", max_count: int = 1000) -> List[Dict]:
        self._require_auth()
        all_files: List[Dict] = []
        next_file_name = None

        while True:
            payload = {
                "bucketId": bucket_id,
                "maxFileCount": max_count,
            }
            if prefix:
                payload["prefix"] = prefix
            if next_file_name:
                payload["startFileName"] = next_file_name

            response = requests.post(
                f"{self.api_url}/b2api/v2/b2_list_file_names",
                headers={"Authorization": self.authorization_token},
                json=payload,
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            all_files.extend(data.get("files", []))
            next_file_name = data.get("nextFileName")
            if not next_file_name:
                break

        return all_files

    def get_download_authorization(self, bucket_id: str, file_name: str, valid_seconds: int) -> str:
        self._require_auth()

        response = requests.post(
            f"{self.api_url}/b2api/v2/b2_get_download_authorization",
            headers={"Authorization": self.authorization_token},
            json={
                "bucketId": bucket_id,
                "fileNamePrefix": file_name,
                "validDurationInSeconds": valid_seconds,
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["authorizationToken"]

    def make_direct_url(self, bucket_name: str, file_name: str, auth_token: Optional[str] = None) -> str:
        if not self.download_url:
            raise RuntimeError("Missing download URL. Authorize first.")

        encoded_file_name = quote(file_name, safe="/")
        url = f"{self.download_url}/file/{bucket_name}/{encoded_file_name}"

        if auth_token:
            return f"{url}?{urlencode({'Authorization': auth_token})}"
        return url

    def download_file(
        self,
        bucket_name: str,
        file_name: str,
        target_path: str,
        progress_cb: Optional[Callable[[int, int], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        wait_if_paused: Optional[Callable[[], None]] = None,
    ) -> None:
        self._require_auth()
        url = self.make_direct_url(bucket_name, file_name)
        headers = {"Authorization": self.authorization_token}

        response = requests.get(url, headers=headers, stream=True, timeout=120)
        if response.status_code >= 400:
            try:
                details = response.json()
            except Exception:
                details = response.text
            raise RuntimeError(f"Download failed ({response.status_code}): {details}")

        total = int(response.headers.get("Content-Length", "0"))
        downloaded = 0
        chunk_size = 1024 * 256

        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with open(target_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                if should_stop and should_stop():
                    raise RuntimeError("Transfer stopped by user.")
                if wait_if_paused:
                    wait_if_paused()
                f.write(chunk)
                downloaded += len(chunk)
                if progress_cb:
                    progress_cb(downloaded, total)

        if progress_cb:
            progress_cb(downloaded, total)


class UploadProgressReader:
    def __init__(
        self,
        file_obj,
        total_size: int,
        progress_cb: Optional[Callable[[str, int, int], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        wait_if_paused: Optional[Callable[[], None]] = None,
    ) -> None:
        self.file_obj = file_obj
        self.total_size = total_size
        self.sent = 0
        self.progress_cb = progress_cb
        self.should_stop = should_stop
        self.wait_if_paused = wait_if_paused

    def __len__(self) -> int:
        return self.total_size

    def tell(self) -> int:
        return self.sent

    def seek(self, offset: int, whence: int = 0) -> int:
        pos = self.file_obj.seek(offset, whence)
        self.sent = self.file_obj.tell()
        return pos

    def read(self, amt: int = -1) -> bytes:
        if self.should_stop and self.should_stop():
            raise RuntimeError("Transfer stopped by user.")
        if self.wait_if_paused:
            self.wait_if_paused()
        chunk = self.file_obj.read(amt)
        if not chunk:
            if self.progress_cb:
                self.progress_cb("upload", self.total_size, self.total_size)
            return b""

        self.sent += len(chunk)
        if self.progress_cb:
            self.progress_cb("upload", self.sent, self.total_size)
        return chunk


class SettingsStore:
    def __init__(self) -> None:
        self.path = self._get_settings_path()

    def _get_settings_path(self) -> Path:
        if os.name == "nt":
            base = Path(os.environ.get("APPDATA", Path.home()))
            root = base / "BackblazeB2Client"
        elif os.uname().sysname == "Darwin":
            root = Path.home() / "Library" / "Application Support" / "BackblazeB2Client"
        else:
            root = Path.home() / ".config" / "BackblazeB2Client"
        return root / "settings.json"

    def load(self) -> Dict:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save(self, data: Dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(data, ensure_ascii=True, indent=2), encoding="utf-8")


class HistoryStore:
    def __init__(self, settings_store: SettingsStore) -> None:
        self.path = settings_store.path.parent / "history.jsonl"
        self._lock = threading.Lock()

    def append(self, action: str, status: str, details: str, bytes_count: int = 0) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "action": action,
            "status": status,
            "details": details,
            "bytes": int(max(0, bytes_count)),
        }
        line = json.dumps(row, ensure_ascii=True)
        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")

    def tail(self, max_rows: int = 300) -> List[Dict]:
        if not self.path.exists():
            return []
        lines = self.path.read_text(encoding="utf-8").splitlines()[-max_rows:]
        rows: List[Dict] = []
        for line in lines:
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
        return rows


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

        layout = QVBoxLayout(self)
        self.status_label = QLabel("Loading preview...")
        layout.addWidget(self.status_label)

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

    def closeEvent(self, event) -> None:
        self.player.stop()
        super().closeEvent(event)

    def show_image(self, file_name: str, data: bytes) -> None:
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


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("PlayUA Desktop Client")
        self.resize(1240, 780)
        self.setAcceptDrops(True)

        self.client = BackblazeB2Client()
        self.settings_store = SettingsStore()
        self.history_store = HistoryStore(self.settings_store)
        self.last_auth_key = None

        self.file_rows = []
        self.filtered_rows = []
        self.browser_rows: List[Dict] = []
        self.base_bucket_prefix = ""
        self.current_folder_prefix = ""
        self.share_rows: List[Dict] = []
        self.selected_upload_items: List[Tuple[str, str, int]] = []
        self._workers = []
        self.theme_mode = "dark"
        self.transfer_pause = threading.Event()
        self.transfer_stop = threading.Event()
        self.transfer_active = False
        self.transfer_background = False
        self.profiles: Dict[str, Dict] = {}
        self.active_profile_name = "Default"
        self.update_repo = DEFAULT_UPDATE_REPO
        self._preview_windows: List[PreviewDialog] = []

        self._build_ui()
        self._set_human_friendly_defaults()
        self._load_settings()
        self._refresh_queue_table()
        self._refresh_history_table()

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        main = QVBoxLayout(root)
        main.setContentsMargins(18, 14, 18, 14)
        main.setSpacing(10)

        self.title_label = QLabel("PlayUA Desktop Client")
        self.title_label.setObjectName("titleLabel")
        main.addWidget(self.title_label)
        self.subtitle_label = QLabel("Upload files and folders, track progress, and manage direct links in one app.")
        self.subtitle_label.setObjectName("subtitleLabel")
        main.addWidget(self.subtitle_label)
        self.version_label = QLabel(f"Version {APP_VERSION}")
        self.version_label.setObjectName("subtitleLabel")
        main.addWidget(self.version_label)

        body_splitter = QSplitter(Qt.Horizontal)
        main.addWidget(body_splitter, 1)

        left_panel = QWidget()
        left_col = QVBoxLayout(left_panel)
        left_col.setContentsMargins(0, 0, 0, 0)
        left_col.setSpacing(10)

        right_panel = QWidget()
        right_col = QVBoxLayout(right_panel)
        right_col.setContentsMargins(0, 0, 0, 0)
        right_col.setSpacing(10)

        body_splitter.addWidget(left_panel)
        body_splitter.addWidget(right_panel)
        body_splitter.setStretchFactor(0, 4)
        body_splitter.setStretchFactor(1, 6)

        connection_group = QGroupBox("Backblaze Connection")
        left_col.addWidget(connection_group)
        grid = QGridLayout(connection_group)
        grid.setHorizontalSpacing(10)
        grid.setVerticalSpacing(8)

        self.key_id_input = QLineEdit()
        self.app_key_input = QLineEdit()
        self.app_key_input.setEchoMode(QLineEdit.Password)
        self.bucket_id_input = QLineEdit()
        self.bucket_name_input = QLineEdit()
        self.prefix_input = QLineEdit()
        self.ttl_input = QLineEdit("3600")
        self.remember_check = QCheckBox("Remember settings (includes key)")
        self.remember_check.setChecked(True)
        self.dark_theme_check = QCheckBox("Dark theme")
        self.dark_theme_check.setChecked(True)
        self.profile_combo = QComboBox()
        self.profile_save_btn = QPushButton("Save Profile")
        self.profile_delete_btn = QPushButton("Delete Profile")

        grid.addWidget(QLabel("Application Key ID"), 0, 0)
        grid.addWidget(self.key_id_input, 0, 1)
        grid.addWidget(QLabel("Bucket ID"), 0, 2)
        grid.addWidget(self.bucket_id_input, 0, 3)

        grid.addWidget(QLabel("Application Key"), 1, 0)
        grid.addWidget(self.app_key_input, 1, 1)
        grid.addWidget(QLabel("Bucket Name"), 1, 2)
        grid.addWidget(self.bucket_name_input, 1, 3)

        grid.addWidget(QLabel("Prefix (optional)"), 2, 0)
        grid.addWidget(self.prefix_input, 2, 1)
        grid.addWidget(QLabel("Private URL TTL (sec)"), 2, 2)
        grid.addWidget(self.ttl_input, 2, 3)

        grid.addWidget(self.remember_check, 3, 0, 1, 2)
        grid.addWidget(self.dark_theme_check, 3, 2, 1, 2)
        grid.addWidget(QLabel("Profile"), 4, 0)
        grid.addWidget(self.profile_combo, 4, 1)
        profile_row = QHBoxLayout()
        profile_row.addWidget(self.profile_save_btn)
        profile_row.addWidget(self.profile_delete_btn)
        grid.addLayout(profile_row, 4, 2, 1, 2)

        row1 = QHBoxLayout()
        row1.setSpacing(8)
        left_col.addLayout(row1)

        self.save_btn = QPushButton("Save Settings")
        self.auth_btn = QPushButton("Authorize")
        self.select_files_btn = QPushButton("Select Files")
        self.select_folder_btn = QPushButton("Select Folder")
        self.clear_selection_btn = QPushButton("Clear Selection")
        self.upload_btn = QPushButton("Upload")
        self.download_btn = QPushButton("Download")
        self.refresh_btn = QPushButton("Refresh")
        self.more_btn = QToolButton()
        self.more_btn.setText("More")
        self.more_btn.setPopupMode(QToolButton.InstantPopup)
        self.sync_btn = QPushButton("Sync Folder -> Prefix")
        self.pause_btn = QPushButton("Pause")
        self.resume_btn = QPushButton("Resume")
        self.stop_btn = QPushButton("Stop")
        self.background_check = QCheckBox("Background transfers")

        row1.addWidget(self.auth_btn)
        row1.addWidget(self.upload_btn)
        row1.addWidget(self.download_btn)
        row1.addWidget(self.more_btn)
        row1.addStretch(1)

        self.copy_public_btn = QPushButton("Copy Public Link")
        self.open_public_btn = QPushButton("Open Public Link")
        self.copy_private_btn = QPushButton("Copy Private Link")
        self.open_private_btn = QPushButton("Open Private Link")
        self.download_selected_btn = QPushButton("Download Selected")
        self.download_folder_btn = QPushButton("Download Folder")

        self.save_btn.setVisible(False)
        self.select_files_btn.setVisible(False)
        self.select_folder_btn.setVisible(False)
        self.copy_public_btn.setVisible(False)
        self.open_public_btn.setVisible(False)
        self.copy_private_btn.setVisible(False)
        self.open_private_btn.setVisible(False)
        self.download_selected_btn.setVisible(False)
        self.download_folder_btn.setVisible(False)
        self.sync_btn.setVisible(False)
        self.pause_btn.setVisible(False)
        self.resume_btn.setVisible(False)
        self.stop_btn.setVisible(False)
        self.background_check.setVisible(False)

        queue_group = QGroupBox("Upload Queue")
        left_col.addWidget(queue_group, 1)
        queue_layout = QVBoxLayout(queue_group)
        queue_layout.setSpacing(8)

        self.upload_selection_label = QLabel("No files selected")
        queue_layout.addWidget(self.upload_selection_label)

        self.queue_hint_label = QLabel("Items that will be uploaded")
        self.queue_hint_label.setObjectName("sectionLabel")
        queue_layout.addWidget(self.queue_hint_label)

        self.queue_table = QTableWidget(0, 2)
        self.queue_table.setHorizontalHeaderLabels(["Target Path in Bucket", "Size"])
        self.queue_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.queue_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.queue_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.queue_table.setSelectionMode(QAbstractItemView.MultiSelection)
        self.queue_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.queue_table.setAlternatingRowColors(True)
        queue_layout.addWidget(self.queue_table, 1)

        queue_actions = QHBoxLayout()
        self.remove_selected_btn = QPushButton("Remove Selected From Queue")
        queue_actions.addWidget(self.remove_selected_btn)
        queue_layout.addLayout(queue_actions)

        right_tabs = QTabWidget()
        right_col.addWidget(right_tabs, 1)

        files_group = QGroupBox("Files In Bucket")
        files_layout = QVBoxLayout(files_group)
        files_layout.setSpacing(8)

        filters_row = QHBoxLayout()
        self.folder_back_btn = QPushButton("Back")
        self.folder_back_btn.setEnabled(False)
        self.breadcrumb_container = QWidget()
        self.breadcrumb_layout = QHBoxLayout(self.breadcrumb_container)
        self.breadcrumb_layout.setContentsMargins(0, 0, 0, 0)
        self.breadcrumb_layout.setSpacing(4)
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search file name...")
        self.type_filter = QComboBox()
        self.type_filter.addItems(["All types", "Images", "Video", "Audio", "Documents", "Archives"])
        self.size_filter = QComboBox()
        self.size_filter.addItems(["Any size", "< 10 MB", "10-100 MB", "100 MB - 1 GB", "> 1 GB"])
        self.download_folder_current_btn = QPushButton("Download Folder")
        filters_row.addWidget(self.folder_back_btn)
        filters_row.addWidget(self.breadcrumb_container, 2)
        filters_row.addWidget(self.search_input, 2)
        filters_row.addWidget(self.type_filter, 1)
        filters_row.addWidget(self.size_filter, 1)
        filters_row.addWidget(self.download_folder_current_btn)
        filters_row.addStretch(1)
        filters_row.addWidget(self.refresh_btn)
        files_layout.addLayout(filters_row)

        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["Name", "Type", "Size", "Uploaded (UTC)", "Preview", "Download"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.ResizeToContents)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        files_layout.addWidget(self.table, 1)
        right_tabs.addTab(files_group, "Files")

        shares_group = QGroupBox("Share Manager")
        shares_layout = QVBoxLayout(shares_group)
        self.share_table = QTableWidget(0, 5)
        self.share_table.setHorizontalHeaderLabels(["File", "Type", "Created (UTC)", "Expires", "URL"])
        self.share_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.share_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.share_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.share_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.share_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.share_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.share_table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.share_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.share_table.setAlternatingRowColors(True)
        shares_layout.addWidget(self.share_table)

        share_actions = QHBoxLayout()
        self.copy_share_btn = QPushButton("Copy Selected Share URL")
        self.open_share_btn = QPushButton("Open Selected Share URL")
        share_actions.addWidget(self.copy_share_btn)
        share_actions.addWidget(self.open_share_btn)
        shares_layout.addLayout(share_actions)
        right_tabs.addTab(shares_group, "Shares")

        history_group = QGroupBox("Transfer History")
        history_layout = QVBoxLayout(history_group)
        self.history_table = QTableWidget(0, 5)
        self.history_table.setHorizontalHeaderLabels(["Time (UTC)", "Action", "Status", "Size", "Details"])
        self.history_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.history_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.history_table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.history_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.history_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.history_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.history_table.setAlternatingRowColors(True)
        history_layout.addWidget(self.history_table)
        right_tabs.addTab(history_group, "History")

        progress_group = QGroupBox("Current Operation")
        main.addWidget(progress_group)
        progress_layout = QVBoxLayout(progress_group)
        progress_layout.setSpacing(6)

        self.status_label = QLabel("Ready")
        progress_layout.addWidget(self.status_label)
        self.progress_label = QLabel("Idle")
        progress_layout.addWidget(self.progress_label)
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        progress_layout.addWidget(self.progress_bar)

        self.save_btn.clicked.connect(self.save_settings)
        self.auth_btn.clicked.connect(self.authorize)
        self.select_files_btn.clicked.connect(self.select_files)
        self.select_folder_btn.clicked.connect(self.select_folder)
        self.clear_selection_btn.clicked.connect(self.clear_upload_selection)
        self.upload_btn.clicked.connect(self.upload_selected_file)
        self.download_btn.clicked.connect(self.download_selected_files)
        self.refresh_btn.clicked.connect(self.refresh_files)
        self.sync_btn.clicked.connect(self.sync_folder_to_prefix)
        self.pause_btn.clicked.connect(self.pause_transfer)
        self.resume_btn.clicked.connect(self.resume_transfer)
        self.stop_btn.clicked.connect(self.stop_transfer)
        self.copy_public_btn.clicked.connect(self.copy_public_link)
        self.open_public_btn.clicked.connect(self.open_public_link)
        self.copy_private_btn.clicked.connect(self.copy_private_link)
        self.open_private_btn.clicked.connect(self.open_private_link)
        self.download_selected_btn.clicked.connect(self.download_selected_files)
        self.download_folder_btn.clicked.connect(self.download_folder_by_prefix)
        self.remove_selected_btn.clicked.connect(self.remove_selected_upload_items)
        self.dark_theme_check.toggled.connect(self._on_theme_toggled)
        self.profile_combo.currentTextChanged.connect(self._on_profile_changed)
        self.profile_save_btn.clicked.connect(self.save_profile)
        self.profile_delete_btn.clicked.connect(self.delete_profile)
        self.search_input.textChanged.connect(self._apply_filters)
        self.type_filter.currentIndexChanged.connect(self._apply_filters)
        self.size_filter.currentIndexChanged.connect(self._apply_filters)
        self.table.itemSelectionChanged.connect(self._update_bucket_actions_state)
        self.table.itemDoubleClicked.connect(self._on_table_item_double_clicked)
        self.folder_back_btn.clicked.connect(self.open_parent_folder)
        self.download_folder_current_btn.clicked.connect(self.download_current_folder)
        self.copy_share_btn.clicked.connect(self.copy_selected_share_url)
        self.open_share_btn.clicked.connect(self.open_selected_share_url)

        self._setup_context_menus()
        self._setup_more_menu()

    def _set_human_friendly_defaults(self) -> None:
        self.auth_btn.setObjectName("primaryBtn")
        self.upload_btn.setObjectName("primaryBtn")
        self.download_btn.setObjectName("secondaryBtn")
        self.more_btn.setObjectName("secondaryBtn")
        self.pause_btn.setObjectName("secondaryBtn")
        self.resume_btn.setObjectName("secondaryBtn")
        self.stop_btn.setObjectName("dangerBtn")
        self.refresh_btn.setObjectName("secondaryBtn")
        self.sync_btn.setObjectName("secondaryBtn")
        self.copy_public_btn.setObjectName("secondaryBtn")
        self.open_public_btn.setObjectName("secondaryBtn")
        self.copy_private_btn.setObjectName("secondaryBtn")
        self.open_private_btn.setObjectName("secondaryBtn")
        self.download_selected_btn.setObjectName("secondaryBtn")
        self.download_folder_btn.setObjectName("secondaryBtn")
        self.remove_selected_btn.setObjectName("dangerBtn")
        self.folder_back_btn.setObjectName("secondaryBtn")
        self.download_folder_current_btn.setObjectName("secondaryBtn")
        self.profile_save_btn.setObjectName("secondaryBtn")
        self.profile_delete_btn.setObjectName("dangerBtn")
        self._configure_hints()
        self._polish_tables()
        self.resume_btn.setEnabled(False)
        self.pause_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)
        self.download_btn.setEnabled(False)
        self._update_bucket_actions_state()

        self._apply_theme("dark")

    def _apply_theme(self, theme: str) -> None:
        self.theme_mode = theme
        if theme == "light":
            self.setStyleSheet(
                """
                QWidget { font-size: 13px; color: #1f2937; }
                QMainWindow, QWidget { background: #f5f7fb; }
                #titleLabel { font-size: 24px; font-weight: 700; margin-bottom: 2px; color: #0f172a; }
                #subtitleLabel { color: #64748b; margin-bottom: 8px; }
                #sectionLabel { font-weight: 600; color: #475569; }
                QGroupBox {
                  border: 1px solid #dbe3f0;
                  border-radius: 12px;
                  margin-top: 8px;
                  background: white;
                  font-weight: 600;
                  padding-top: 12px;
                }
                QGroupBox::title {
                  subcontrol-origin: margin;
                  left: 10px;
                  padding: 0 6px 0 6px;
                  color: #0f172a;
                  background: white;
                }
                QLineEdit {
                  padding: 7px 9px;
                  border: 1px solid #cbd5e1;
                  border-radius: 8px;
                  background: #ffffff;
                }
                QLineEdit:focus { border: 1px solid #60a5fa; }
                QPushButton, QToolButton {
                  padding: 7px 12px;
                  border-radius: 8px;
                  border: 1px solid #cbd5e1;
                  background: #ffffff;
                }
                QPushButton:hover, QToolButton:hover { background: #f8fafc; }
                QPushButton#primaryBtn, QToolButton#primaryBtn {
                  background: #2563eb;
                  border: 1px solid #2563eb;
                  color: white;
                  font-weight: 600;
                }
                QPushButton#primaryBtn:hover, QToolButton#primaryBtn:hover { background: #1e4fd8; }
                QPushButton#secondaryBtn, QToolButton#secondaryBtn {
                  background: #0f766e;
                  border: 1px solid #0f766e;
                  color: white;
                  font-weight: 600;
                }
                QPushButton#secondaryBtn:hover, QToolButton#secondaryBtn:hover { background: #0d635c; }
                QPushButton#dangerBtn, QToolButton#dangerBtn {
                  background: #fef2f2;
                  color: #b91c1c;
                  border: 1px solid #fecaca;
                }
                QPushButton:disabled, QToolButton:disabled {
                  color: #9ca3af;
                  background: #f1f5f9;
                  border: 1px solid #e2e8f0;
                }
                QTableWidget {
                  border: 1px solid #e2e8f0;
                  border-radius: 8px;
                  gridline-color: #eef2f7;
                  background: #ffffff;
                  color: #1f2937;
                  alternate-background-color: #f8fafc;
                }
                QHeaderView::section {
                  background: #f1f5f9;
                  border: 0;
                  border-bottom: 1px solid #e2e8f0;
                  padding: 6px;
                  font-weight: 600;
                  color: #334155;
                }
                QTabWidget::pane {
                  border: 1px solid #dbe3f0;
                  border-radius: 10px;
                  top: -1px;
                  background: #ffffff;
                }
                QTabBar::tab {
                  background: #eef2ff;
                  color: #334155;
                  border: 1px solid #dbe3f0;
                  border-bottom: none;
                  border-top-left-radius: 8px;
                  border-top-right-radius: 8px;
                  padding: 6px 12px;
                  margin-right: 4px;
                }
                QTabBar::tab:selected {
                  background: #ffffff;
                  color: #0f172a;
                }
                QProgressBar {
                  min-height: 18px;
                  border: 1px solid #cbd5e1;
                  border-radius: 8px;
                  background: #eef2ff;
                  text-align: center;
                  color: #334155;
                }
                QProgressBar::chunk {
                  border-radius: 8px;
                  background: #3b82f6;
                }
                """
            )
            return

        self.setStyleSheet(
            """
            QWidget { font-size: 13px; color: #e5e7eb; }
            QMainWindow, QWidget { background: #0b0b0d; }
            #titleLabel { font-size: 24px; font-weight: 700; margin-bottom: 2px; color: #f8fafc; }
            #subtitleLabel { color: #9ca3af; margin-bottom: 8px; }
            #sectionLabel { font-weight: 600; color: #cbd5e1; }
            QGroupBox {
              border: 1px solid #2a2a2f;
              border-radius: 12px;
              margin-top: 8px;
              background: #141418;
              font-weight: 600;
              padding-top: 12px;
            }
            QGroupBox::title {
              subcontrol-origin: margin;
              left: 10px;
              padding: 0 6px 0 6px;
              color: #f3f4f6;
              background: #141418;
            }
            QLineEdit {
              padding: 7px 9px;
              border: 1px solid #34343c;
              border-radius: 8px;
              background: #111318;
              color: #f3f4f6;
            }
            QLineEdit:focus { border: 1px solid #60a5fa; }
            QPushButton, QToolButton {
              padding: 7px 12px;
              border-radius: 8px;
              border: 1px solid #3f3f46;
              background: #1b1b21;
              color: #e5e7eb;
            }
            QPushButton:hover, QToolButton:hover { background: #23232b; }
            QPushButton#primaryBtn, QToolButton#primaryBtn {
              background: #2563eb;
              border: 1px solid #2563eb;
              color: white;
              font-weight: 600;
            }
            QPushButton#primaryBtn:hover, QToolButton#primaryBtn:hover { background: #1e4fd8; }
            QPushButton#secondaryBtn, QToolButton#secondaryBtn {
              background: #0f766e;
              border: 1px solid #0f766e;
              color: white;
              font-weight: 600;
            }
            QPushButton#secondaryBtn:hover, QToolButton#secondaryBtn:hover { background: #0d635c; }
            QPushButton#dangerBtn, QToolButton#dangerBtn {
              background: #3a1113;
              color: #fecaca;
              border: 1px solid #7f1d1d;
            }
            QPushButton:disabled, QToolButton:disabled {
              color: #6b7280;
              background: #16161b;
              border: 1px solid #2a2a2f;
            }
            QTableWidget {
              border: 1px solid #2f2f37;
              border-radius: 8px;
              gridline-color: #26262e;
              background: #101217;
              color: #e5e7eb;
              alternate-background-color: #161923;
            }
            QHeaderView::section {
              background: #1c1f2a;
              border: 0;
              border-bottom: 1px solid #2f2f37;
              padding: 6px;
              font-weight: 600;
              color: #f3f4f6;
            }
            QTabWidget::pane {
              border: 1px solid #2a2a2f;
              border-radius: 10px;
              top: -1px;
              background: #141418;
            }
            QTabBar::tab {
              background: #1c2230;
              color: #cbd5e1;
              border: 1px solid #2f3544;
              border-bottom: none;
              border-top-left-radius: 8px;
              border-top-right-radius: 8px;
              padding: 6px 12px;
              margin-right: 4px;
            }
            QTabBar::tab:selected {
              background: #141418;
              color: #f8fafc;
            }
            QProgressBar {
              min-height: 18px;
              border: 1px solid #2f2f37;
              border-radius: 8px;
              background: #141722;
              text-align: center;
              color: #e5e7eb;
            }
            QProgressBar::chunk {
              border-radius: 8px;
              background: #3b82f6;
            }
            """
        )

    def _on_theme_toggled(self, checked: bool) -> None:
        self._apply_theme("dark" if checked else "light")

    def dragEnterEvent(self, event) -> None:
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
            return
        event.ignore()

    def keyPressEvent(self, event) -> None:
        if event.matches(QKeySequence.Find):
            self.search_input.setFocus()
            self.search_input.selectAll()
            event.accept()
            return

        if event.matches(QKeySequence.Refresh):
            self.refresh_files()
            event.accept()
            return

        if event.key() == Qt.Key_Delete:
            if self.queue_table.hasFocus():
                self.remove_selected_upload_items()
                event.accept()
                return

        if event.key() == Qt.Key_Space:
            focused = QApplication.focusWidget()
            if isinstance(focused, QLineEdit):
                super().keyPressEvent(event)
                return
            if self.transfer_active:
                if self.transfer_pause.is_set():
                    self.resume_transfer()
                else:
                    self.pause_transfer()
                event.accept()
                return

        super().keyPressEvent(event)

    def dropEvent(self, event) -> None:
        urls = event.mimeData().urls()
        if not urls:
            return
        items: List[Tuple[str, str, int]] = []
        for url in urls:
            path = url.toLocalFile()
            if not path:
                continue
            if os.path.isfile(path):
                size = os.path.getsize(path)
                items.append((path, os.path.basename(path), size))
            elif os.path.isdir(path):
                base_name = os.path.basename(path.rstrip(os.sep))
                for root, _, files in os.walk(path):
                    for file_name in files:
                        local_path = os.path.join(root, file_name)
                        rel_path = os.path.relpath(local_path, path).replace("\\", "/")
                        target_rel = f"{base_name}/{rel_path}"
                        size = os.path.getsize(local_path)
                        items.append((local_path, target_rel, size))
        if items:
            items.sort(key=lambda x: x[1])
            self._merge_upload_items(items)
            self.set_status(f"Added {len(items)} item(s) via drag & drop")
            event.acceptProposedAction()

    def _set_transfer_state(self, active: bool) -> None:
        self.transfer_active = active
        if not active:
            self.transfer_pause.clear()
            self.transfer_stop.clear()
        self.pause_btn.setEnabled(active and not self.transfer_pause.is_set())
        self.resume_btn.setEnabled(active and self.transfer_pause.is_set())
        self.stop_btn.setEnabled(active)

    def pause_transfer(self) -> None:
        if not self.transfer_active:
            return
        self.transfer_pause.set()
        self.pause_btn.setEnabled(False)
        self.resume_btn.setEnabled(True)
        self.progress_label.setText("Paused")

    def resume_transfer(self) -> None:
        if not self.transfer_active:
            return
        self.transfer_pause.clear()
        self.pause_btn.setEnabled(True)
        self.resume_btn.setEnabled(False)

    def stop_transfer(self) -> None:
        if not self.transfer_active:
            return
        self.transfer_stop.set()
        self.transfer_pause.clear()
        self.pause_btn.setEnabled(False)
        self.resume_btn.setEnabled(False)
        self.progress_label.setText("Stopping...")

    def _wait_if_paused(self) -> None:
        while self.transfer_pause.is_set() and not self.transfer_stop.is_set():
            time.sleep(0.1)

    def _should_stop_transfer(self) -> bool:
        return self.transfer_stop.is_set()

    def _append_history(self, action: str, status: str, details: str, bytes_count: int = 0) -> None:
        self.history_store.append(action, status, details, bytes_count)
        self._refresh_history_table()

    def _refresh_history_table(self) -> None:
        rows = self.history_store.tail(120)
        rows.reverse()
        self.history_table.setRowCount(len(rows))
        for i, row in enumerate(rows):
            ts_raw = str(row.get("ts", ""))
            ts = ts_raw.replace("T", " ").replace("+00:00", "")
            action = str(row.get("action", ""))
            status = str(row.get("status", ""))
            details = str(row.get("details", ""))
            bytes_count = int(row.get("bytes", 0) or 0)
            self.history_table.setItem(i, 0, QTableWidgetItem(ts))
            self.history_table.setItem(i, 1, QTableWidgetItem(action))
            self.history_table.setItem(i, 2, QTableWidgetItem(status))
            self.history_table.setItem(i, 3, QTableWidgetItem(format_bytes(bytes_count)))
            self.history_table.setItem(i, 4, QTableWidgetItem(details))

    def _append_share(self, file_name: str, link_type: str, url: str, ttl_seconds: Optional[int]) -> None:
        created = dt.datetime.now(dt.timezone.utc)
        expires = ""
        if ttl_seconds:
            expires = (created + dt.timedelta(seconds=ttl_seconds)).strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "file": file_name,
            "type": link_type,
            "created": created.strftime("%Y-%m-%d %H:%M:%S"),
            "expires": expires,
            "url": url,
        }
        self.share_rows.insert(0, row)
        self.share_rows = self.share_rows[:300]
        self._refresh_share_table()

    def _refresh_share_table(self) -> None:
        self.share_table.setRowCount(len(self.share_rows))
        for i, row in enumerate(self.share_rows):
            self.share_table.setItem(i, 0, QTableWidgetItem(row["file"]))
            self.share_table.setItem(i, 1, QTableWidgetItem(row["type"]))
            self.share_table.setItem(i, 2, QTableWidgetItem(row["created"]))
            self.share_table.setItem(i, 3, QTableWidgetItem(row["expires"]))
            url_item = QTableWidgetItem(row["url"])
            url_item.setToolTip(row["url"])
            self.share_table.setItem(i, 4, url_item)

    def _selected_share_url(self) -> Optional[str]:
        row = self.share_table.currentRow()
        if row < 0 or row >= len(self.share_rows):
            return None
        return self.share_rows[row]["url"]

    def copy_selected_share_url(self) -> None:
        url = self._selected_share_url()
        if not url:
            QMessageBox.information(self, "Share Manager", "Select a row in Share Manager.")
            return
        self._copy_text(url)
        self.set_status("Share URL copied")

    def open_selected_share_url(self) -> None:
        url = self._selected_share_url()
        if not url:
            QMessageBox.information(self, "Share Manager", "Select a row in Share Manager.")
            return
        QDesktopServices.openUrl(QUrl(url))
        self.set_status("Share URL opened")

    def _notify_transfer_done(self, title: str, message: str) -> None:
        if self.background_check.isChecked():
            QApplication.alert(self, 3000)
            return
        QMessageBox.information(self, title, message)

    def _update_bucket_actions_state(self) -> None:
        has_selection = bool(self._selected_file_names())
        self.download_btn.setEnabled(has_selection)
        target_role = "primaryBtn" if has_selection else "secondaryBtn"
        if self.download_btn.objectName() != target_role:
            self.download_btn.setObjectName(target_role)
            self.download_btn.style().unpolish(self.download_btn)
            self.download_btn.style().polish(self.download_btn)
            self.download_btn.update()

    def _configure_hints(self) -> None:
        self.key_id_input.setPlaceholderText("e.g. 004a7f3f...")
        self.app_key_input.setPlaceholderText("Application Key")
        self.bucket_id_input.setPlaceholderText("Bucket ID")
        self.bucket_name_input.setPlaceholderText("Bucket name")
        self.prefix_input.setPlaceholderText("optional/path")
        self.ttl_input.setPlaceholderText("3600")

        self.select_files_btn.setToolTip("Pick one or more individual files.")
        self.select_folder_btn.setToolTip("Pick a folder. All files inside will be uploaded recursively.")
        self.upload_btn.setToolTip("Start uploading all queued items.")
        self.download_btn.setToolTip("Download selected files from bucket table.")
        self.more_btn.setToolTip("More actions and advanced controls.")
        self.clear_selection_btn.setToolTip("Clear the whole upload queue.")
        self.remove_selected_btn.setToolTip("Remove selected rows from upload queue preview.")
        self.download_selected_btn.setToolTip("Download selected files from bucket to a local folder.")
        self.download_folder_btn.setToolTip("Download all files by prefix from bucket.")
        self.folder_back_btn.setToolTip("Go to parent folder in bucket browser.")
        self.download_folder_current_btn.setToolTip("Download currently opened folder from bucket.")
        self.pause_btn.setToolTip("Pause current transfer (upload/download).")
        self.resume_btn.setToolTip("Resume paused transfer.")
        self.stop_btn.setToolTip("Stop current transfer.")
        self.sync_btn.setToolTip("Choose local folder and upload missing/changed files into selected prefix.")
        self.profile_save_btn.setToolTip("Save current connection fields into selected profile.")
        self.profile_delete_btn.setToolTip("Delete selected profile.")

    def _polish_tables(self) -> None:
        for t in [self.queue_table, self.table, self.history_table]:
            t.verticalHeader().setVisible(False)
            t.setShowGrid(False)
            t.setWordWrap(False)
        self.history_table.setSortingEnabled(True)
        self.table.setSortingEnabled(False)

    def _setup_context_menus(self) -> None:
        self.queue_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.share_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.history_table.setContextMenuPolicy(Qt.CustomContextMenu)

        self.queue_table.customContextMenuRequested.connect(self._show_queue_context_menu)
        self.table.customContextMenuRequested.connect(self._show_files_context_menu)
        self.share_table.customContextMenuRequested.connect(self._show_share_context_menu)
        self.history_table.customContextMenuRequested.connect(self._show_history_context_menu)

    def _setup_more_menu(self) -> None:
        menu = QMenu(self)
        self.more_btn.setMenu(menu)

        self.more_save_action = QAction("Save Settings", self)
        self.more_select_files_action = QAction("Select Files", self)
        self.more_select_folder_action = QAction("Select Folder", self)
        self.more_clear_queue_action = QAction("Clear Queue", self)
        self.more_download_folder_action = QAction("Download Folder by Prefix", self)
        self.more_sync_action = QAction("Sync Folder -> Prefix", self)
        self.more_check_updates_action = QAction("Check for Updates", self)
        self.more_set_update_repo_action = QAction("Set Update Repo", self)
        self.more_profile_new_action = QAction("Create Profile", self)
        self.more_pause_action = QAction("Pause Transfer", self)
        self.more_resume_action = QAction("Resume Transfer", self)
        self.more_stop_action = QAction("Stop Transfer", self)
        self.more_background_action = QAction("Background Transfers", self)
        self.more_background_action.setCheckable(True)

        menu.addAction(self.more_save_action)
        menu.addSeparator()
        menu.addAction(self.more_select_files_action)
        menu.addAction(self.more_select_folder_action)
        menu.addAction(self.more_clear_queue_action)
        menu.addSeparator()
        menu.addAction(self.more_download_folder_action)
        menu.addAction(self.more_sync_action)
        menu.addSeparator()
        menu.addAction(self.more_profile_new_action)
        menu.addSeparator()
        menu.addAction(self.more_check_updates_action)
        menu.addAction(self.more_set_update_repo_action)
        menu.addSeparator()
        menu.addAction(self.more_pause_action)
        menu.addAction(self.more_resume_action)
        menu.addAction(self.more_stop_action)
        menu.addSeparator()
        menu.addAction(self.more_background_action)

        self.more_save_action.triggered.connect(self.save_settings)
        self.more_select_files_action.triggered.connect(self.select_files)
        self.more_select_folder_action.triggered.connect(self.select_folder)
        self.more_clear_queue_action.triggered.connect(self.clear_upload_selection)
        self.more_download_folder_action.triggered.connect(self.download_folder_by_prefix)
        self.more_sync_action.triggered.connect(self.sync_folder_to_prefix)
        self.more_check_updates_action.triggered.connect(self.check_for_updates)
        self.more_set_update_repo_action.triggered.connect(self.set_update_repo)
        self.more_profile_new_action.triggered.connect(self.create_profile)
        self.more_pause_action.triggered.connect(self.pause_transfer)
        self.more_resume_action.triggered.connect(self.resume_transfer)
        self.more_stop_action.triggered.connect(self.stop_transfer)
        self.more_background_action.toggled.connect(self.background_check.setChecked)
        self.background_check.toggled.connect(self.more_background_action.setChecked)

        menu.aboutToShow.connect(self._sync_more_menu_state)

    def _sync_more_menu_state(self) -> None:
        self.more_clear_queue_action.setEnabled(bool(self.selected_upload_items))
        self.more_pause_action.setEnabled(self.transfer_active and not self.transfer_pause.is_set())
        self.more_resume_action.setEnabled(self.transfer_active and self.transfer_pause.is_set())
        self.more_stop_action.setEnabled(self.transfer_active)
        self.more_background_action.setChecked(self.background_check.isChecked())
        self.more_profile_new_action.setEnabled(True)

    def _select_row_at_context(self, table: QTableWidget, pos) -> None:
        item = table.itemAt(pos)
        if item:
            table.selectRow(item.row())

    def _show_queue_context_menu(self, pos) -> None:
        self._select_row_at_context(self.queue_table, pos)
        menu = QMenu(self)
        act_remove = menu.addAction("Remove Selected")
        act_clear = menu.addAction("Clear Queue")
        act_remove.setEnabled(bool(self.queue_table.selectionModel().selectedRows()))
        act_clear.setEnabled(bool(self.selected_upload_items))
        chosen = menu.exec(self.queue_table.viewport().mapToGlobal(pos))
        if chosen == act_remove:
            self.remove_selected_upload_items()
        elif chosen == act_clear:
            self.clear_upload_selection()

    def _show_files_context_menu(self, pos) -> None:
        self._select_row_at_context(self.table, pos)
        selected = self._selected_file_names()
        has_selection = bool(selected)
        current_item = self.table.item(self.table.currentRow(), 0) if self.table.currentRow() >= 0 else None
        is_folder_selected = bool(current_item and current_item.data(Qt.UserRole + 1) == "folder")
        selected_folder = str(current_item.data(Qt.UserRole) or "") if is_folder_selected else ""

        menu = QMenu(self)
        act_copy_public = menu.addAction("Copy Public Link")
        act_open_public = menu.addAction("Open Public Link")
        act_copy_private = menu.addAction("Copy Private Link")
        act_open_private = menu.addAction("Open Private Link")
        act_preview = menu.addAction("Preview Selected")
        menu.addSeparator()
        act_download = menu.addAction("Download Folder" if is_folder_selected else "Download Selected")
        act_refresh = menu.addAction("Refresh List")

        for action in [act_copy_public, act_open_public, act_copy_private, act_open_private, act_preview]:
            action.setEnabled(has_selection)
        act_download.setEnabled(has_selection or is_folder_selected)

        chosen = menu.exec(self.table.viewport().mapToGlobal(pos))
        if chosen == act_copy_public:
            self.copy_public_link()
        elif chosen == act_open_public:
            self.open_public_link()
        elif chosen == act_copy_private:
            self.copy_private_link()
        elif chosen == act_open_private:
            self.open_private_link()
        elif chosen == act_preview:
            self.preview_selected_file()
        elif chosen == act_download:
            if is_folder_selected and selected_folder:
                self.download_folder_by_prefix(prefix_override=selected_folder)
            else:
                self.download_selected_files()
        elif chosen == act_refresh:
            self.refresh_files()

    def _show_share_context_menu(self, pos) -> None:
        self._select_row_at_context(self.share_table, pos)
        menu = QMenu(self)
        act_copy = menu.addAction("Copy Share URL")
        act_open = menu.addAction("Open Share URL")
        has_selection = self._selected_share_url() is not None
        act_copy.setEnabled(has_selection)
        act_open.setEnabled(has_selection)
        chosen = menu.exec(self.share_table.viewport().mapToGlobal(pos))
        if chosen == act_copy:
            self.copy_selected_share_url()
        elif chosen == act_open:
            self.open_selected_share_url()

    def _show_history_context_menu(self, pos) -> None:
        self._select_row_at_context(self.history_table, pos)
        row = self.history_table.currentRow()
        menu = QMenu(self)
        act_copy_row = menu.addAction("Copy Row")
        act_copy_row.setEnabled(row >= 0)
        chosen = menu.exec(self.history_table.viewport().mapToGlobal(pos))
        if chosen != act_copy_row or row < 0:
            return
        values = []
        for col in range(self.history_table.columnCount()):
            item = self.history_table.item(row, col)
            values.append(item.text() if item else "")
        self._copy_text(" | ".join(values))
        self.set_status("History row copied")

    def set_status(self, text: str) -> None:
        self.status_label.setText(text)

    def _profile_payload_from_fields(self) -> Dict:
        return {
            "key_id": self.key_id_input.text().strip(),
            "app_key": self.app_key_input.text().strip(),
            "bucket_id": self.bucket_id_input.text().strip(),
            "bucket_name": self.bucket_name_input.text().strip(),
            "prefix": self.prefix_input.text().strip(),
            "private_ttl": self.ttl_input.text().strip() or "3600",
        }

    def _apply_profile_payload(self, payload: Dict) -> None:
        self.key_id_input.setText(str(payload.get("key_id", "")))
        self.app_key_input.setText(str(payload.get("app_key", "")))
        self.bucket_id_input.setText(str(payload.get("bucket_id", "")))
        self.bucket_name_input.setText(str(payload.get("bucket_name", "")))
        self.prefix_input.setText(str(payload.get("prefix", "")))
        self.ttl_input.setText(str(payload.get("private_ttl", "3600")))

    def _refresh_profile_combo(self) -> None:
        names = sorted(self.profiles.keys())
        if not names:
            self.profiles["Default"] = self._profile_payload_from_fields()
            names = ["Default"]
        if self.active_profile_name not in self.profiles:
            self.active_profile_name = names[0]
        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        self.profile_combo.addItems(names)
        self.profile_combo.setCurrentText(self.active_profile_name)
        self.profile_combo.blockSignals(False)

    def _on_profile_changed(self, name: str) -> None:
        if not name or name not in self.profiles:
            return
        self.active_profile_name = name
        self._apply_profile_payload(self.profiles[name])
        self.set_status(f"Profile loaded: {name}")

    def create_profile(self) -> None:
        name, ok = QInputDialog.getText(self, "Create Profile", "Profile name:")
        if not ok:
            return
        name = name.strip()
        if not name:
            QMessageBox.warning(self, "Profile", "Profile name cannot be empty.")
            return
        if name in self.profiles:
            QMessageBox.warning(self, "Profile", f"Profile already exists: {name}")
            return
        self.profiles[name] = self._profile_payload_from_fields()
        self.active_profile_name = name
        self._refresh_profile_combo()
        self.save_settings()
        self.set_status(f"Profile created: {name}")

    def save_profile(self) -> None:
        name = self.profile_combo.currentText().strip()
        if not name:
            name = "Default"
        self.profiles[name] = self._profile_payload_from_fields()
        self.active_profile_name = name
        self._refresh_profile_combo()
        self.save_settings()
        self.set_status(f"Profile saved: {name}")

    def delete_profile(self) -> None:
        name = self.profile_combo.currentText().strip()
        if not name:
            return
        if name == "Default":
            QMessageBox.warning(self, "Profile", "Default profile cannot be deleted.")
            return
        if name in self.profiles:
            del self.profiles[name]
        if not self.profiles:
            self.profiles["Default"] = self._profile_payload_from_fields()
        self.active_profile_name = sorted(self.profiles.keys())[0]
        self._refresh_profile_combo()
        self._apply_profile_payload(self.profiles[self.active_profile_name])
        self.save_settings()
        self.set_status(f"Profile deleted: {name}")

    def _set_busy(self, busy: bool) -> None:
        if self.transfer_background and self.transfer_active:
            self.pause_btn.setEnabled(self.transfer_active and not self.transfer_pause.is_set())
            self.resume_btn.setEnabled(self.transfer_active and self.transfer_pause.is_set())
            self.stop_btn.setEnabled(self.transfer_active)
            return
        controls = [
            self.save_btn,
            self.auth_btn,
            self.select_files_btn,
            self.select_folder_btn,
            self.clear_selection_btn,
            self.download_btn,
            self.more_btn,
            self.profile_combo,
            self.profile_save_btn,
            self.profile_delete_btn,
            self.remove_selected_btn,
            self.dark_theme_check,
            self.upload_btn,
            self.refresh_btn,
            self.copy_public_btn,
            self.open_public_btn,
            self.copy_private_btn,
            self.open_private_btn,
            self.download_selected_btn,
            self.download_folder_btn,
            self.sync_btn,
            self.search_input,
            self.type_filter,
            self.size_filter,
        ]
        for w in controls:
            w.setEnabled(not busy)

    def _current_config(self) -> Dict:
        return {
            "key_id": self.key_id_input.text().strip(),
            "app_key": self.app_key_input.text().strip(),
            "bucket_id": self.bucket_id_input.text().strip(),
            "bucket_name": self.bucket_name_input.text().strip(),
            "prefix": self.prefix_input.text().strip(),
            "remember": self.remember_check.isChecked(),
            "private_ttl": self.ttl_input.text().strip() or "3600",
            "background": self.background_check.isChecked(),
        }

    def _private_ttl(self) -> int:
        raw = self.ttl_input.text().strip()
        try:
            ttl = int(raw)
        except ValueError as exc:
            raise RuntimeError("Private URL TTL must be an integer.") from exc
        if ttl < 1 or ttl > 604800:
            raise RuntimeError("Private URL TTL must be between 1 and 604800 seconds.")
        return ttl

    def _auth_key(self, cfg: Dict) -> Tuple[str, str]:
        return (cfg["key_id"], cfg["app_key"])

    def _ensure_authorized(self, cfg: Dict) -> None:
        if not cfg["key_id"] or not cfg["app_key"]:
            raise RuntimeError("Fill Application Key ID and Application Key.")

        key = self._auth_key(cfg)
        if key != self.last_auth_key or not self.client.authorization_token:
            self.client.authorize(cfg["key_id"], cfg["app_key"])
            self.last_auth_key = key

    def _run_bg(self, fn, on_success=None, on_progress=None, transfer_job: bool = False, action_name: str = "") -> None:
        signals = WorkerSignals()
        if transfer_job:
            self.transfer_background = self.background_check.isChecked()
            self._set_transfer_state(True)
        self._set_busy(True)

        def handle_success(result: object) -> None:
            try:
                if on_success:
                    on_success(result)
            finally:
                self._set_busy(False)
                if transfer_job:
                    self._set_transfer_state(False)
                self._workers.remove(signals)

        def handle_error(msg: str) -> None:
            try:
                if "stopped by user" in msg.lower():
                    self.set_status("Stopped")
                    self.progress_label.setText("Stopped")
                else:
                    QMessageBox.critical(self, "Error", msg)
                    self.set_status("Error")
                if transfer_job and action_name:
                    self._append_history(action_name, "error", msg)
            finally:
                self._set_busy(False)
                if transfer_job:
                    self._set_transfer_state(False)
                self._workers.remove(signals)

        signals.success.connect(handle_success)
        signals.error.connect(handle_error)
        if on_progress:
            signals.progress.connect(on_progress)
        self._workers.append(signals)

        def worker() -> None:
            try:
                if len(inspect.signature(fn).parameters) > 0:
                    result = fn(lambda p, t: signals.progress.emit(p, t))
                else:
                    result = fn()
                signals.success.emit(result)
            except Exception as exc:
                signals.error.emit(str(exc))

        threading.Thread(target=worker, daemon=True).start()

    def _load_settings(self) -> None:
        data = self.settings_store.load()
        self.update_repo = str(data.get("update_repo", DEFAULT_UPDATE_REPO))
        self.remember_check.setChecked(bool(data.get("remember", True)))
        self.background_check.setChecked(bool(data.get("background", False)))
        self.profiles = dict(data.get("profiles", {})) if isinstance(data.get("profiles", {}), dict) else {}
        if not self.profiles:
            self.profiles = {
                "Default": {
                    "key_id": str(data.get("key_id", "")),
                    "app_key": str(data.get("app_key", "")),
                    "bucket_id": str(data.get("bucket_id", "")),
                    "bucket_name": str(data.get("bucket_name", "")),
                    "prefix": str(data.get("prefix", "")),
                    "private_ttl": str(data.get("private_ttl", 3600)),
                }
            }
        self.active_profile_name = str(data.get("active_profile", "Default"))
        self._refresh_profile_combo()
        self._apply_profile_payload(self.profiles.get(self.active_profile_name, self.profiles["Default"]))
        theme = str(data.get("theme", "dark")).lower()
        if theme not in ("dark", "light"):
            theme = "dark"
        self.dark_theme_check.blockSignals(True)
        self.dark_theme_check.setChecked(theme == "dark")
        self.dark_theme_check.blockSignals(False)
        self._apply_theme(theme)

    def save_settings(self) -> None:
        cfg = self._current_config()
        profile_name = self.profile_combo.currentText().strip() or "Default"
        self.profiles[profile_name] = self._profile_payload_from_fields()
        self.active_profile_name = profile_name
        payload = {
            "key_id": cfg["key_id"],
            "app_key": cfg["app_key"] if cfg["remember"] else "",
            "bucket_id": cfg["bucket_id"],
            "bucket_name": cfg["bucket_name"],
            "prefix": cfg["prefix"],
            "private_ttl": self._private_ttl(),
            "remember": cfg["remember"],
            "theme": "dark" if self.dark_theme_check.isChecked() else "light",
            "background": cfg["background"],
            "profiles": self.profiles,
            "active_profile": self.active_profile_name,
            "update_repo": self.update_repo,
        }
        self.settings_store.save(payload)
        self.set_status(f"Settings saved: {self.settings_store.path}")

    def authorize(self) -> None:
        cfg = self._current_config()
        self.set_status("Authorizing...")

        def task():
            self._ensure_authorized(cfg)
            return None

        def done(_: object) -> None:
            self.save_settings()
            self.set_status("Authorized")
            QMessageBox.information(self, "Success", "Authorized successfully.")

        self._run_bg(task, done)

    def _update_upload_selection_label(self) -> None:
        if not self.selected_upload_items:
            self.upload_selection_label.setText("No files selected")
            return
        total = sum(size for _, _, size in self.selected_upload_items)
        count = len(self.selected_upload_items)
        self.upload_selection_label.setText(f"Selected {count} file(s), total {format_bytes(total)}")

    def _refresh_queue_table(self) -> None:
        self.queue_table.setRowCount(len(self.selected_upload_items))
        for row, (_, target_rel, size) in enumerate(self.selected_upload_items):
            self.queue_table.setItem(row, 0, QTableWidgetItem(target_rel))
            self.queue_table.setItem(row, 1, QTableWidgetItem(format_bytes(size)))

    def _merge_upload_items(self, items: List[Tuple[str, str, int]]) -> None:
        existing = {local_path for local_path, _, _ in self.selected_upload_items}
        for local_path, target_rel, size in items:
            if local_path not in existing:
                self.selected_upload_items.append((local_path, target_rel, size))
                existing.add(local_path)
        self._update_upload_selection_label()
        self._refresh_queue_table()

    def select_files(self) -> None:
        paths, _ = QFileDialog.getOpenFileNames(self, "Select files")
        if not paths:
            return
        items = []
        for path in paths:
            size = os.path.getsize(path)
            target_rel = os.path.basename(path)
            items.append((path, target_rel, size))
        self._merge_upload_items(items)

    def select_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(self, "Select folder")
        if not folder:
            return

        base_name = os.path.basename(folder.rstrip(os.sep))
        items = []
        for root, _, files in os.walk(folder):
            for file_name in files:
                local_path = os.path.join(root, file_name)
                rel_path = os.path.relpath(local_path, folder).replace("\\", "/")
                target_rel = f"{base_name}/{rel_path}"
                size = os.path.getsize(local_path)
                items.append((local_path, target_rel, size))

        if not items:
            QMessageBox.warning(self, "Empty folder", "Selected folder has no files.")
            return

        items.sort(key=lambda x: x[1])
        self._merge_upload_items(items)

    def clear_upload_selection(self) -> None:
        self.selected_upload_items = []
        self._update_upload_selection_label()
        self._refresh_queue_table()

    def remove_selected_upload_items(self) -> None:
        selected_rows = sorted({idx.row() for idx in self.queue_table.selectionModel().selectedRows()}, reverse=True)
        if not selected_rows:
            QMessageBox.information(self, "Queue", "Select one or more queue rows to remove.")
            return
        for row in selected_rows:
            if 0 <= row < len(self.selected_upload_items):
                del self.selected_upload_items[row]
        self._update_upload_selection_label()
        self._refresh_queue_table()

    def upload_selected_file(self) -> None:
        cfg = self._current_config()

        if not self.selected_upload_items:
            QMessageBox.warning(self, "No files", "Select one or more files first.")
            return
        if not cfg["bucket_id"]:
            QMessageBox.warning(self, "Missing data", "Bucket ID is required.")
            return

        items = list(self.selected_upload_items)
        total_files = len(items)
        total_bytes = sum(size for _, _, size in items)
        prefix = cfg["prefix"].strip("/")

        self.set_status(f"Uploading {total_files} file(s)...")
        self.progress_label.setText("Preparing upload...")
        self.progress_bar.setValue(0)
        self.transfer_stop.clear()
        self.transfer_pause.clear()

        def task(progress):
            self._ensure_authorized(cfg)
            uploaded_done = 0
            retries = 3

            for idx, (local_path, target_rel, file_size) in enumerate(items, start=1):
                if self._should_stop_transfer():
                    raise RuntimeError("Transfer stopped by user.")
                file_name_in_bucket = f"{prefix}/{target_rel}" if prefix else target_rel
                file_label = os.path.basename(local_path)

                progress_pct = int((uploaded_done * 100) / max(1, total_bytes))
                progress(
                    progress_pct,
                    f"[{idx}/{total_files}] Preparing {file_label} | "
                    f"{format_bytes(uploaded_done)} / {format_bytes(total_bytes)} uploaded, "
                    f"left {format_bytes(max(0, total_bytes - uploaded_done))}",
                )

                def upload_progress(phase: str, current: int, total: int) -> None:
                    nonlocal uploaded_done
                    if phase == "upload":
                        uploaded_current = uploaded_done + max(0, current)
                        pct = int((uploaded_current * 100) / max(1, total_bytes))
                        progress(
                            pct,
                            f"[{idx}/{total_files}] Uploading {file_label} | "
                            f"{format_bytes(uploaded_current)} / {format_bytes(total_bytes)} uploaded, "
                            f"left {format_bytes(max(0, total_bytes - uploaded_current))}",
                        )
                    else:
                        progress(
                            int((uploaded_done * 100) / max(1, total_bytes)),
                            f"[{idx}/{total_files}] Calculating checksum for {file_label}...",
                        )

                attempt = 0
                while True:
                    try:
                        self.client.upload_file(
                            cfg["bucket_id"],
                            local_path,
                            file_name_in_bucket,
                            progress_cb=upload_progress,
                            should_stop=self._should_stop_transfer,
                            wait_if_paused=self._wait_if_paused,
                        )
                        break
                    except Exception:
                        attempt += 1
                        if attempt >= retries:
                            raise
                        progress(
                            int((uploaded_done * 100) / max(1, total_bytes)),
                            f"[{idx}/{total_files}] Retry {attempt}/{retries - 1} for {file_label}...",
                        )
                        time.sleep(min(2 * attempt, 5))
                uploaded_done += file_size

            progress(
                100,
                f"Upload completed | {format_bytes(total_bytes)} / {format_bytes(total_bytes)} uploaded, left 0 B",
            )
            return self.client.list_files_all(cfg["bucket_id"], cfg["prefix"])

        def done(files: object) -> None:
            self._fill_table(files)
            self.set_status("Upload completed")
            self.progress_label.setText("Upload completed")
            self.progress_bar.setValue(100)
            self._append_history("upload", "success", f"{total_files} files", total_bytes)
            self._notify_transfer_done("Uploaded", f"Uploaded {total_files} file(s).")
            self.clear_upload_selection()

        def on_progress(pct: int, text: str) -> None:
            self.progress_bar.setValue(max(0, min(100, pct)))
            self.progress_label.setText(text)

        self._run_bg(task, done, on_progress, transfer_job=True, action_name="upload")

    def refresh_files(self) -> None:
        cfg = self._current_config()
        if not cfg["bucket_id"]:
            QMessageBox.warning(self, "Missing data", "Bucket ID is required.")
            return

        self.set_status("Loading files...")

        def task():
            self._ensure_authorized(cfg)
            return self.client.list_files_all(cfg["bucket_id"], cfg["prefix"])

        def done(files: object) -> None:
            self._fill_table(files)
            self.set_status(f"Loaded {len(self.file_rows)} files")

        self._run_bg(task, done)

    def _fill_table(self, files: object) -> None:
        self.file_rows = list(files)
        cfg = self._current_config()
        self.base_bucket_prefix = cfg["prefix"].strip("/")
        if not self.current_folder_prefix:
            self.current_folder_prefix = self.base_bucket_prefix
        if self.base_bucket_prefix and not self.current_folder_prefix.startswith(self.base_bucket_prefix):
            self.current_folder_prefix = self.base_bucket_prefix
        if not self.base_bucket_prefix and self.current_folder_prefix:
            current = self.current_folder_prefix.strip("/") + "/"
            has_current = any(str(r.get("fileName", "")).startswith(current) for r in self.file_rows)
            if not has_current:
                self.current_folder_prefix = ""
        self._apply_filters()

    def _file_type_matches(self, file_name: str, choice: str) -> bool:
        ext = Path(file_name).suffix.lower().lstrip(".")
        if choice == "All types":
            return True
        groups = {
            "Images": {"jpg", "jpeg", "png", "gif", "webp", "bmp", "svg"},
            "Video": {"mp4", "mov", "avi", "mkv", "webm", "m4v"},
            "Audio": {"mp3", "wav", "flac", "aac", "ogg", "m4a"},
            "Documents": {"pdf", "doc", "docx", "txt", "rtf", "xls", "xlsx", "ppt", "pptx"},
            "Archives": {"zip", "rar", "7z", "tar", "gz", "bz2"},
        }
        return ext in groups.get(choice, set())

    def _size_filter_matches(self, size: int, choice: str) -> bool:
        mb = 1024 * 1024
        gb = 1024 * mb
        if choice == "Any size":
            return True
        if choice == "< 10 MB":
            return size < 10 * mb
        if choice == "10-100 MB":
            return 10 * mb <= size <= 100 * mb
        if choice == "100 MB - 1 GB":
            return 100 * mb < size <= gb
        if choice == "> 1 GB":
            return size > gb
        return True

    def _apply_filters(self) -> None:
        query = self.search_input.text().strip().lower() if hasattr(self, "search_input") else ""
        type_choice = self.type_filter.currentText() if hasattr(self, "type_filter") else "All types"
        size_choice = self.size_filter.currentText() if hasattr(self, "size_filter") else "Any size"

        rows = self._build_browser_rows()
        self.filtered_rows = []
        for row in rows:
            name = str(row.get("display_name", ""))
            file_name = str(row.get("fileName", ""))
            if query and query not in name.lower() and query not in file_name.lower():
                continue
            if row.get("kind") == "folder":
                self.filtered_rows.append(row)
                continue
            size = int(row.get("size", 0) or 0)
            if not self._file_type_matches(file_name, type_choice):
                continue
            if not self._size_filter_matches(size, size_choice):
                continue
            self.filtered_rows.append(row)

        rows = self.filtered_rows
        self.browser_rows = rows
        self.table.setRowCount(len(rows))
        for i, row in enumerate(rows):
            file_name = str(row.get("fileName", ""))
            is_folder = row.get("kind") == "folder"
            display_name = str(row.get("display_name", file_name))
            size = int(row.get("size", 0) or 0)
            upload_ts = row.get("uploadTimestamp")
            uploaded = ""
            if upload_ts:
                uploaded = dt.datetime.fromtimestamp(upload_ts / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            name_item = QTableWidgetItem(display_name)
            name_item.setData(Qt.UserRole, file_name)
            name_item.setData(Qt.UserRole + 1, row.get("kind"))
            type_item = QTableWidgetItem("Folder" if is_folder else "File")
            size_item = QTableWidgetItem("" if is_folder else format_bytes(size))
            size_item.setToolTip(f"{size} bytes")
            uploaded_item = QTableWidgetItem(uploaded)

            self.table.setItem(i, 0, name_item)
            self.table.setItem(i, 1, type_item)
            self.table.setItem(i, 2, size_item)
            self.table.setItem(i, 3, uploaded_item)
            self.table.setCellWidget(i, 4, self._new_table_preview_button(file_name, enabled=not is_folder))
            self.table.setCellWidget(i, 5, self._new_table_download_button(file_name, is_folder))
        self._update_folder_path_ui()
        self.set_status(f"Loaded {len(self.filtered_rows)} file(s) (filtered)")

    def _build_browser_rows(self) -> List[Dict]:
        current = self.current_folder_prefix.strip("/")
        folders: Dict[str, Dict] = {}
        files: List[Dict] = []

        for row in self.file_rows:
            file_name = str(row.get("fileName", ""))
            if not file_name:
                continue
            if current:
                start = current + "/"
                if not file_name.startswith(start):
                    continue
                remainder = file_name[len(start) :]
            else:
                remainder = file_name
            if not remainder:
                continue
            if "/" in remainder:
                folder_name = remainder.split("/", 1)[0]
                full_prefix = f"{current}/{folder_name}" if current else folder_name
                folders[full_prefix] = {
                    "kind": "folder",
                    "fileName": full_prefix,
                    "display_name": folder_name,
                    "size": 0,
                }
                continue
            file_row = dict(row)
            file_row["kind"] = "file"
            file_row["display_name"] = remainder
            file_row["size"] = self._extract_file_size(row)
            files.append(file_row)

        folder_rows = sorted(folders.values(), key=lambda r: str(r["display_name"]).lower())
        file_rows = sorted(files, key=lambda r: str(r.get("display_name", "")).lower())
        return folder_rows + file_rows

    def _update_folder_path_ui(self) -> None:
        base = self.base_bucket_prefix.strip("/")
        current = self.current_folder_prefix.strip("/")
        self.folder_back_btn.setEnabled(bool(current and current != base))
        self._render_breadcrumbs(base, current)

    def _render_breadcrumbs(self, base: str, current: str) -> None:
        while self.breadcrumb_layout.count():
            item = self.breadcrumb_layout.takeAt(0)
            widget = item.widget()
            if widget:
                widget.deleteLater()

        root_btn = QPushButton("/")
        root_btn.setObjectName("secondaryBtn")
        root_btn.setEnabled(current != base)
        root_btn.clicked.connect(lambda _=False, p=base: self._open_folder_from_breadcrumb(p))
        self.breadcrumb_layout.addWidget(root_btn)

        rel = current
        if base and current.startswith(base):
            rel = current[len(base) :].lstrip("/")
        parts = [p for p in rel.split("/") if p]

        path_acc = base
        for part in parts:
            self.breadcrumb_layout.addWidget(QLabel(">"))
            if path_acc:
                path_acc = f"{path_acc}/{part}"
            else:
                path_acc = part
            crumb = QPushButton(part)
            crumb.setObjectName("secondaryBtn")
            crumb.setEnabled(path_acc != current)
            crumb.clicked.connect(lambda _=False, p=path_acc: self._open_folder_from_breadcrumb(p))
            self.breadcrumb_layout.addWidget(crumb)

        self.breadcrumb_layout.addStretch(1)

    def _extract_file_size(self, row: Dict) -> int:
        # B2 may return size as `size` (list_file_names) or `contentLength` (versions/other APIs).
        raw = row.get("size", row.get("contentLength", 0))
        try:
            return int(raw)
        except Exception:
            return 0

    def _selected_file_name(self) -> Optional[str]:
        row = self.table.currentRow()
        if row < 0:
            return None
        item = self.table.item(row, 0)
        if not item:
            return None
        kind = item.data(Qt.UserRole + 1)
        if kind == "folder":
            return None
        return str(item.data(Qt.UserRole) or item.text())

    def _selected_file_names(self) -> List[str]:
        rows = sorted({idx.row() for idx in self.table.selectionModel().selectedRows()})
        result: List[str] = []
        for row in rows:
            item = self.table.item(row, 0)
            if not item:
                continue
            if item.data(Qt.UserRole + 1) == "folder":
                continue
            full_name = str(item.data(Qt.UserRole) or item.text())
            if full_name:
                result.append(full_name)
        return result

    def _copy_text(self, text: str) -> None:
        QApplication.clipboard().setText(text)

    def set_update_repo(self) -> None:
        value, ok = QInputDialog.getText(
            self,
            "Set Update Repo",
            "GitHub repo (owner/name):",
            text=self.update_repo,
        )
        if not ok:
            return
        value = value.strip()
        if not re.match(r"^[^/\s]+/[^/\s]+$", value):
            QMessageBox.warning(self, "Update repo", "Format must be owner/name.")
            return
        self.update_repo = value
        self.save_settings()
        self.set_status(f"Update repo set: {value}")

    def check_for_updates(self) -> None:
        repo = self.update_repo.strip() or DEFAULT_UPDATE_REPO
        self.set_status("Checking updates...")

        def task():
            url = f"https://api.github.com/repos/{repo}/releases/latest"
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            tag = str(data.get("tag_name", ""))
            html_url = str(data.get("html_url", ""))
            name = str(data.get("name", tag))
            return {"repo": repo, "tag": tag, "url": html_url, "name": name}

        def done(result: object) -> None:
            info = dict(result)
            latest_tag = str(info.get("tag", ""))
            latest_ver = parse_semver(latest_tag)
            current_ver = parse_semver(APP_VERSION)
            if latest_ver > current_ver and info.get("url"):
                answer = QMessageBox.question(
                    self,
                    "Update available",
                    f"Current: v{APP_VERSION}\nLatest: {latest_tag}\n\nOpen release page?",
                )
                if answer == QMessageBox.Yes:
                    QDesktopServices.openUrl(QUrl(str(info["url"])))
                self.set_status(f"Update available: {latest_tag}")
                return
            QMessageBox.information(
                self,
                "Updates",
                f"You are up to date.\nCurrent: v{APP_VERSION}\nLatest: {latest_tag or 'unknown'}",
            )
            self.set_status("No updates found")

        self._run_bg(task, done)

    def _build_preview_url(self, cfg: Dict, file_name: str) -> str:
        # QMediaPlayer can't inject auth headers reliably, so use temporary signed URL if possible.
        if cfg.get("bucket_id"):
            ttl = 3600
            try:
                ttl = self._private_ttl()
            except Exception:
                ttl = 3600
            token = self.client.get_download_authorization(cfg["bucket_id"], file_name, ttl)
            return self.client.make_direct_url(cfg["bucket_name"], file_name, auth_token=token)
        return self.client.make_direct_url(cfg["bucket_name"], file_name)

    def _new_table_preview_button(self, file_name: str, enabled: bool = True) -> QPushButton:
        btn = QPushButton("Preview")
        btn.setObjectName("secondaryBtn")
        btn.setEnabled(enabled)
        btn.clicked.connect(lambda _=False, name=file_name: self._open_preview_dialog_for_file(name))
        return btn

    def _new_table_download_button(self, file_name: str, is_folder: bool) -> QPushButton:
        btn = QPushButton("Download")
        btn.setObjectName("secondaryBtn")
        if is_folder:
            btn.clicked.connect(lambda _=False, prefix=file_name: self.download_folder_by_prefix(prefix_override=prefix))
        else:
            btn.clicked.connect(lambda _=False, name=file_name: self.download_single_file(name))
        return btn

    def _on_table_item_double_clicked(self, item: QTableWidgetItem) -> None:
        if item.column() != 0:
            return
        kind = item.data(Qt.UserRole + 1)
        full_name = str(item.data(Qt.UserRole) or item.text())
        if kind == "folder":
            self.open_folder(full_name)

    def open_folder(self, folder_prefix: str) -> None:
        self.current_folder_prefix = folder_prefix.strip("/")
        self._apply_filters()

    def _open_folder_from_breadcrumb(self, folder_prefix: str) -> None:
        self.current_folder_prefix = folder_prefix.strip("/")
        self._apply_filters()

    def open_parent_folder(self) -> None:
        current = self.current_folder_prefix.strip("/")
        base = self.base_bucket_prefix.strip("/")
        if not current or current == base:
            return
        parent = current.rsplit("/", 1)[0] if "/" in current else ""
        if base and parent and not parent.startswith(base):
            parent = base
        self.current_folder_prefix = parent
        self._apply_filters()

    def download_current_folder(self) -> None:
        prefix = self.current_folder_prefix.strip("/")
        self.download_folder_by_prefix(prefix_override=prefix)

    def _open_preview_dialog_for_file(self, file_name: str) -> None:
        cfg = self._current_config()
        if not cfg["bucket_name"]:
            QMessageBox.warning(self, "Preview", "Bucket Name is required.")
            return

        ext = Path(file_name).suffix.lower()
        image_ext = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}
        video_ext = {".mp4", ".mov", ".webm", ".mkv", ".m4v"}
        audio_ext = {".mp3", ".wav", ".flac", ".m4a", ".aac", ".ogg"}
        media_ext = image_ext | video_ext | audio_ext

        if ext not in media_ext:
            QMessageBox.information(self, "Preview", f"Preview is not supported for {ext or 'this file type'}.")
            return

        dialog = PreviewDialog(self, file_name)
        self._preview_windows.append(dialog)
        dialog.destroyed.connect(lambda: self._preview_windows.remove(dialog) if dialog in self._preview_windows else None)
        dialog.show()

        def task():
            self._ensure_authorized(cfg)
            url = self._build_preview_url(cfg, file_name)
            if ext in image_ext:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                return {"kind": "image", "data": resp.content, "name": file_name}
            if ext in video_ext:
                return {"kind": "video", "url": url, "name": file_name}
            return {"kind": "audio", "url": url, "name": file_name}

        def done(result: object) -> None:
            info = dict(result)
            kind = str(info.get("kind", ""))
            if kind == "image":
                dialog.show_image(str(info.get("name", "")), bytes(info.get("data", b"")))
                return
            if kind == "video":
                dialog.show_media(str(info.get("name", "")), str(info.get("url", "")), is_video=True)
                return
            dialog.show_media(str(info.get("name", "")), str(info.get("url", "")), is_video=False)

        self._run_bg(task, done)

    def preview_selected_file(self) -> None:
        file_name = self._selected_file_name()
        if not file_name:
            QMessageBox.warning(self, "Preview", "Select a file in the Files tab.")
            return
        self._open_preview_dialog_for_file(file_name)

    def _public_link_task(self, cfg: Dict, file_name: str) -> str:
        self._ensure_authorized(cfg)
        return self.client.make_direct_url(cfg["bucket_name"], file_name)

    def _private_link_task(self, cfg: Dict, file_name: str) -> str:
        self._ensure_authorized(cfg)
        token = self.client.get_download_authorization(cfg["bucket_id"], file_name, self._private_ttl())
        return self.client.make_direct_url(cfg["bucket_name"], file_name, auth_token=token)

    def copy_public_link(self) -> None:
        cfg = self._current_config()
        file_name = self._selected_file_name()
        if not cfg["bucket_name"]:
            QMessageBox.warning(self, "Missing data", "Bucket Name is required.")
            return
        if not file_name:
            QMessageBox.warning(self, "No selection", "Select a file from the list.")
            return

        self.set_status("Generating public link...")

        def task():
            return self._public_link_task(cfg, file_name)

        def done(link: object) -> None:
            url = str(link)
            self._copy_text(url)
            self._append_share(file_name, "public", url, None)
            self._append_history("share-public", "success", file_name)
            self.set_status("Public link copied")

        self._run_bg(task, done)

    def open_public_link(self) -> None:
        cfg = self._current_config()
        file_name = self._selected_file_name()
        if not cfg["bucket_name"]:
            QMessageBox.warning(self, "Missing data", "Bucket Name is required.")
            return
        if not file_name:
            QMessageBox.warning(self, "No selection", "Select a file from the list.")
            return

        self.set_status("Opening public link...")

        def task():
            return self._public_link_task(cfg, file_name)

        def done(link: object) -> None:
            url = str(link)
            QDesktopServices.openUrl(QUrl(url))
            self._append_share(file_name, "public", url, None)
            self._append_history("share-public", "success", file_name)
            self.set_status("Public link opened")

        self._run_bg(task, done)

    def copy_private_link(self) -> None:
        cfg = self._current_config()
        file_name = self._selected_file_name()
        if not cfg["bucket_name"] or not cfg["bucket_id"]:
            QMessageBox.warning(self, "Missing data", "Bucket ID and Bucket Name are required.")
            return
        if not file_name:
            QMessageBox.warning(self, "No selection", "Select a file from the list.")
            return

        self.set_status("Generating private link...")

        def task():
            return self._private_link_task(cfg, file_name)

        def done(link: object) -> None:
            url = str(link)
            self._copy_text(url)
            self._append_share(file_name, "private", url, self._private_ttl())
            self._append_history("share-private", "success", file_name)
            self.set_status("Private link copied")

        self._run_bg(task, done)

    def open_private_link(self) -> None:
        cfg = self._current_config()
        file_name = self._selected_file_name()
        if not cfg["bucket_name"] or not cfg["bucket_id"]:
            QMessageBox.warning(self, "Missing data", "Bucket ID and Bucket Name are required.")
            return
        if not file_name:
            QMessageBox.warning(self, "No selection", "Select a file from the list.")
            return

        self.set_status("Opening private link...")

        def task():
            return self._private_link_task(cfg, file_name)

        def done(link: object) -> None:
            url = str(link)
            QDesktopServices.openUrl(QUrl(url))
            self._append_share(file_name, "private", url, self._private_ttl())
            self._append_history("share-private", "success", file_name)
            self.set_status("Private link opened")

        self._run_bg(task, done)

    def _download_batch(self, cfg: Dict, file_names: List[str], destination_root: str) -> None:
        total_files = len(file_names)
        total_expected = 0
        for name in file_names:
            row = next((r for r in self.file_rows if r.get("fileName") == name), None)
            if row:
                total_expected += self._extract_file_size(row)

        self.set_status(f"Downloading {total_files} file(s)...")
        self.progress_bar.setValue(0)
        self.progress_label.setText("Preparing download...")
        self.transfer_stop.clear()
        self.transfer_pause.clear()

        def task(progress):
            self._ensure_authorized(cfg)
            downloaded_done = 0
            retries = 3

            for idx, file_name in enumerate(file_names, start=1):
                if self._should_stop_transfer():
                    raise RuntimeError("Transfer stopped by user.")
                target_path = os.path.join(destination_root, file_name.replace("/", os.sep))
                file_label = os.path.basename(file_name) or file_name

                progress(
                    int((downloaded_done * 100) / max(1, total_expected)) if total_expected > 0 else 0,
                    f"[{idx}/{total_files}] Downloading {file_label}...",
                )

                def on_file_progress(current: int, total: int) -> None:
                    if total_expected > 0 and total > 0:
                        global_current = downloaded_done + current
                        pct = int((global_current * 100) / max(1, total_expected))
                        progress(
                            pct,
                            f"[{idx}/{total_files}] "
                            f"{format_bytes(global_current)} / {format_bytes(total_expected)} downloaded, "
                            f"left {format_bytes(max(0, total_expected - global_current))}",
                        )
                    else:
                        progress(0, f"[{idx}/{total_files}] Downloading {file_label}...")

                attempt = 0
                while True:
                    try:
                        self.client.download_file(
                            cfg["bucket_name"],
                            file_name,
                            target_path,
                            progress_cb=on_file_progress,
                            should_stop=self._should_stop_transfer,
                            wait_if_paused=self._wait_if_paused,
                        )
                        break
                    except Exception:
                        attempt += 1
                        if attempt >= retries:
                            raise
                        progress(
                            int((downloaded_done * 100) / max(1, total_expected)) if total_expected else 0,
                            f"[{idx}/{total_files}] Retry {attempt}/{retries - 1} for {file_label}...",
                        )
                        time.sleep(min(2 * attempt, 5))

                file_size = 0
                try:
                    file_size = os.path.getsize(target_path)
                except Exception:
                    pass
                downloaded_done += file_size

            progress(100, "Download completed")
            return None

        def done(_: object) -> None:
            self.set_status("Download completed")
            self.progress_label.setText("Download completed")
            self.progress_bar.setValue(100)
            self._append_history("download", "success", f"{total_files} files", total_expected)
            self._notify_transfer_done("Downloaded", f"Downloaded {total_files} file(s).")

        def on_progress(pct: int, text: str) -> None:
            self.progress_bar.setValue(max(0, min(100, pct)))
            self.progress_label.setText(text)

        self._run_bg(task, done, on_progress, transfer_job=True, action_name="download")

    def download_selected_files(self) -> None:
        cfg = self._current_config()
        if not cfg["bucket_name"]:
            QMessageBox.warning(self, "Missing data", "Bucket Name is required.")
            return

        selected = self._selected_file_names()
        if not selected:
            QMessageBox.warning(self, "No selection", "Select one or more files in the bucket table.")
            return

        destination = QFileDialog.getExistingDirectory(self, "Select destination folder")
        if not destination:
            return

        self._download_batch(cfg, selected, destination)

    def download_single_file(self, file_name: str) -> None:
        cfg = self._current_config()
        if not cfg["bucket_name"]:
            QMessageBox.warning(self, "Missing data", "Bucket Name is required.")
            return
        destination = QFileDialog.getExistingDirectory(self, "Select destination folder")
        if not destination:
            return
        self._download_batch(cfg, [file_name], destination)

    def download_folder_by_prefix(self, prefix_override: Optional[str] = None) -> None:
        cfg = self._current_config()
        if not cfg["bucket_name"] or not cfg["bucket_id"]:
            QMessageBox.warning(self, "Missing data", "Bucket ID and Bucket Name are required.")
            return

        prefix = (prefix_override if prefix_override is not None else cfg["prefix"]).strip()
        if not prefix:
            QMessageBox.warning(self, "Missing prefix", "Set folder prefix in the Prefix field (e.g. media/2026/).")
            return

        destination = QFileDialog.getExistingDirectory(self, "Select destination folder for folder download")
        if not destination:
            return

        self.set_status("Loading folder file list...")
        self.progress_label.setText("Loading folder file list...")
        self.progress_bar.setValue(0)

        def task(progress):
            self._ensure_authorized(cfg)
            files = self.client.list_files_all(cfg["bucket_id"], prefix=prefix)
            names = [f.get("fileName", "") for f in files if f.get("fileName")]
            if not names:
                raise RuntimeError(f"No files found for prefix: {prefix}")
            progress(5, f"Found {len(names)} file(s). Starting download...")
            return names

        def done(file_names: object) -> None:
            self._download_batch(cfg, list(file_names), destination)

        def on_progress(pct: int, text: str) -> None:
            self.progress_bar.setValue(max(0, min(100, pct)))
            self.progress_label.setText(text)

        self._run_bg(task, done, on_progress)

    def sync_folder_to_prefix(self) -> None:
        cfg = self._current_config()
        if not cfg["bucket_id"]:
            QMessageBox.warning(self, "Missing data", "Bucket ID is required.")
            return
        prefix = cfg["prefix"].strip("/")
        folder = QFileDialog.getExistingDirectory(self, "Select local folder to sync")
        if not folder:
            return

        self.set_status("Sync: loading remote index...")
        self.progress_bar.setValue(0)
        self.progress_label.setText("Sync: loading remote index...")

        def task(progress):
            self._ensure_authorized(cfg)
            remote_files = self.client.list_files_all(cfg["bucket_id"], prefix=prefix)
            remote_index: Dict[str, int] = {}
            for row in remote_files:
                name = row.get("fileName", "")
                if not name:
                    continue
                rel = name
                if prefix and rel.startswith(prefix + "/"):
                    rel = rel[len(prefix) + 1 :]
                remote_index[rel] = self._extract_file_size(row)

            local_items: List[Tuple[str, str, int]] = []
            for root, _, files in os.walk(folder):
                for file_name in files:
                    local_path = os.path.join(root, file_name)
                    rel_path = os.path.relpath(local_path, folder).replace("\\", "/")
                    size = os.path.getsize(local_path)
                    remote_size = remote_index.get(rel_path)
                    if remote_size != size:
                        target_rel = rel_path
                        local_items.append((local_path, target_rel, size))

            if not local_items:
                progress(100, "Sync: everything is up to date.")
                return {"items": [], "bytes": 0}

            total = len(local_items)
            total_bytes = sum(item[2] for item in local_items)
            uploaded = 0

            for idx, (local_path, target_rel, size) in enumerate(local_items, start=1):
                if self._should_stop_transfer():
                    raise RuntimeError("Sync stopped by user.")
                self._wait_if_paused()
                file_name_in_bucket = f"{prefix}/{target_rel}" if prefix else target_rel
                label = os.path.basename(local_path)

                def sync_progress(phase: str, current: int, _total: int) -> None:
                    if phase != "upload":
                        return
                    global_current = uploaded + current
                    pct = int((global_current * 100) / max(1, total_bytes))
                    progress(
                        pct,
                        f"[{idx}/{total}] Syncing {label} | "
                        f"{format_bytes(global_current)} / {format_bytes(total_bytes)} uploaded",
                    )

                self.client.upload_file(
                    cfg["bucket_id"],
                    local_path,
                    file_name_in_bucket,
                    progress_cb=sync_progress,
                    should_stop=self._should_stop_transfer,
                    wait_if_paused=self._wait_if_paused,
                )
                uploaded += size

            progress(100, f"Sync completed: {len(local_items)} file(s)")
            return {"items": local_items, "bytes": total_bytes}

        def done(result: object) -> None:
            data = dict(result)
            items = list(data.get("items", []))
            total_bytes = int(data.get("bytes", 0))
            self._append_history("sync", "success", f"{len(items)} files", total_bytes)
            if not items:
                self._notify_transfer_done("Sync", "Everything is already up to date.")
                self.set_status("Sync completed (no changes)")
                return
            self.set_status(f"Sync completed: {len(items)} file(s)")
            self._notify_transfer_done("Sync", f"Synced {len(items)} file(s).")
            self.refresh_files()

        def on_progress(pct: int, text: str) -> None:
            self.progress_bar.setValue(max(0, min(100, pct)))
            self.progress_label.setText(text)

        self._run_bg(task, done, on_progress, transfer_job=True, action_name="sync")


def run() -> None:
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == "__main__":
    run()
