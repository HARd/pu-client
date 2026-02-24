import base64
import datetime as dt
import hashlib
import inspect
import json
import os
from pathlib import Path
import threading
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, urlencode

import requests
from PySide6.QtCore import QObject, Qt, QUrl, Signal
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QFileDialog,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)


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
    ) -> dict:
        upload_info = self.get_upload_url(bucket_id)
        upload_url = upload_info["uploadUrl"]
        upload_auth_token = upload_info["authorizationToken"]

        total_size = os.path.getsize(local_path)
        sha1 = self._compute_file_sha1(local_path, total_size, progress_cb)

        headers = {
            "Authorization": upload_auth_token,
            "X-Bz-File-Name": quote(file_name_in_bucket, safe="/"),
            "Content-Type": "b2/x-auto",
            "Content-Length": str(total_size),
            "X-Bz-Content-Sha1": sha1,
        }

        with open(local_path, "rb") as f:
            stream = UploadProgressReader(f, total_size, progress_cb)
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
    ) -> str:
        hasher = hashlib.sha1()
        processed = 0
        chunk_size = 1024 * 1024

        with open(local_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
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
    ) -> None:
        self.file_obj = file_obj
        self.total_size = total_size
        self.sent = 0
        self.progress_cb = progress_cb

    def __len__(self) -> int:
        return self.total_size

    def tell(self) -> int:
        return self.sent

    def seek(self, offset: int, whence: int = 0) -> int:
        pos = self.file_obj.seek(offset, whence)
        self.sent = self.file_obj.tell()
        return pos

    def read(self, amt: int = -1) -> bytes:
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


class WorkerSignals(QObject):
    success = Signal(object)
    error = Signal(str)
    progress = Signal(int, str)


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("PlayUA Desktop Client")
        self.resize(1240, 780)

        self.client = BackblazeB2Client()
        self.settings_store = SettingsStore()
        self.last_auth_key = None

        self.file_rows = []
        self.selected_upload_items: List[Tuple[str, str, int]] = []
        self._workers = []
        self.theme_mode = "dark"

        self._build_ui()
        self._set_human_friendly_defaults()
        self._load_settings()
        self._refresh_queue_table()

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

        body = QHBoxLayout()
        body.setSpacing(12)
        main.addLayout(body, 1)

        left_col = QVBoxLayout()
        left_col.setSpacing(10)
        right_col = QVBoxLayout()
        right_col.setSpacing(10)
        body.addLayout(left_col, 4)
        body.addLayout(right_col, 6)

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

        row1 = QHBoxLayout()
        row1.setSpacing(8)
        left_col.addLayout(row1)

        self.save_btn = QPushButton("Save Settings")
        self.auth_btn = QPushButton("Authorize")
        self.select_files_btn = QPushButton("Select Files")
        self.select_folder_btn = QPushButton("Select Folder")
        self.clear_selection_btn = QPushButton("Clear Selection")
        self.upload_btn = QPushButton("Upload")
        self.refresh_btn = QPushButton("Refresh File List")

        row1.addWidget(self.save_btn)
        row1.addWidget(self.auth_btn)
        row1.addWidget(self.select_files_btn)
        row1.addWidget(self.select_folder_btn)
        row1.addWidget(self.clear_selection_btn)
        row1.addWidget(self.upload_btn)
        row1.addWidget(self.refresh_btn)

        row2 = QHBoxLayout()
        row2.setSpacing(8)
        right_col.addLayout(row2)

        self.copy_public_btn = QPushButton("Copy Public Link")
        self.open_public_btn = QPushButton("Open Public Link")
        self.copy_private_btn = QPushButton("Copy Private Link")
        self.open_private_btn = QPushButton("Open Private Link")
        self.download_selected_btn = QPushButton("Download Selected")
        self.download_folder_btn = QPushButton("Download Folder")

        row2.addWidget(self.copy_public_btn)
        row2.addWidget(self.open_public_btn)
        row2.addWidget(self.copy_private_btn)
        row2.addWidget(self.open_private_btn)
        row2.addWidget(self.download_selected_btn)
        row2.addWidget(self.download_folder_btn)

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

        files_group = QGroupBox("Files In Bucket")
        right_col.addWidget(files_group, 1)
        files_layout = QVBoxLayout(files_group)
        files_layout.setSpacing(8)

        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["File Name", "Size", "Uploaded (UTC)"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        files_layout.addWidget(self.table, 1)

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
        self.refresh_btn.clicked.connect(self.refresh_files)
        self.copy_public_btn.clicked.connect(self.copy_public_link)
        self.open_public_btn.clicked.connect(self.open_public_link)
        self.copy_private_btn.clicked.connect(self.copy_private_link)
        self.open_private_btn.clicked.connect(self.open_private_link)
        self.download_selected_btn.clicked.connect(self.download_selected_files)
        self.download_folder_btn.clicked.connect(self.download_folder_by_prefix)
        self.remove_selected_btn.clicked.connect(self.remove_selected_upload_items)
        self.dark_theme_check.toggled.connect(self._on_theme_toggled)

    def _set_human_friendly_defaults(self) -> None:
        self.auth_btn.setObjectName("primaryBtn")
        self.upload_btn.setObjectName("primaryBtn")
        self.refresh_btn.setObjectName("secondaryBtn")
        self.copy_public_btn.setObjectName("secondaryBtn")
        self.open_public_btn.setObjectName("secondaryBtn")
        self.copy_private_btn.setObjectName("secondaryBtn")
        self.open_private_btn.setObjectName("secondaryBtn")
        self.download_selected_btn.setObjectName("secondaryBtn")
        self.download_folder_btn.setObjectName("secondaryBtn")
        self.remove_selected_btn.setObjectName("dangerBtn")
        self._configure_hints()

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
                QPushButton {
                  padding: 7px 12px;
                  border-radius: 8px;
                  border: 1px solid #cbd5e1;
                  background: #ffffff;
                }
                QPushButton:hover { background: #f8fafc; }
                QPushButton#primaryBtn {
                  background: #2563eb;
                  border: 1px solid #2563eb;
                  color: white;
                  font-weight: 600;
                }
                QPushButton#primaryBtn:hover { background: #1e4fd8; }
                QPushButton#secondaryBtn {
                  background: #0f766e;
                  border: 1px solid #0f766e;
                  color: white;
                  font-weight: 600;
                }
                QPushButton#secondaryBtn:hover { background: #0d635c; }
                QPushButton#dangerBtn {
                  background: #fef2f2;
                  color: #b91c1c;
                  border: 1px solid #fecaca;
                }
                QPushButton:disabled {
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
            QPushButton {
              padding: 7px 12px;
              border-radius: 8px;
              border: 1px solid #3f3f46;
              background: #1b1b21;
              color: #e5e7eb;
            }
            QPushButton:hover { background: #23232b; }
            QPushButton#primaryBtn {
              background: #2563eb;
              border: 1px solid #2563eb;
              color: white;
              font-weight: 600;
            }
            QPushButton#primaryBtn:hover { background: #1e4fd8; }
            QPushButton#secondaryBtn {
              background: #0f766e;
              border: 1px solid #0f766e;
              color: white;
              font-weight: 600;
            }
            QPushButton#secondaryBtn:hover { background: #0d635c; }
            QPushButton#dangerBtn {
              background: #3a1113;
              color: #fecaca;
              border: 1px solid #7f1d1d;
            }
            QPushButton:disabled {
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
        self.clear_selection_btn.setToolTip("Clear the whole upload queue.")
        self.remove_selected_btn.setToolTip("Remove selected rows from upload queue preview.")
        self.download_selected_btn.setToolTip("Download selected files from bucket to a local folder.")
        self.download_folder_btn.setToolTip("Download all files by prefix from bucket.")

    def set_status(self, text: str) -> None:
        self.status_label.setText(text)

    def _set_busy(self, busy: bool) -> None:
        controls = [
            self.save_btn,
            self.auth_btn,
            self.select_files_btn,
            self.select_folder_btn,
            self.clear_selection_btn,
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

    def _run_bg(self, fn, on_success=None, on_progress=None) -> None:
        signals = WorkerSignals()
        self._set_busy(True)

        def handle_success(result: object) -> None:
            try:
                if on_success:
                    on_success(result)
            finally:
                self._set_busy(False)
                self._workers.remove(signals)

        def handle_error(msg: str) -> None:
            try:
                QMessageBox.critical(self, "Error", msg)
                self.set_status("Error")
            finally:
                self._set_busy(False)
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
        self.key_id_input.setText(data.get("key_id", ""))
        self.app_key_input.setText(data.get("app_key", ""))
        self.bucket_id_input.setText(data.get("bucket_id", ""))
        self.bucket_name_input.setText(data.get("bucket_name", ""))
        self.prefix_input.setText(data.get("prefix", ""))
        self.ttl_input.setText(str(data.get("private_ttl", 3600)))
        self.remember_check.setChecked(bool(data.get("remember", True)))
        theme = str(data.get("theme", "dark")).lower()
        if theme not in ("dark", "light"):
            theme = "dark"
        self.dark_theme_check.blockSignals(True)
        self.dark_theme_check.setChecked(theme == "dark")
        self.dark_theme_check.blockSignals(False)
        self._apply_theme(theme)

    def save_settings(self) -> None:
        cfg = self._current_config()
        payload = {
            "key_id": cfg["key_id"],
            "app_key": cfg["app_key"] if cfg["remember"] else "",
            "bucket_id": cfg["bucket_id"],
            "bucket_name": cfg["bucket_name"],
            "prefix": cfg["prefix"],
            "private_ttl": self._private_ttl(),
            "remember": cfg["remember"],
            "theme": "dark" if self.dark_theme_check.isChecked() else "light",
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

        def task(progress):
            self._ensure_authorized(cfg)
            uploaded_done = 0

            for idx, (local_path, target_rel, file_size) in enumerate(items, start=1):
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

                self.client.upload_file(
                    cfg["bucket_id"],
                    local_path,
                    file_name_in_bucket,
                    progress_cb=upload_progress,
                )
                uploaded_done += file_size

            progress(
                100,
                f"Upload completed | {format_bytes(total_bytes)} / {format_bytes(total_bytes)} uploaded, left 0 B",
            )
            return self.client.list_files(cfg["bucket_id"], cfg["prefix"])

        def done(files: object) -> None:
            self._fill_table(files)
            self.set_status("Upload completed")
            self.progress_label.setText("Upload completed")
            self.progress_bar.setValue(100)
            QMessageBox.information(self, "Uploaded", f"Uploaded {total_files} file(s).")
            self.clear_upload_selection()

        def on_progress(pct: int, text: str) -> None:
            self.progress_bar.setValue(max(0, min(100, pct)))
            self.progress_label.setText(text)

        self._run_bg(task, done, on_progress)

    def refresh_files(self) -> None:
        cfg = self._current_config()
        if not cfg["bucket_id"]:
            QMessageBox.warning(self, "Missing data", "Bucket ID is required.")
            return

        self.set_status("Loading files...")

        def task():
            self._ensure_authorized(cfg)
            return self.client.list_files(cfg["bucket_id"], cfg["prefix"])

        def done(files: object) -> None:
            self._fill_table(files)
            self.set_status(f"Loaded {len(self.file_rows)} files")

        self._run_bg(task, done)

    def _fill_table(self, files: object) -> None:
        rows = list(files)
        self.file_rows = rows

        self.table.setRowCount(len(rows))
        for i, row in enumerate(rows):
            file_name = row.get("fileName", "")
            size = self._extract_file_size(row)
            upload_ts = row.get("uploadTimestamp")
            uploaded = ""
            if upload_ts:
                uploaded = dt.datetime.fromtimestamp(upload_ts / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            name_item = QTableWidgetItem(file_name)
            size_item = QTableWidgetItem(format_bytes(size))
            size_item.setToolTip(f"{size} bytes")
            uploaded_item = QTableWidgetItem(uploaded)

            self.table.setItem(i, 0, name_item)
            self.table.setItem(i, 1, size_item)
            self.table.setItem(i, 2, uploaded_item)

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
        return item.text()

    def _selected_file_names(self) -> List[str]:
        rows = sorted({idx.row() for idx in self.table.selectionModel().selectedRows()})
        result: List[str] = []
        for row in rows:
            item = self.table.item(row, 0)
            if item and item.text():
                result.append(item.text())
        return result

    def _copy_text(self, text: str) -> None:
        QApplication.clipboard().setText(text)

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
            self._copy_text(str(link))
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
            QDesktopServices.openUrl(QUrl(str(link)))
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
            self._copy_text(str(link))
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
            QDesktopServices.openUrl(QUrl(str(link)))
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

        def task(progress):
            self._ensure_authorized(cfg)
            downloaded_done = 0

            for idx, file_name in enumerate(file_names, start=1):
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

                self.client.download_file(
                    cfg["bucket_name"],
                    file_name,
                    target_path,
                    progress_cb=on_file_progress,
                )

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
            QMessageBox.information(self, "Downloaded", f"Downloaded {total_files} file(s).")

        def on_progress(pct: int, text: str) -> None:
            self.progress_bar.setValue(max(0, min(100, pct)))
            self.progress_label.setText(text)

        self._run_bg(task, done, on_progress)

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

    def download_folder_by_prefix(self) -> None:
        cfg = self._current_config()
        if not cfg["bucket_name"] or not cfg["bucket_id"]:
            QMessageBox.warning(self, "Missing data", "Bucket ID and Bucket Name are required.")
            return

        prefix = cfg["prefix"].strip()
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


def run() -> None:
    app = QApplication([])
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == "__main__":
    run()
