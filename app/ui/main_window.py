import datetime as dt
import inspect
import json
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple
from urllib.parse import quote, urlencode

import requests
from PySide6.QtCore import QObject, Qt, QUrl, Signal
from PySide6.QtGui import QAction, QDesktopServices, QIcon, QKeySequence, QPixmap
from PySide6.QtWidgets import (
    QAbstractItemView, QApplication, QCheckBox, QComboBox, QDialog, QFileDialog,
    QGridLayout, QGroupBox, QHBoxLayout, QHeaderView, QInputDialog, QLabel,
    QLineEdit, QMainWindow, QMenu, QMessageBox, QProgressBar, QPushButton,
    QSlider, QSplitter, QTabWidget, QTableWidget, QTableWidgetItem, QToolButton,
    QVBoxLayout, QWidget
)

from app.api.b2_client import BackblazeB2Client, UploadProgressReader
from app.core.stores import HistoryStore, SettingsStore
from app.core.utils import (APP_VERSION, DEFAULT_UPDATE_REPO, app_root_path,
                            format_bytes, parse_semver, resolve_app_icon_path,
                            should_set_runtime_icon)
from app.ui.preview_dialog import PreviewDialog, WorkerSignals
from app.ui.components.connection_panel import ConnectionPanel
from app.ui.components.transfer_queue import TransferQueueWidget
from app.ui.components.bucket_browser import BucketBrowserWidget
from app.ui.components.share_manager import ShareManagerWidget
from app.ui.components.transfer_history import TransferHistoryWidget
from app.ui.themes import DARK_THEME, LIGHT_THEME

class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("PlayUA Desktop Client")
        self.resize(1240, 780)
        self.setAcceptDrops(True)
        icon_path = resolve_app_icon_path()
        if icon_path and should_set_runtime_icon():
            self.setWindowIcon(QIcon(str(icon_path)))

        self.client = BackblazeB2Client()
        self.settings_store = SettingsStore()
        self.history_store = HistoryStore(self.settings_store)
        self.last_auth_key = None

        self.file_rows = []
        self.filtered_rows = []
        self.browser_rows: List[Dict] = []
        self.base_bucket_prefix = ""
        self.current_folder_prefix = ""
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

        self.connection_panel = ConnectionPanel()
        left_col.addWidget(self.connection_panel)

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

        self.transfer_queue = TransferQueueWidget()
        left_col.addWidget(self.transfer_queue, 1)

        right_tabs = QTabWidget()
        right_col.addWidget(right_tabs, 1)

        self.bucket_browser = BucketBrowserWidget()
        right_tabs.addTab(self.bucket_browser, "Files")

        self.share_manager = ShareManagerWidget()
        right_tabs.addTab(self.share_manager, "Shares")

        self.history_widget = TransferHistoryWidget()
        right_tabs.addTab(self.history_widget, "History")
        self.history_widget.statusChanged.connect(self.set_status)

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
        self.clear_selection_btn.clicked.connect(self.transfer_queue.clear_upload_selection)
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
        self.bucket_browser.refreshRequested.connect(self.refresh_files)
        self.bucket_browser.downloadFolderRequested.connect(self.download_folder_by_prefix)
        self.bucket_browser.downloadFileRequested.connect(self.download_single_file)
        self.bucket_browser.downloadSelectedRequested.connect(self.download_selected_files)
        self.bucket_browser.previewRequested.connect(self._open_preview_dialog_for_file)
        self.bucket_browser.copyPublicLinkRequested.connect(self.copy_public_link)
        self.bucket_browser.copyPrivateLinkRequested.connect(self.copy_private_link)
        self.bucket_browser.openPublicLinkRequested.connect(self.open_public_link)
        self.bucket_browser.openPrivateLinkRequested.connect(self.open_private_link)
        self.bucket_browser.statusChanged.connect(self.set_status)
        self.bucket_browser.selectionChanged.connect(self._sync_more_menu_state)
        self.connection_panel.themeToggled.connect(self._on_theme_toggled)
        self.connection_panel.profileChanged.connect(self._on_profile_changed)
        self.connection_panel.saveProfileRequested.connect(self.save_profile)
        self.connection_panel.deleteProfileRequested.connect(self.delete_profile)

        self.share_manager.statusChanged.connect(self.set_status)

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

        self._configure_hints()
        self.resume_btn.setEnabled(False)
        self.pause_btn.setEnabled(False)
        self.stop_btn.setEnabled(False)
        self.download_btn.setEnabled(False)
        self._update_bucket_actions_state()

        self._apply_theme("dark")

    def _apply_theme(self, theme: str) -> None:
        self.theme_mode = theme
        if theme == "light":
            self.setStyleSheet(LIGHT_THEME)
        else:
            self.setStyleSheet(DARK_THEME)
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
            if self.transfer_queue.has_focus():
                self.transfer_queue.remove_selected_upload_items()
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
            self.transfer_queue.add_items(items)
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
        self.history_widget.populate(rows)

    def _notify_transfer_done(self, title: str, message: str) -> None:
        if self.background_check.isChecked():
            QApplication.alert(self, 3000)
            return
        QMessageBox.information(self, title, message)

    def _update_bucket_actions_state(self) -> None:
        has_selection = bool(self.bucket_browser.selected_file_names())
        self.download_btn.setEnabled(has_selection)
        target_role = "primaryBtn" if has_selection else "secondaryBtn"
        if self.download_btn.objectName() != target_role:
            self.download_btn.setObjectName(target_role)
            self.download_btn.style().unpolish(self.download_btn)
            self.download_btn.style().polish(self.download_btn)
            self.download_btn.update()



    def _configure_hints(self) -> None:
        self.select_files_btn.setToolTip("Pick one or more individual files.")
        self.select_folder_btn.setToolTip("Pick a folder. All files inside will be uploaded recursively.")
        self.upload_btn.setToolTip("Start uploading all queued items.")
        self.download_btn.setToolTip("Download selected files from bucket table.")
        self.more_btn.setToolTip("More actions and advanced controls.")
        self.clear_selection_btn.setToolTip("Clear the whole upload queue.")
        self.download_selected_btn.setToolTip("Download selected files from bucket to a local folder.")
        self.download_folder_btn.setToolTip("Download all files by prefix from bucket.")
        self.pause_btn.setToolTip("Pause current transfer (upload/download).")
        self.resume_btn.setToolTip("Resume paused transfer.")
        self.stop_btn.setToolTip("Stop current transfer.")
        self.sync_btn.setToolTip("Choose local folder and upload missing/changed files into selected prefix.")


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
        self.more_clear_queue_action.triggered.connect(self.transfer_queue.clear_upload_selection)
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
        self.more_clear_queue_action.setEnabled(bool(self.transfer_queue.get_items()))
        self.more_pause_action.setEnabled(self.transfer_active and not self.transfer_pause.is_set())
        self.more_resume_action.setEnabled(self.transfer_active and self.transfer_pause.is_set())
        self.more_stop_action.setEnabled(self.transfer_active)
        self.more_background_action.setChecked(self.background_check.isChecked())
        self.more_profile_new_action.setEnabled(True)


    def set_status(self, text: str) -> None:
        self.status_label.setText(text)

    def _profile_payload_from_fields(self) -> Dict:
        cfg = self.connection_panel.get_config()
        return {
            "key_id": cfg["key_id"],
            "app_key": cfg["app_key"],
            "bucket_id": cfg["bucket_id"],
            "bucket_name": cfg["bucket_name"],
            "prefix": cfg["prefix"],
            "private_ttl": cfg["private_ttl"],
        }

    def _apply_profile_payload(self, payload: Dict) -> None:
        self.connection_panel.apply_profile_payload(payload)

    def _refresh_profile_combo(self) -> None:
        names = sorted(self.profiles.keys())
        if not names:
            self.profiles["Default"] = self._profile_payload_from_fields()
            names = ["Default"]
        if self.active_profile_name not in self.profiles:
            self.active_profile_name = names[0]
        self.connection_panel.set_profiles(names, self.active_profile_name)

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
        name = self.connection_panel.current_profile()
        if not name:
            name = "Default"
        self.profiles[name] = self._profile_payload_from_fields()
        self.active_profile_name = name
        self._refresh_profile_combo()
        self.save_settings()
        self.set_status(f"Profile saved: {name}")

    def delete_profile(self) -> None:
        name = self.connection_panel.current_profile()
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
            self.upload_btn,
            self.refresh_btn,
            self.copy_public_btn,
            self.open_public_btn,
            self.copy_private_btn,
            self.open_private_btn,
            self.download_selected_btn,
            self.download_folder_btn,
            self.sync_btn,
        ]
        for w in controls:
            w.setEnabled(not busy)
        self.connection_panel.set_busy(busy)

    def _current_config(self) -> Dict:
        cfg = self.connection_panel.get_config()
        cfg["background"] = self.background_check.isChecked()
        return cfg

    def _private_ttl(self) -> int:
        return self.connection_panel.get_private_ttl()

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
        self.connection_panel.set_remember_checked(bool(data.get("remember", True)))
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
        self.connection_panel.set_theme_checked(theme == "dark")
        self._apply_theme(theme)

    def save_settings(self) -> None:
        cfg = self._current_config()
        profile_name = self.connection_panel.current_profile() or "Default"
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
            "theme": "dark" if self.connection_panel.is_theme_dark() else "light",
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

    def select_files(self) -> None:
        paths, _ = QFileDialog.getOpenFileNames(self, "Select files")
        if not paths:
            return
        items = []
        for path in paths:
            size = os.path.getsize(path)
            target_rel = os.path.basename(path)
            items.append((path, target_rel, size))
        self.transfer_queue.add_items(items)

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
        self.transfer_queue.add_items(items)

    def upload_selected_file(self) -> None:
        cfg = self._current_config()

        items = self.transfer_queue.get_items()
        if not items:
            QMessageBox.warning(self, "No files", "Select one or more files first.")
            return
        if not cfg["bucket_id"]:
            QMessageBox.warning(self, "Missing data", "Bucket ID is required.")
            return
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
            files_list = list(files)
            self.bucket_browser.set_file_rows(files_list, cfg["prefix"])
            self.set_status(f"Loaded {len(files_list)} files")

        self._run_bg(task, done)

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
            assets = data.get("assets", [])
            return {"repo": repo, "tag": tag, "url": html_url, "name": name, "assets": assets}

        def done(result: object) -> None:
            info = dict(result)
            latest_tag = str(info.get("tag", ""))
            latest_ver = parse_semver(latest_tag)
            current_ver = parse_semver(APP_VERSION)
            if latest_ver > current_ver and info.get("url"):
                answer = QMessageBox.question(
                    self,
                    "Update available",
                    f"Current: v{APP_VERSION}\nLatest: {latest_tag}\n\nDownload and install update now?",
                )
                if answer == QMessageBox.Yes:
                    self.start_self_update(info)
                self.set_status(f"Update available: {latest_tag}")
                return
            QMessageBox.information(
                self,
                "Updates",
                f"You are up to date.\nCurrent: v{APP_VERSION}\nLatest: {latest_tag or 'unknown'}",
            )
            self.set_status("No updates found")

        self._run_bg(task, done)

    def _pick_update_asset(self, info: Dict) -> Optional[Dict]:
        assets = info.get("assets", [])
        if not isinstance(assets, list):
            return None
        if os.name == "nt":
            for a in assets:
                name = str(a.get("name", "")).lower()
                if name.endswith(".exe"):
                    return a
        elif sys.platform == "darwin":
            for a in assets:
                name = str(a.get("name", "")).lower()
                if name.endswith(".dmg"):
                    return a
        return None

    def start_self_update(self, info: Dict) -> None:
        asset = self._pick_update_asset(info)
        if not asset:
            QMessageBox.information(
                self,
                "Update",
                "No compatible update asset found for this OS. Opening release page instead.",
            )
            if info.get("url"):
                QDesktopServices.openUrl(QUrl(str(info["url"])))
            return

        download_url = str(asset.get("browser_download_url", ""))
        asset_name = str(asset.get("name", "update.bin"))
        if not download_url:
            QMessageBox.warning(self, "Update", "Update asset has no download URL.")
            return

        self.set_status(f"Downloading update: {asset_name} ...")

        def task(progress):
            target_path = Path(tempfile.gettempdir()) / asset_name
            with requests.get(download_url, stream=True, timeout=120) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("Content-Length", "0") or 0)
                written = 0
                with target_path.open("wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        if not chunk:
                            continue
                        f.write(chunk)
                        written += len(chunk)
                        pct = int((written * 100) / max(1, total)) if total else 0
                        progress(pct, f"Downloading update... {format_bytes(written)}")
            progress(100, "Update downloaded.")
            return str(target_path)

        def done(downloaded_path: object) -> None:
            path = str(downloaded_path)
            if os.name == "nt":
                self._install_update_windows(path)
                return
            if sys.platform == "darwin":
                self._install_update_macos(path)
                return
            QMessageBox.information(self, "Update", f"Downloaded update to:\n{path}")

        def on_progress(pct: int, text: str) -> None:
            self.progress_bar.setValue(max(0, min(100, pct)))
            self.progress_label.setText(text)

        self._run_bg(task, done, on_progress)

    def _install_update_windows(self, downloaded_exe: str) -> None:
        if not getattr(sys, "frozen", False):
            QMessageBox.information(
                self,
                "Update downloaded",
                f"Downloaded updater:\n{downloaded_exe}\n\nRun it manually in dev mode.",
            )
            return
        current_exe = Path(sys.executable)
        pid = os.getpid()
        updater_bat = Path(tempfile.gettempdir()) / "playua_update.bat"
        script = (
            "@echo off\r\n"
            f"set PID={pid}\r\n"
            ":waitloop\r\n"
            'tasklist /FI "PID eq %PID%" | find "%PID%" >nul\r\n'
            "if not errorlevel 1 (\r\n"
            "  timeout /t 1 /nobreak >nul\r\n"
            "  goto waitloop\r\n"
            ")\r\n"
            f'copy /Y "{downloaded_exe}" "{current_exe}" >nul\r\n'
            f'start "" "{current_exe}"\r\n'
            'del "%~f0"\r\n'
        )
        updater_bat.write_text(script, encoding="utf-8")
        subprocess.Popen(["cmd", "/c", str(updater_bat)], creationflags=0x08000000)
        QMessageBox.information(self, "Updating", "Update downloaded. App will restart now.")
        QApplication.quit()

    def _install_update_macos(self, downloaded_dmg: str) -> None:
        if not getattr(sys, "frozen", False):
            try:
                subprocess.Popen(["open", downloaded_dmg])
            except Exception as exc:
                QMessageBox.warning(self, "Update", f"Failed to open DMG automatically: {exc}")
                return
            QMessageBox.information(
                self,
                "Update downloaded",
                "Opened DMG.\nInstall the app from DMG manually in dev mode.",
            )
            return

        app_path = Path(sys.executable).resolve().parents[2]
        target_app = Path("/Applications") / app_path.name
        if not target_app.parent.exists() or not os.access(str(target_app.parent), os.W_OK):
            target_app = app_path

        updater_script = Path(tempfile.gettempdir()) / "playua_update_macos.sh"
        pid = os.getpid()
        dmg_q = shlex.quote(downloaded_dmg)
        target_q = shlex.quote(str(target_app))
        app_q = shlex.quote(str(app_path))
        script = f"""#!/bin/bash
set -e
PID="{pid}"
DMG={dmg_q}
TARGET_APP={target_q}
CURRENT_APP={app_q}
MOUNTPOINT="$(mktemp -d /tmp/playua_update_mount.XXXXXX)"

while kill -0 "$PID" >/dev/null 2>&1; do
  sleep 1
done

hdiutil attach "$DMG" -mountpoint "$MOUNTPOINT" -nobrowse -quiet
SRC_APP="$(find "$MOUNTPOINT" -maxdepth 1 -type d -name '*.app' | head -n1)"
if [[ -z "$SRC_APP" ]]; then
  hdiutil detach "$MOUNTPOINT" -quiet || true
  exit 1
fi

rm -rf "$TARGET_APP"
ditto "$SRC_APP" "$TARGET_APP"
hdiutil detach "$MOUNTPOINT" -quiet || true
open "$TARGET_APP"
"""
        updater_script.write_text(script, encoding="utf-8")
        os.chmod(updater_script, 0o755)
        subprocess.Popen(["/bin/bash", str(updater_script)])
        QMessageBox.information(
            self,
            "Updating",
            "Update downloaded. App will close now and install the update automatically.",
        )
        QApplication.quit()

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
        preview_files = self._previewable_file_names()
        if file_name not in preview_files:
            preview_files.append(file_name)
        state = {"idx": max(0, preview_files.index(file_name)), "req": 0}

        def update_nav_state() -> None:
            idx = int(state["idx"])
            dialog.set_navigation_state(idx > 0, idx < len(preview_files) - 1)
            dialog.set_download_enabled(True)

        def render_current() -> None:
            current_name = preview_files[int(state["idx"])]
            current_ext = Path(current_name).suffix.lower()
            req_id = int(state["req"]) + 1
            state["req"] = req_id
            dialog.status_label.setText(f"Loading preview: {current_name}")
            self._focus_file_in_table(current_name)

            def task():
                local_cfg = self._current_config()
                self._ensure_authorized(local_cfg)
                url = self._build_preview_url(local_cfg, current_name)
                if current_ext in image_ext:
                    resp = requests.get(url, timeout=30)
                    resp.raise_for_status()
                    return {"kind": "image", "data": resp.content, "name": current_name, "req": req_id}
                if current_ext in video_ext:
                    return {"kind": "video", "url": url, "name": current_name, "req": req_id}
                return {"kind": "audio", "url": url, "name": current_name, "req": req_id}

            def done(result: object) -> None:
                info = dict(result)
                if int(info.get("req", -1)) != int(state["req"]):
                    return
                kind = str(info.get("kind", ""))
                if kind == "image":
                    dialog.show_image(str(info.get("name", "")), bytes(info.get("data", b"")))
                    return
                if kind == "video":
                    dialog.show_media(str(info.get("name", "")), str(info.get("url", "")), is_video=True)
                    return
                dialog.show_media(str(info.get("name", "")), str(info.get("url", "")), is_video=False)

            self._run_bg(task, done)

        def go_prev() -> None:
            if int(state["idx"]) <= 0:
                return
            state["idx"] = int(state["idx"]) - 1
            update_nav_state()
            render_current()

        def go_next() -> None:
            if int(state["idx"]) >= len(preview_files) - 1:
                return
            state["idx"] = int(state["idx"]) + 1
            update_nav_state()
            render_current()

        def download_current() -> None:
            self.download_single_file(preview_files[int(state["idx"])])

        dialog.set_navigation_handlers(go_prev, go_next, download_current)
        update_nav_state()
        render_current()

    def preview_selected_file(self) -> None:
        file_name = self._selected_file_name()
        if not file_name:
            QMessageBox.warning(self, "Preview", "Select a file in the Files tab.")
            return
        self._open_preview_dialog_for_file(file_name)

    def _previewable_file_names(self) -> List[str]:
        image_ext = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}
        video_ext = {".mp4", ".mov", ".webm", ".mkv", ".m4v"}
        audio_ext = {".mp3", ".wav", ".flac", ".m4a", ".aac", ".ogg"}
        allowed = image_ext | video_ext | audio_ext
        names: List[str] = []
        for row in self.browser_rows:
            if str(row.get("kind", "")) == "folder":
                continue
            full_name = str(row.get("fileName", ""))
            if not full_name:
                continue
            if Path(full_name).suffix.lower() in allowed:
                names.append(full_name)
        return names

    def _focus_file_in_table(self, file_name: str) -> None:
        for row in range(self.table.rowCount()):
            item = self.table.item(row, 0)
            if not item:
                continue
            full_name = str(item.data(Qt.UserRole) or item.text())
            if full_name == file_name:
                self.table.selectRow(row)
                self.table.scrollToItem(item, QAbstractItemView.PositionAtCenter)
                return

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
            self.share_manager.append_share(file_name, "public", url, None)
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
            self.share_manager.append_share(file_name, "public", url, None)
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
            self.share_manager.append_share(file_name, "private", url, self._private_ttl())
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
            self.share_manager.append_share(file_name, "private", url, self._private_ttl())
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
    if os.name == "nt":
        try:
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(APP_USER_MODEL_ID)
        except Exception:
            pass
    app = QApplication([])
    icon_path = resolve_app_icon_path()
    if icon_path and should_set_runtime_icon():
        app.setWindowIcon(QIcon(str(icon_path)))
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == "__main__":
    run()
