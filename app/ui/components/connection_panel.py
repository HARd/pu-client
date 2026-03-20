from typing import Dict, List, Tuple
from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QGroupBox, QGridLayout, QLabel, QLineEdit, QCheckBox, 
    QComboBox, QPushButton, QHBoxLayout, QWidget
)

class ConnectionPanel(QGroupBox):
    themeToggled = Signal(bool)
    profileChanged = Signal(str)
    saveProfileRequested = Signal()
    deleteProfileRequested = Signal()

    def __init__(self, parent: QWidget = None):
        super().__init__("Backblaze Connection", parent)
        self._build_ui()
        self._configure_hints()
        self._set_human_friendly_defaults()

    def _build_ui(self) -> None:
        grid = QGridLayout(self)
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
        
        self.dark_theme_check.toggled.connect(self.themeToggled.emit)
        self.profile_combo.currentTextChanged.connect(self.profileChanged.emit)
        self.profile_save_btn.clicked.connect(self.saveProfileRequested.emit)
        self.profile_delete_btn.clicked.connect(self.deleteProfileRequested.emit)

    def _configure_hints(self) -> None:
        self.key_id_input.setPlaceholderText("e.g. 004a7f3f...")
        self.app_key_input.setPlaceholderText("Application Key")
        self.bucket_id_input.setPlaceholderText("Bucket ID")
        self.bucket_name_input.setPlaceholderText("Bucket name")
        self.prefix_input.setPlaceholderText("optional/path")
        self.ttl_input.setPlaceholderText("3600")
        self.profile_save_btn.setToolTip("Save current connection fields into selected profile.")
        self.profile_delete_btn.setToolTip("Delete selected profile.")
        
    def _set_human_friendly_defaults(self) -> None:
        self.profile_save_btn.setObjectName("secondaryBtn")
        self.profile_delete_btn.setObjectName("dangerBtn")

    def get_config(self) -> Dict:
        return {
            "key_id": self.key_id_input.text().strip(),
            "app_key": self.app_key_input.text().strip(),
            "bucket_id": self.bucket_id_input.text().strip(),
            "bucket_name": self.bucket_name_input.text().strip(),
            "prefix": self.prefix_input.text().strip(),
            "remember": self.remember_check.isChecked(),
            "private_ttl": self.ttl_input.text().strip() or "3600",
        }

    def get_private_ttl(self) -> int:
        raw = self.ttl_input.text().strip()
        try:
            ttl = int(raw)
        except ValueError as exc:
            raise RuntimeError("Private URL TTL must be an integer.") from exc
        if ttl < 1 or ttl > 604800:
            raise RuntimeError("Private URL TTL must be between 1 and 604800 seconds.")
        return ttl
        
    def set_remember_checked(self, checked: bool) -> None:
        self.remember_check.setChecked(checked)

    def set_profiles(self, profiles: List[str], active_profile: str) -> None:
        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        self.profile_combo.addItems(profiles)
        self.profile_combo.setCurrentText(active_profile)
        self.profile_combo.blockSignals(False)
        
    def current_profile(self) -> str:
        return self.profile_combo.currentText().strip()

    def set_theme_checked(self, checked: bool) -> None:
        self.dark_theme_check.blockSignals(True)
        self.dark_theme_check.setChecked(checked)
        self.dark_theme_check.blockSignals(False)
        
    def is_theme_dark(self) -> bool:
        return self.dark_theme_check.isChecked()

    def apply_profile_payload(self, payload: Dict) -> None:
        self.key_id_input.setText(str(payload.get("key_id", "")))
        self.app_key_input.setText(str(payload.get("app_key", "")))
        self.bucket_id_input.setText(str(payload.get("bucket_id", "")))
        self.bucket_name_input.setText(str(payload.get("bucket_name", "")))
        self.prefix_input.setText(str(payload.get("prefix", "")))
        ttl = payload.get("private_ttl", 3600)
        self.ttl_input.setText(str(ttl))

    def set_busy(self, busy: bool) -> None:
        controls = [
            self.profile_combo,
            self.profile_save_btn,
            self.profile_delete_btn,
            self.dark_theme_check,
        ]
        for w in controls:
            w.setEnabled(not busy)
