import datetime as dt
from typing import Dict, List, Optional
from PySide6.QtCore import Qt, Signal, QUrl
from PySide6.QtWidgets import (
    QGroupBox, QVBoxLayout, QHBoxLayout, QPushButton, QTableWidget, 
    QHeaderView, QAbstractItemView, QTableWidgetItem, QMenu, QApplication, QMessageBox, QWidget
)
from PySide6.QtGui import QDesktopServices

class ShareManagerWidget(QGroupBox):
    statusChanged = Signal(str)

    def __init__(self, parent: QWidget = None):
        super().__init__("Share Manager", parent)
        self.share_rows: List[Dict] = []
        self._build_ui()
        self._setup_context_menu()
        self._polish_tables()

    def _build_ui(self) -> None:
        shares_layout = QVBoxLayout(self)

        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["File", "Type", "Created (UTC)", "Expires", "URL"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        shares_layout.addWidget(self.table)

        share_actions = QHBoxLayout()
        self.copy_share_btn = QPushButton("Copy Selected Share URL")
        self.copy_share_btn.setObjectName("secondaryBtn")
        self.open_share_btn = QPushButton("Open Selected Share URL")
        self.open_share_btn.setObjectName("secondaryBtn")
        share_actions.addWidget(self.copy_share_btn)
        share_actions.addWidget(self.open_share_btn)
        shares_layout.addLayout(share_actions)

        self.copy_share_btn.clicked.connect(self.copy_selected_share_url)
        self.open_share_btn.clicked.connect(self.open_selected_share_url)

    def _polish_tables(self) -> None:
        self.table.verticalHeader().setVisible(False)
        self.table.setShowGrid(False)
        self.table.setWordWrap(False)
        self.table.setSortingEnabled(False)

    def _setup_context_menu(self) -> None:
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_share_context_menu)

    def append_share(self, file_name: str, link_type: str, url: str, ttl_seconds: Optional[int]) -> None:
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
        self._refresh_table()

    def _refresh_table(self) -> None:
        self.table.setRowCount(len(self.share_rows))
        for i, row in enumerate(self.share_rows):
            self.table.setItem(i, 0, QTableWidgetItem(row["file"]))
            self.table.setItem(i, 1, QTableWidgetItem(row["type"]))
            self.table.setItem(i, 2, QTableWidgetItem(row["created"]))
            self.table.setItem(i, 3, QTableWidgetItem(row["expires"]))
            url_item = QTableWidgetItem(row["url"])
            url_item.setToolTip(row["url"])
            self.table.setItem(i, 4, url_item)

    def selected_url(self) -> Optional[str]:
        row = self.table.currentRow()
        if row < 0 or row >= len(self.share_rows):
            return None
        return self.share_rows[row]["url"]

    def _copy_text(self, text: str) -> None:
        QApplication.clipboard().setText(text)

    def copy_selected_share_url(self) -> None:
        url = self.selected_url()
        if not url:
            QMessageBox.information(self, "Share Manager", "Select a row in Share Manager.")
            return
        self._copy_text(url)
        self.statusChanged.emit("Share URL copied")

    def open_selected_share_url(self) -> None:
        url = self.selected_url()
        if not url:
            QMessageBox.information(self, "Share Manager", "Select a row in Share Manager.")
            return
        QDesktopServices.openUrl(QUrl(url))
        self.statusChanged.emit("Share URL opened")

    def _select_row_at_context(self, pos) -> None:
        item = self.table.itemAt(pos)
        if item:
            self.table.selectRow(item.row())

    def _show_share_context_menu(self, pos) -> None:
        self._select_row_at_context(pos)
        menu = QMenu(self)
        act_copy = menu.addAction("Copy Share URL")
        act_open = menu.addAction("Open Share URL")
        has_selection = self.selected_url() is not None
        act_copy.setEnabled(has_selection)
        act_open.setEnabled(has_selection)
        chosen = menu.exec(self.table.viewport().mapToGlobal(pos))
        if chosen == act_copy:
            self.copy_selected_share_url()
        elif chosen == act_open:
            self.open_selected_share_url()
