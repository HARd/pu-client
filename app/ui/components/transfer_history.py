from typing import Dict, List, Optional
from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QGroupBox, QVBoxLayout, QTableWidget, 
    QHeaderView, QAbstractItemView, QTableWidgetItem, QMenu, QApplication, QWidget
)

class TransferHistoryWidget(QGroupBox):
    statusChanged = Signal(str)

    def __init__(self, parent: QWidget = None):
        super().__init__("Transfer History", parent)
        self._build_ui()
        self._setup_context_menu()
        self._polish_table()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        self.table = QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(["Time (UTC)", "Action", "Status", "Size", "Details"])
        self.table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        layout.addWidget(self.table)

    def _polish_table(self) -> None:
        self.table.verticalHeader().setVisible(False)
        self.table.setShowGrid(False)
        self.table.setWordWrap(False)
        self.table.setSortingEnabled(True)

    def _setup_context_menu(self) -> None:
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_history_context_menu)

    def _select_row_at_context(self, pos) -> None:
        item = self.table.itemAt(pos)
        if item:
            self.table.selectRow(item.row())

    def _copy_text(self, text: str) -> None:
        QApplication.clipboard().setText(text)

    def _show_history_context_menu(self, pos) -> None:
        self._select_row_at_context(pos)
        row = self.table.currentRow()
        menu = QMenu(self)
        act_copy_row = menu.addAction("Copy Row")
        act_copy_row.setEnabled(row >= 0)
        chosen = menu.exec(self.table.viewport().mapToGlobal(pos))
        if chosen != act_copy_row or row < 0:
            return
        
        values = []
        for col in range(self.table.columnCount()):
            item = self.table.item(row, col)
            values.append(item.text() if item else "")
        self._copy_text(" | ".join(values))
        self.statusChanged.emit("History row copied")

    def populate(self, rows: List[Dict]) -> None:
        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(rows))
        for i, row in enumerate(rows):
            ts_raw = str(row.get("ts", ""))
            ts = ts_raw.replace("T", " ").replace("+00:00", "")
            action = str(row.get("action", ""))
            status = str(row.get("status", ""))
            details = str(row.get("details", ""))
            
            from app.core.utils import format_bytes
            bytes_count = int(row.get("bytes", 0) or 0)
            
            self.table.setItem(i, 0, QTableWidgetItem(ts))
            self.table.setItem(i, 1, QTableWidgetItem(action))
            self.table.setItem(i, 2, QTableWidgetItem(status))
            self.table.setItem(i, 3, QTableWidgetItem(format_bytes(bytes_count)))
            self.table.setItem(i, 4, QTableWidgetItem(details))
        self.table.setSortingEnabled(True)
