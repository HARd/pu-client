import os
from typing import List, Tuple
from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QGroupBox, QVBoxLayout, QLabel, QTableWidget, QTableWidgetItem,
    QHeaderView, QAbstractItemView, QPushButton, QHBoxLayout, QMessageBox, QMenu, QWidget
)
from app.core.utils import format_bytes

class TransferQueueWidget(QGroupBox):
    queueChanged = Signal()

    def __init__(self, parent: QWidget = None):
        super().__init__("Upload Queue", parent)
        self.selected_upload_items: List[Tuple[str, str, int]] = []
        self._build_ui()
        self._configure_hints()
        self._polish_tables()
        self._setup_context_menu()

    def _build_ui(self) -> None:
        queue_layout = QVBoxLayout(self)
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
        self.remove_selected_btn.setObjectName("dangerBtn")
        queue_actions.addWidget(self.remove_selected_btn)
        queue_layout.addLayout(queue_actions)

        self.remove_selected_btn.clicked.connect(self.remove_selected_upload_items)

    def _configure_hints(self) -> None:
        self.remove_selected_btn.setToolTip("Remove selected rows from upload queue preview.")

    def _polish_tables(self) -> None:
        self.queue_table.verticalHeader().setVisible(False)
        self.queue_table.setShowGrid(False)
        self.queue_table.setWordWrap(False)

    def _setup_context_menu(self) -> None:
        self.queue_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.queue_table.customContextMenuRequested.connect(self._show_queue_context_menu)

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

    def add_items(self, items: List[Tuple[str, str, int]]) -> None:
        existing = {local_path for local_path, _, _ in self.selected_upload_items}
        for local_path, target_rel, size in items:
            if local_path not in existing:
                self.selected_upload_items.append((local_path, target_rel, size))
                existing.add(local_path)
        self._update_upload_selection_label()
        self._refresh_queue_table()
        self.queueChanged.emit()

    def clear_upload_selection(self) -> None:
        self.selected_upload_items = []
        self._update_upload_selection_label()
        self._refresh_queue_table()
        self.queueChanged.emit()

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
        self.queueChanged.emit()

    def _select_row_at_context(self, pos) -> None:
        item = self.queue_table.itemAt(pos)
        if item:
            self.queue_table.selectRow(item.row())

    def _show_queue_context_menu(self, pos) -> None:
        self._select_row_at_context(pos)
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

    def get_items(self) -> List[Tuple[str, str, int]]:
        return list(self.selected_upload_items)
        
    def has_focus(self) -> bool:
        return self.queue_table.hasFocus()
