import datetime as dt
from pathlib import Path
from typing import Dict, List, Optional
from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QGroupBox, QVBoxLayout, QHBoxLayout, QWidget, QLineEdit, QComboBox, 
    QPushButton, QTableWidget, QHeaderView, QAbstractItemView, QTableWidgetItem, QLabel, QMenu, QApplication
)
from app.core.utils import format_bytes

class BucketBrowserWidget(QGroupBox):
    # Signals
    refreshRequested = Signal()
    downloadFolderRequested = Signal(str)    # prefix
    downloadFileRequested = Signal(str)      # file_name
    downloadSelectedRequested = Signal()     # triggers download of whatever is selected
    previewRequested = Signal(str)           # file_name
    copyPublicLinkRequested = Signal()
    copyPrivateLinkRequested = Signal()
    openPublicLinkRequested = Signal()
    openPrivateLinkRequested = Signal()
    statusChanged = Signal(str)
    selectionChanged = Signal()
    
    def __init__(self, parent: QWidget = None):
        super().__init__("Files In Bucket", parent)
        self.file_rows: List[Dict] = []
        self.filtered_rows: List[Dict] = []
        self.browser_rows: List[Dict] = []
        self.base_bucket_prefix = ""
        self.current_folder_prefix = ""
        
        self._build_ui()
        self._setup_context_menu()
        self._polish_tables()

    def _build_ui(self) -> None:
        files_layout = QVBoxLayout(self)
        files_layout.setSpacing(8)

        filters_row = QHBoxLayout()
        self.folder_back_btn = QPushButton("Back")
        self.folder_back_btn.setObjectName("secondaryBtn")
        self.folder_back_btn.setEnabled(False)
        self.folder_back_btn.setToolTip("Go to parent folder in bucket browser.")
        self.folder_back_btn.clicked.connect(self.open_parent_folder)

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
        self.download_folder_current_btn.setObjectName("secondaryBtn")
        self.download_folder_current_btn.setToolTip("Download currently opened folder from bucket.")
        
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.setObjectName("secondaryBtn")

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
        self.table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Fixed)
        self.table.horizontalHeader().setSectionResizeMode(5, QHeaderView.Fixed)
        self.table.setColumnWidth(4, 96)
        self.table.setColumnWidth(5, 118)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setAlternatingRowColors(True)
        files_layout.addWidget(self.table, 1)

        self.search_input.textChanged.connect(self._apply_filters)
        self.type_filter.currentIndexChanged.connect(self._apply_filters)
        self.size_filter.currentIndexChanged.connect(self._apply_filters)
        self.table.itemSelectionChanged.connect(self._on_table_selection_changed)
        self.table.itemDoubleClicked.connect(self._on_table_item_double_clicked)
        self.download_folder_current_btn.clicked.connect(self.download_current_folder)
        self.refresh_btn.clicked.connect(self.refreshRequested.emit)
        
    def _polish_tables(self) -> None:
        self.table.verticalHeader().setVisible(False)
        self.table.setShowGrid(False)
        self.table.setWordWrap(False)
        self.table.setSortingEnabled(False)

    def _setup_context_menu(self) -> None:
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._show_files_context_menu)

    def clear(self) -> None:
        self.file_rows = []
        self.filtered_rows = []
        self.browser_rows = []
        self.table.setRowCount(0)

    def set_file_rows(self, files: List[Dict], base_prefix: str) -> None:
        self.file_rows = files
        self.base_bucket_prefix = base_prefix.strip("/")
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
        
    def set_busy(self, busy: bool) -> None:
        controls = [
            self.refresh_btn,
            self.search_input,
            self.type_filter,
            self.size_filter,
        ]
        for w in controls:
            w.setEnabled(not busy)

    def focus_search(self) -> None:
        self.search_input.setFocus()
        self.search_input.selectAll()

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
            return 100 * mb <= size <= 1 * gb
        if choice == "> 1 GB":
            return size > 1 * gb
        return True

    def _apply_filters(self) -> None:
        query = self.search_input.text().strip().lower()
        type_choice = self.type_filter.currentText()
        size_choice = self.size_filter.currentText()

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

        self.browser_rows = self.filtered_rows
        self.table.setRowCount(len(self.browser_rows))
        for i, row in enumerate(self.browser_rows):
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
            if not is_folder:
                size_item.setToolTip(f"{size} bytes")
            uploaded_item = QTableWidgetItem(uploaded)

            self.table.setItem(i, 0, name_item)
            self.table.setItem(i, 1, type_item)
            self.table.setItem(i, 2, size_item)
            self.table.setItem(i, 3, uploaded_item)
            self.table.removeCellWidget(i, 4)
            self.table.removeCellWidget(i, 5)
            
        self._refresh_table_row_actions()
        self._update_folder_path_ui()
        self.statusChanged.emit(f"Loaded {len(self.filtered_rows)} file(s) (filtered)")

    def _refresh_table_row_actions(self) -> None:
        current_row = self.table.currentRow()
        for row in range(self.table.rowCount()):
            self.table.removeCellWidget(row, 4)
            self.table.removeCellWidget(row, 5)
            if row != current_row:
                continue
            item = self.table.item(row, 0)
            if not item:
                continue
            kind = item.data(Qt.UserRole + 1)
            file_name = str(item.data(Qt.UserRole) or item.text())
            is_folder = kind == "folder"
            if not is_folder:
                self.table.setCellWidget(row, 4, self._new_table_preview_button(file_name, enabled=True))
            self.table.setCellWidget(row, 5, self._new_table_download_button(file_name, is_folder))

    def _new_table_preview_button(self, file_name: str, enabled: bool = True) -> QPushButton:
        btn = QPushButton("Preview")
        btn.setObjectName("secondaryBtn")
        self._style_table_action_button(btn, width=84)
        btn.setEnabled(enabled)
        btn.clicked.connect(lambda _=False, name=file_name: self.previewRequested.emit(name))
        return btn

    def _new_table_download_button(self, file_name: str, is_folder: bool) -> QPushButton:
        btn = QPushButton("Download")
        btn.setObjectName("secondaryBtn")
        self._style_table_action_button(btn, width=96 if not is_folder else 112)
        if is_folder:
            btn.clicked.connect(lambda _=False, prefix=file_name: self.downloadFolderRequested.emit(prefix))
        else:
            btn.clicked.connect(lambda _=False, name=file_name: self.downloadFileRequested.emit(name))
        return btn

    def _style_table_action_button(self, btn: QPushButton, width: int) -> None:
        btn.setMinimumHeight(24)
        btn.setMaximumHeight(24)
        btn.setMinimumWidth(width)
        btn.setMaximumWidth(width)
        btn.setStyleSheet("padding: 2px 8px; border-radius: 7px; font-size: 12px;")

    def _extract_file_size(self, row: Dict) -> int:
        raw = row.get("size", row.get("contentLength", 0))
        try:
            return int(raw)
        except Exception:
            return 0
            
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

    def selected_file_names(self) -> List[str]:
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
        
    def _on_table_selection_changed(self) -> None:
        self._refresh_table_row_actions()
        self.selectionChanged.emit()

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
        self.downloadFolderRequested.emit(prefix)

    def _show_files_context_menu(self, pos) -> None:
        item = self.table.itemAt(pos)
        if item:
            self.table.selectRow(item.row())

        selected = self.selected_file_names()
        has_selection = bool(selected)
        current_item = self.table.item(self.table.currentRow(), 0) if self.table.currentRow() >= 0 else None
        is_folder_selected = bool(current_item and current_item.data(Qt.UserRole + 1) == "folder")
        selected_folder = str(current_item.data(Qt.UserRole) or "") if is_folder_selected else ""

        menu = QMenu(self)
        act_copy_public = menu.addAction("Copy Public Link")
        act_open_public = menu.addAction("Open Public Link")
        act_copy_private = menu.addAction("Copy Private Link")
        act_open_private = menu.addAction("Open Private Link")
        def req_preview():
            if has_selection and len(selected) > 0:
                self.previewRequested.emit(selected[0])
        act_preview = menu.addAction("Preview Selected")
        menu.addSeparator()
        act_download = menu.addAction("Download Folder" if is_folder_selected else "Download Selected")
        act_refresh = menu.addAction("Refresh List")

        for action in [act_copy_public, act_open_public, act_copy_private, act_open_private, act_preview]:
            action.setEnabled(has_selection)
        act_download.setEnabled(has_selection or is_folder_selected)

        chosen = menu.exec(self.table.viewport().mapToGlobal(pos))
        if chosen == act_copy_public:
            self.copyPublicLinkRequested.emit()
        elif chosen == act_open_public:
            self.openPublicLinkRequested.emit()
        elif chosen == act_copy_private:
            self.copyPrivateLinkRequested.emit()
        elif chosen == act_open_private:
            self.openPrivateLinkRequested.emit()
        elif chosen == act_preview:
            req_preview()
        elif chosen == act_download:
            if is_folder_selected and selected_folder:
                self.downloadFolderRequested.emit(selected_folder)
            else:
                self.downloadSelectedRequested.emit()
        elif chosen == act_refresh:
            self.refreshRequested.emit()
