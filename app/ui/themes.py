LIGHT_THEME = """
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
QMenu {
    background: #ffffff;
    color: #0f172a;
    border: 1px solid #dbe3f0;
    padding: 6px;
}
QMenu::item {
    padding: 6px 10px;
    border-radius: 6px;
}
QMenu::item:selected {
    background: #e0ecff;
    color: #0f172a;
}
QMenu::separator {
    height: 1px;
    background: #e2e8f0;
    margin: 5px 8px;
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

DARK_THEME = """
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
QMenu {
    background: #0f1117;
    color: #e5e7eb;
    border: 1px solid #2f3544;
    padding: 6px;
}
QMenu::item {
    padding: 6px 10px;
    border-radius: 6px;
}
QMenu::item:selected {
    background: #1f304d;
    color: #f8fafc;
}
QMenu::separator {
    height: 1px;
    background: #2f3544;
    margin: 5px 8px;
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
