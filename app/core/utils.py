import os
import re
import sys
from pathlib import Path
from typing import Optional, Tuple

APP_USER_MODEL_ID = "PlayUA.Desktop.Client"
DEFAULT_UPDATE_REPO = "HARd/pu-client"

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

def app_root_path() -> Path:
    if getattr(sys, "frozen", False):
        return Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    return Path(__file__).resolve().parent.parent.parent

def resolve_app_version() -> str:
    env_v = os.environ.get("APP_VERSION", "").strip()
    if env_v:
        return env_v.lstrip("v")
    root = app_root_path()
    for p in [root / "assets" / "version.txt", root / "build" / "version.txt"]:
        try:
            if p.exists():
                raw = p.read_text(encoding="utf-8").strip()
                if raw:
                    return raw.lstrip("v")
        except Exception:
            continue
    return "0.1.0"

APP_VERSION = resolve_app_version()

def resolve_app_icon_path() -> Optional[Path]:
    root = app_root_path()
    candidates = [
        root / "build" / "icons" / "app-icon.ico",
        root / "assets" / "icon.png",
        root / "assets" / "app-icon.png",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None

def should_set_runtime_icon() -> bool:
    # On macOS bundled apps should use the icon from Info.plist/.icns.
    # Setting a runtime PNG icon via Qt can override Dock icon styling.
    if sys.platform == "darwin" and getattr(sys, "frozen", False):
        return False
    return True
