import datetime as dt
import json
import os
import threading
from pathlib import Path
from typing import Dict, List

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
