#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -n "${PYTHON_BIN:-}" ]]; then
  CANDIDATES=("$PYTHON_BIN")
else
  CANDIDATES=("python" "python3" "/opt/homebrew/bin/python3" "/usr/bin/python3")
fi

PYTHON_BIN=""
for c in "${CANDIDATES[@]}"; do
  if [[ "$c" == */* ]]; then
    if [[ -x "$c" ]]; then
      PYTHON_BIN="$c"
      break
    fi
  elif command -v "$c" >/dev/null 2>&1; then
    PYTHON_BIN="$c"
    break
  fi
done

if [[ -z "$PYTHON_BIN" ]]; then
  echo "Python not found in PATH."
  exit 1
fi

if ! "$PYTHON_BIN" -m PyInstaller --version >/dev/null 2>&1; then
  echo "PyInstaller module not found for $PYTHON_BIN. Install build deps first:"
  echo "  $PYTHON_BIN -m pip install -r requirements-build.txt"
  exit 1
fi

if ! "$PYTHON_BIN" -c "import requests; import PySide6" >/dev/null 2>&1; then
  echo "Runtime dependencies are missing for $PYTHON_BIN. Install app deps first:"
  echo "  $PYTHON_BIN -m pip install -r requirements.txt"
  exit 1
fi

rm -rf build dist

if ! "$PYTHON_BIN" scripts/prepare_icons.py; then
  echo "Failed to prepare app icons."
  exit 1
fi

ICON_ARG=()
if [[ -f "build/icons/app-icon.icns" ]]; then
  ICON_ARG=(--icon "build/icons/app-icon.icns")
fi

"$PYTHON_BIN" -m PyInstaller \
  --name playua-desktop-client \
  --windowed \
  --noconfirm \
  --add-data "assets/icon.png:assets" \
  "${ICON_ARG[@]}" \
  app/main.py

echo
echo "Build finished. Output in: $ROOT_DIR/dist"
