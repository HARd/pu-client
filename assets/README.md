# App Icon

Place the source icon PNG here as:

- `assets/icon.png` (preferred)
- `assets/app-icon.png` (legacy fallback)

Recommended: square image, ideally `1024x1024`.

During build, `scripts/prepare_icons.py` will generate:

- `build/icons/app-icon.ico` for Windows
- `build/icons/app-icon.icns` for macOS (when built on macOS)
