#!/usr/bin/env python3
from __future__ import annotations

import platform
import shutil
import subprocess
import sys
from pathlib import Path

from PIL import Image, ImageDraw


ROOT_DIR = Path(__file__).resolve().parent.parent
ASSETS_DIR = ROOT_DIR / "assets"
BUILD_ICONS_DIR = ROOT_DIR / "build" / "icons"
NORMALIZED_PNG = BUILD_ICONS_DIR / "app-icon-1024.png"
MAC_NORMALIZED_PNG = BUILD_ICONS_DIR / "app-icon-macos-1024.png"
WIN_ICO = BUILD_ICONS_DIR / "app-icon.ico"
MAC_ICNS = BUILD_ICONS_DIR / "app-icon.icns"


def resolve_source_icon() -> Path | None:
    preferred = ASSETS_DIR / "icon.png"
    legacy = ASSETS_DIR / "app-icon.png"
    if preferred.exists():
        return preferred
    if legacy.exists():
        return legacy
    return None


def normalize_png(source_png: Path) -> None:
    with Image.open(source_png) as img:
        img = img.convert("RGBA")
        width, height = img.size

        if width != height:
            side = min(width, height)
            left = (width - side) // 2
            top = (height - side) // 2
            img = img.crop((left, top, left + side, top + side))

        img = img.resize((1024, 1024), Image.Resampling.LANCZOS)
        img.save(NORMALIZED_PNG, format="PNG")


def build_macos_png() -> None:
    with Image.open(NORMALIZED_PNG) as img:
        img = img.convert("RGBA")
        size = 1024
        # Larger safe area for a more native macOS Dock appearance.
        inset = 116
        inner_size = size - (inset * 2)
        corner_radius = 190

        inner = img.resize((inner_size, inner_size), Image.Resampling.LANCZOS)

        mask = Image.new("L", (inner_size, inner_size), 0)
        draw = ImageDraw.Draw(mask)
        draw.rounded_rectangle((0, 0, inner_size - 1, inner_size - 1), radius=corner_radius, fill=255)

        rounded = Image.new("RGBA", (inner_size, inner_size), (0, 0, 0, 0))
        rounded.paste(inner, (0, 0), mask)

        canvas = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        canvas.paste(rounded, (inset, inset), rounded)
        canvas.save(MAC_NORMALIZED_PNG, format="PNG")


def build_ico() -> None:
    with Image.open(NORMALIZED_PNG) as img:
        img.save(
            WIN_ICO,
            format="ICO",
            sizes=[(16, 16), (24, 24), (32, 32), (48, 48), (64, 64), (128, 128), (256, 256)],
        )


def run_cmd(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def build_icns() -> bool:
    if platform.system() != "Darwin":
        return False

    if not shutil.which("sips") or not shutil.which("iconutil"):
        return False

    iconset_dir = BUILD_ICONS_DIR / "app.iconset"
    if iconset_dir.exists():
        shutil.rmtree(iconset_dir)
    iconset_dir.mkdir(parents=True, exist_ok=True)

    source_for_icns = MAC_NORMALIZED_PNG if MAC_NORMALIZED_PNG.exists() else NORMALIZED_PNG
    icon_sizes = [16, 32, 128, 256, 512]
    for size in icon_sizes:
        out = iconset_dir / f"icon_{size}x{size}.png"
        out2x = iconset_dir / f"icon_{size}x{size}@2x.png"
        run_cmd(["sips", "-z", str(size), str(size), str(source_for_icns), "--out", str(out)])
        run_cmd(["sips", "-z", str(size * 2), str(size * 2), str(source_for_icns), "--out", str(out2x)])

    run_cmd(["iconutil", "-c", "icns", str(iconset_dir), "-o", str(MAC_ICNS)])
    return True


def main() -> int:
    BUILD_ICONS_DIR.mkdir(parents=True, exist_ok=True)
    source_png = resolve_source_icon()

    if source_png is None:
        print(f"Icon source not found. Expected one of: {ASSETS_DIR / 'icon.png'} or {ASSETS_DIR / 'app-icon.png'}")
        return 0

    try:
        normalize_png(source_png)
        build_macos_png()
        build_ico()
        has_icns = build_icns()
    except Exception as exc:
        print(f"Failed to prepare icons: {exc}")
        return 1

    print(f"Prepared Windows icon: {WIN_ICO}")
    if has_icns and MAC_ICNS.exists():
        print(f"Prepared macOS icon: {MAC_ICNS}")
    else:
        print("macOS .icns was not generated on this platform.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
