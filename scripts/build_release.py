from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parents[1]
BUILD_DIR = ROOT / "build"
DIST_DIR = ROOT / "dist"
RELEASE_DIR = ROOT / "release"

sys.path.insert(0, str(ROOT))

from spreadsheet_tool.version import APP_NAME, APP_SLUG, __version__  # noqa: E402


def run(command: list[str]) -> None:
    subprocess.run(command, cwd=ROOT, check=True)


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def write_release_readme(target: Path) -> None:
    content = f"""\
{APP_NAME} v{__version__}

Quick start
1. Double-click `启动表格处理工具.bat`.
2. If SmartScreen prompts, choose More info -> Run anyway.
3. No separate Python installation is required for this portable package.

What is included
- Python runtime
- pandas / openpyxl
- tkinter / tkinterdnd2 drag-and-drop support
- All UI files needed to run the desktop app on Windows x64

Tested build environment
- Windows x64
- Python 3.13

Notes
- Keep the whole folder structure unchanged.
- `wenjian-chuli.exe` is the packaged application binary.
- `启动表格处理工具.vbs` starts the app without showing a console window.
"""
    target.write_text(content, encoding="utf-8")


def copy_if_exists(source: Path, target: Path) -> None:
    if source.exists():
        shutil.copy2(source, target)


def prune_packaged_files(package_dir: Path) -> None:
    tkdnd_root = package_dir / "_internal" / "tkinterdnd2" / "tkdnd"
    if tkdnd_root.exists():
        for child in tkdnd_root.iterdir():
            if child.is_dir() and child.name != "win-x64":
                shutil.rmtree(child)
        for lib_file in tkdnd_root.rglob("*.lib"):
            lib_file.unlink()


def zip_directory(source_dir: Path, zip_path: Path) -> None:
    remove_path(zip_path)
    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
        for path in sorted(source_dir.rglob("*")):
            archive.write(path, path.relative_to(source_dir.parent))


def main() -> None:
    package_name = f"{APP_SLUG}-windows-x64-v{__version__}"
    package_dir = RELEASE_DIR / package_name
    zip_path = RELEASE_DIR / f"{package_name}.zip"

    for path in [BUILD_DIR, DIST_DIR, package_dir, zip_path]:
        remove_path(path)

    RELEASE_DIR.mkdir(parents=True, exist_ok=True)

    run([sys.executable, "-m", "unittest", "discover", "-v"])
    run([sys.executable, "-m", "compileall", "main.py", "spreadsheet_tool", "tests"])
    run([sys.executable, "-m", "PyInstaller", "--noconfirm", "--clean", "wenjian_chuli.spec"])

    shutil.copytree(DIST_DIR / APP_SLUG, package_dir)
    prune_packaged_files(package_dir)
    copy_if_exists(ROOT / "启动表格处理工具.bat", package_dir / "启动表格处理工具.bat")
    copy_if_exists(ROOT / "启动表格处理工具.vbs", package_dir / "启动表格处理工具.vbs")
    write_release_readme(package_dir / "README.txt")
    zip_directory(package_dir, zip_path)

    print(zip_path)


if __name__ == "__main__":
    main()
