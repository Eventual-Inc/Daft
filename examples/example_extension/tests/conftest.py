"""Auto-build the native extension before running tests."""

from __future__ import annotations

import platform
import shutil
import subprocess
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
PACKAGE_DIR = PROJECT_DIR / "daft_ext_example"

LIB_EXT = {
    "Darwin": ".dylib",
    "Linux": ".so",
    "Windows": ".dll",
}.get(platform.system(), ".so")

LIB_NAME = f"libdaft_ext_example_native{LIB_EXT}"


def pytest_configure(config):
    """Build the extension with maturin and copy the library into the package."""
    # Build with maturin (handles RPATH, symbol visibility, platform naming).
    subprocess.check_call(
        ["maturin", "develop"],
        cwd=PROJECT_DIR,
    )

    # maturin develop installs to site-packages; copy the library into
    # the source package directory so load_extension() can find it.
    target_dir = _get_target_dir()
    src = target_dir / "debug" / LIB_NAME
    dst = PACKAGE_DIR / LIB_NAME

    if not src.exists():
        raise RuntimeError(f"Native library not found at {src}.\nRun 'maturin develop' from the extension directory.")

    # Copy if missing or stale.
    if not dst.exists() or src.stat().st_mtime > dst.stat().st_mtime:
        shutil.copy2(str(src), str(dst))


def _get_target_dir() -> Path:
    """Get the cargo target directory from workspace metadata."""
    import json

    result = subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=PROJECT_DIR,
    )
    metadata = json.loads(result)
    return Path(metadata["target_directory"])
