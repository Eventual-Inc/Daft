"""Auto-build the native extension before running tests."""

from __future__ import annotations

import platform
import shutil
import subprocess
from pathlib import Path

# Layout:
#   examples/example_extension/tests/conftest.py   <- this file
#   examples/example_extension/daft_ext_example/    <- package dir (library goes here)
#   target/debug/libdaft_ext_example.{so,dylib}     <- cargo build output

PROJECT_DIR = Path(__file__).resolve().parent.parent
PACKAGE_DIR = PROJECT_DIR / "daft_ext_example"

LIB_EXT = {
    "Darwin": ".dylib",
    "Linux": ".so",
    "Windows": ".dll",
}.get(platform.system(), ".so")

LIB_NAME = f"libdaft_ext_example{LIB_EXT}"


def pytest_configure(config):
    """Build the native extension and copy it into the package directory."""
    # Build the Rust library (cargo is fast if nothing changed).
    subprocess.check_call(
        ["cargo", "build", "-p", "daft-ext-example"],
        cwd=PROJECT_DIR,
    )

    # Locate the built library via cargo metadata.
    target_dir = _get_target_dir()
    src = target_dir / "debug" / LIB_NAME
    dst = PACKAGE_DIR / LIB_NAME

    if not src.exists():
        raise RuntimeError(
            f"Native library not found at {src}.\nRun 'cargo build -p daft-ext-example' from the workspace root."
        )

    # Copy if missing or stale.
    if not dst.exists() or src.stat().st_mtime > dst.stat().st_mtime:
        shutil.copy2(str(src), str(dst))


def _get_target_dir() -> Path:
    """Get the cargo target directory from workspace metadata."""
    result = subprocess.check_output(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=PROJECT_DIR,
    )
    import json

    metadata = json.loads(result)
    return Path(metadata["target_directory"])
