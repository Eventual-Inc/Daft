from __future__ import annotations

import platform
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HELLO_DIR = REPO_ROOT / "examples" / "hello"
ARTIFACT = "libhello.dylib" if platform.system() == "Darwin" else "libhello.so"


@pytest.fixture(scope="session")
def hello_extension_path() -> str:
    """Build the hello cdylib if needed and return the path to the artifact."""
    build_dir = HELLO_DIR / "target" / "release"
    artifact_path = build_dir / ARTIFACT
    if not artifact_path.exists():
        subprocess.run(
            ["cargo", "build", "--release", "--manifest-path", str(HELLO_DIR / "Cargo.toml")],
            check=True,
        )
    assert artifact_path.exists(), f"hello artifact missing at {artifact_path}"
    return str(artifact_path)
