from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_base_daft_import_does_not_require_psycopg():
    code = """
import sys


class _BlockPsycopg:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "psycopg" or fullname.startswith("psycopg."):
            raise ImportError("psycopg intentionally blocked")
        return None


sys.meta_path.insert(0, _BlockPsycopg())

import daft
from daft.catalog import Catalog
from daft.io import read_hologres

assert callable(daft.read_hologres)
assert daft.read_hologres is read_hologres
assert callable(Catalog.from_hologres)
"""
    repo_root = Path(__file__).resolve().parents[2]
    subprocess.run([sys.executable, "-c", code], check=True, cwd=repo_root)
