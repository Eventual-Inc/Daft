"""Reproduction script for the missing `overlay` string function (Daft issue #3792).

Run from the repo root with the venv activated:
    python repro_overlay.py

Expected output: an ImportError because no `overlay` function has been
registered with Daft's function registry.
"""
from daft.functions import overlay

if __name__ == "__main__":
    print("overlay:", overlay)