from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest


@pytest.fixture
def unzip_table_0_x_cow_partitioned(tmp_path):
    zip_file_path = Path(__file__).parent.joinpath("data", "0.x_cow_partitioned.zip")
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(tmp_path)
    return os.path.join(tmp_path, "trips_table")
