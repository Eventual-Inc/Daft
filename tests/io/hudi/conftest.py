from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest


@pytest.fixture(
    params=[
        "v6_complexkeygen_hivestyle",
        "v6_nonpartitioned",
        "v6_simplekeygen_nonhivestyle",
        "v6_simplekeygen_hivestyle_no_metafields",
        "v6_timebasedkeygen_nonhivestyle",
    ]
)
def get_testing_table_for_supported_cases(request, tmp_path) -> str:
    table_name = request.param
    zip_file_path = Path(__file__).parent.joinpath("data", f"{table_name}.zip")
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(tmp_path)
    return os.path.join(tmp_path, table_name)
