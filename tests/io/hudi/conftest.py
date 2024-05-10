from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest


def _extract_testing_table(zip_file_path, target_path, table_name) -> str:
    with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
        zip_ref.extractall(target_path)
    return os.path.join(target_path, table_name)


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
    return _extract_testing_table(zip_file_path, tmp_path, table_name)


@pytest.fixture(
    params=[
        "v6_empty",
    ]
)
def get_empty_table(request, tmp_path) -> str:
    table_name = request.param
    zip_file_path = Path(__file__).parent.joinpath("data", f"{table_name}.zip")
    return _extract_testing_table(zip_file_path, tmp_path, table_name)
