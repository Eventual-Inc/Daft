from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest

HUDI_DATA_DIR = Path(__file__).parent.parent.parent / "io" / "hudi" / "data"


def _extract_table(zip_name: str, tmp_path: Path) -> str:
    zip_path = HUDI_DATA_DIR / f"{zip_name}.zip"
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(tmp_path)
    return os.path.join(tmp_path, zip_name)


V9_PARTITIONED_TABLES = [
    "v9_txns_simple_meta",
    "v9_txns_simple_nometa",
    "v9_txns_complex_meta",
]

V9_NONPARTITIONED_TABLES = [
    "v9_txns_nonpart_meta",
    "v9_txns_nonpart_nometa",
]

V9_TIMEBASEDKEYGEN_TABLES = [
    "v9_timebasedkeygen_nonhivestyle",
    "v9_timebasedkeygen_epochmillis",
    "v9_timebasedkeygen_unixtimestamp",
]

V9_ALL_TABLES = [
    *V9_PARTITIONED_TABLES,
    *V9_NONPARTITIONED_TABLES,
    *V9_TIMEBASEDKEYGEN_TABLES,
]


@pytest.fixture(params=V9_ALL_TABLES)
def v9_table(request, tmp_path) -> str:
    return _extract_table(request.param, tmp_path)


@pytest.fixture(params=V9_PARTITIONED_TABLES)
def v9_partitioned_table(request, tmp_path) -> str:
    return _extract_table(request.param, tmp_path)


@pytest.fixture(params=V9_NONPARTITIONED_TABLES)
def v9_nonpartitioned_table(request, tmp_path) -> str:
    return _extract_table(request.param, tmp_path)


@pytest.fixture(params=V9_TIMEBASEDKEYGEN_TABLES)
def v9_timebasedkeygen_table(request, tmp_path) -> str:
    return _extract_table(request.param, tmp_path)


@pytest.fixture
def v9_txns_simple_meta(tmp_path) -> str:
    return _extract_table("v9_txns_simple_meta", tmp_path)
