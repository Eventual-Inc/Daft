from __future__ import annotations

import uuid

import numpy as np
import pandas as pd
import pytest

import daft
from daft.datatype import DataType
from daft.expressions import col
from daft.series import Series
from tests.conftest import get_tests_daft_runner_name
from tests.cookbook.assets import COOKBOOK_DATA_CSV

COLUMNS = [
    "Unique Key",
    "Complaint Type",
    "Borough",
    "Created Date",
    "Descriptor",
    "Closed Date",
]
CsvPathAndColumns = tuple[str, list[str]]


@pytest.fixture(scope="function", params=["parquet", "csv"])
def daft_df(request, tmp_path):
    if request.param == "csv":
        df = daft.read_csv(COOKBOOK_DATA_CSV)
    elif request.param == "parquet":
        import pyarrow.csv as pacsv
        import pyarrow.parquet as papq

        tmp_file = tmp_path / str(uuid.uuid4())
        papq.write_table(pacsv.read_csv(COOKBOOK_DATA_CSV), str(tmp_file))
        df = daft.read_parquet(str(tmp_file))
    else:
        assert False, "Can only handle CSV/Parquet formats"
    return df.select(*[col(c) for c in COLUMNS])


@pytest.fixture(scope="function")
def service_requests_csv_pd_df():
    return pd.read_csv(COOKBOOK_DATA_CSV, keep_default_na=False)[COLUMNS]


@pytest.fixture(
    scope="module",
    params=[1, 2] if get_tests_daft_runner_name() != "native" else [1],
)
def repartition_nparts(request):
    """Adds a `n_repartitions` parameter to test cases which provides the number of partitions that the test case should repartition its dataset into for testing."""
    return request.param


# ─── Image helpers shared by image_hash test files ────────────────────────────


def make_image_df(arrays):
    """Create a Daft DataFrame with an image column from a list of numpy arrays."""
    s = Series.from_pylist(arrays, dtype=DataType.python())
    df = daft.from_pydict({"img": s})
    return df.select(df["img"].cast(DataType.image()))


def solid_rgb(r, g, b, h=16, w=16):
    arr = np.zeros((h, w, 3), dtype=np.uint8)
    arr[:, :, 0] = r
    arr[:, :, 1] = g
    arr[:, :, 2] = b
    return arr


def gradient_rgb(h=16, w=16, invert=False):
    """Left-to-right grayscale gradient, optionally inverted."""
    row = np.linspace(0 if not invert else 255, 255 if not invert else 0, w, dtype=np.uint8)
    arr = np.tile(row, (h, 1))
    return np.stack([arr, arr, arr], axis=-1)


def checkerboard(h=16, w=16, cell=4):
    """Black-and-white checkerboard pattern."""
    arr = np.zeros((h, w), dtype=np.uint8)
    for r in range(h):
        for c in range(w):
            if ((r // cell) + (c // cell)) % 2 == 0:
                arr[r, c] = 255
    return np.stack([arr, arr, arr], axis=-1)


def grad2d(h=32, w=32):
    """2-D ramp: both horizontal and vertical gradients so phash has rich DCT content."""
    row = np.linspace(0, 255, w, dtype=np.float32)
    col_vec = np.linspace(0, 255, h, dtype=np.float32)
    arr = (row[np.newaxis, :] * 0.5 + col_vec[:, np.newaxis] * 0.5).astype(np.uint8)
    return np.stack([arr, arr, arr], axis=-1)


def colorful_rgb(h=32, w=32):
    """Image with distinct color regions: red, green, blue, yellow quadrants."""
    arr = np.zeros((h, w, 3), dtype=np.uint8)
    arr[: h // 2, : w // 2] = [255, 0, 0]
    arr[: h // 2, w // 2 :] = [0, 255, 0]
    arr[h // 2 :, : w // 2] = [0, 0, 255]
    arr[h // 2 :, w // 2 :] = [255, 255, 0]
    return arr


def hamming_distance(a: bytes, b: bytes) -> int:
    assert len(a) == len(b)
    return sum(bin(x ^ y).count("1") for x, y in zip(a, b))
