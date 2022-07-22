import csv
import tempfile
from typing import Dict, List

import pytest

from daft.dataframe import DataFrame


@pytest.fixture(scope="function")
def valid_data() -> List[Dict[str, float]]:
    items = [
        {"sepal_length": 5.1, "sepal_width": 3.5, "petal_length": 1.4, "petal_width": 0.2, "variety": "Setosa"},
        {"sepal_length": 4.9, "sepal_width": 3.0, "petal_length": 1.4, "petal_width": 0.2, "variety": "Setosa"},
        {"sepal_length": 4.7, "sepal_width": 3.2, "petal_length": 1.3, "petal_width": 0.2, "variety": "Setosa"},
    ]
    return items


def test_create_dataframe(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    assert [c.name() for c in df.schema()] == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]


def test_create_dataframe_pydict(valid_data: List[Dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    df = DataFrame.from_pydict(pydict)
    assert [c.name() for c in df.schema()] == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]


def test_create_dataframe_csv(valid_data: List[Dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        df = DataFrame.from_csv(f.name)
        assert [c.name() for c in df.schema()] == [
            "sepal_length",
            "sepal_width",
            "petal_length",
            "petal_width",
            "variety",
        ]


# def test_select_dataframe(valid_data: List[Dict[str, float]]) -> None:
#     df = DataFrame.from_items(valid_data)
#     assert df.columns == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]
#     df = df.select("sepal_length", "sepal_width")
#     assert df.columns == ["sepal_length", "sepal_width"]

# def test_dataframe_row_view(valid_data: List[Dict[str, float]]) -> None:
#     df = DataFrame.from_items(valid_data)
#     for i, row in enumerate(valid_data):
#         rv = df[i]
#         for k, v in row.items():
#             assert getattr(rv, k) == v
