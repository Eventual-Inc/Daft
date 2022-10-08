import csv
import tempfile
from typing import Dict, List

from daft.dataframe import DataFrame


def test_create_dataframe(valid_data: List[Dict[str, float]]) -> None:
    df = DataFrame.from_pylist(valid_data)
    assert df.column_names == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]


def test_create_dataframe_pydict(valid_data: List[Dict[str, float]]) -> None:
    pydict = {k: [item[k] for item in valid_data] for k in valid_data[0].keys()}
    df = DataFrame.from_pydict(pydict)
    assert df.column_names == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]


def test_create_dataframe_csv(valid_data: List[Dict[str, float]]) -> None:
    with tempfile.NamedTemporaryFile("w") as f:
        header = list(valid_data[0].keys())
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows([[item[col] for col in header] for item in valid_data])
        f.flush()

        df = DataFrame.from_csv(f.name)
        assert df.column_names == [
            "sepal_length",
            "sepal_width",
            "petal_length",
            "petal_width",
            "variety",
        ]
