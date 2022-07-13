from typing import Dict, List

import pytest

from daft.dataframe import DataFrame

@pytest.fixture(scope="function")
def valid_data() -> List[Dict[str, float]]:
    items = [
        {"sepal_length": 5.1, "sepal_width": 3.5,
         "petal_length": 1.4, "petal_width": 0.2, "variety": "Setosa"},
        {"sepal_length": 4.9, "sepal_width": 3.0,
         "petal_length": 1.4, "petal_width": 0.2, "variety": "Setosa"},
        {"sepal_length": 4.7, "sepal_width": 3.2,
         "petal_width": 1.3, "petal_width": 0.2, "variety": "Setosa"},
    ]
    return items


# def test_create_dataframe(valid_data: List[Dict[str, float]]) -> None:
#     df = DataFrame.from_items(valid_data)
#     assert df.columns == ["sepal_length", "sepal_width", "petal_length", "petal_width", "variety"]

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