from __future__ import annotations

from typing import Any

import pytest


@pytest.fixture(scope="function")
def valid_data() -> list[dict[str, Any]]:
    items = [
        {"sepal_length": 5.1, "sepal_width": 3.5, "petal_length": 1.4, "petal_width": 0.2, "variety": "Setosa"},
        {"sepal_length": 4.9, "sepal_width": 3.0, "petal_length": 1.4, "petal_width": 0.2, "variety": "Setosa"},
        {"sepal_length": 4.7, "sepal_width": 3.2, "petal_length": 1.3, "petal_width": 0.2, "variety": "Setosa"},
    ]
    return items
