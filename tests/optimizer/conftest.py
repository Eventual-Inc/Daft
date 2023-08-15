from __future__ import annotations

import json
import pathlib
from typing import Any

import pytest

from daft.context import get_context
from daft.logical.logical_plan import LogicalPlan

collect_ignore_glob = []
if get_context().use_rust_planner:
    collect_ignore_glob.append("*.py")


@pytest.fixture(scope="function")
def valid_data() -> list[dict[str, Any]]:
    items = [
        {"sepal_length": 5.1, "sepal_width": 3.5, "petal_length": 1.4, "petal_width": 0.2, "variety": "Setosa"},
        {"sepal_length": 4.9, "sepal_width": 3.0, "petal_length": 1.4, "petal_width": 0.2, "variety": "Setosa"},
        {"sepal_length": 4.7, "sepal_width": 3.2, "petal_length": 1.3, "petal_width": 0.2, "variety": "Setosa"},
    ]
    return items


@pytest.fixture(scope="function")
def valid_data_json_path(valid_data, tmpdir) -> str:
    json_path = pathlib.Path(tmpdir) / "mydata.csv"
    with open(json_path, "w") as f:
        for data in valid_data:
            f.write(json.dumps(data))
            f.write("\n")
    return str(json_path)


def assert_plan_eq(received: LogicalPlan, expected: LogicalPlan):
    assert received.is_eq(
        expected
    ), f"Expected:\n{expected.pretty_print()}\n\n--------\n\nReceived:\n{received.pretty_print()}"
