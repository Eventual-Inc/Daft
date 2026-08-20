from __future__ import annotations

import daft


@daft.func
def my_add_func(a: int, b: int) -> int:
    return a + b


@daft.udf(return_dtype=daft.DataType.int64())
def catalog_udf(x):
    return x


@daft.udf(return_dtype=daft.DataType.int64())
def double_value(x):
    return [v * 2 for v in x.to_pylist()]


@daft.cls
class MockModelPredictor:
    def __init__(self, model_path: str):
        # This expensive initialization happens once per worker
        self.model = model_path

    def __call__(self, text: str) -> str:
        return f"model {self.model} predict {text}"
