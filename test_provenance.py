#!/usr/bin/env python3
from __future__ import annotations

import daft
from daft.logging import setup_logger

setup_logger("debug")


@daft.cls(on_error="log")
class BonusCalculator:
    def __init__(self, factor: int):
        self.factor = factor

    def calculate(self, score: int, age: int) -> int:
        raise RuntimeError(f"cls boom: {score}, {age}, factor={self.factor}")


data = [
    {"id": 1, "name": "Alice", "age": 25, "score": 85},
    {"id": 2, "name": "Bob", "age": 30, "score": 90},
    {"id": 3, "name": "Charlie", "age": 35, "score": 95},
]

df = daft.from_pylist(data, provenance_fn=lambda row: f"source1_row_{row['id']}")

calc = BonusCalculator(factor=10)

df2 = df.with_column("bonus", calc.calculate(df["score"], df["age"]))

results = df2.to_pydict()
