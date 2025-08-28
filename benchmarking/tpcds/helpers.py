from __future__ import annotations

from pathlib import Path

import daft
from daft.sql.sql import SQLCatalog


def generate_catalog(dir: Path):
    if not dir.exists():
        raise RuntimeError(f"Directory not found: {dir}")
    return SQLCatalog(
        tables={
            file.stem: daft.read_parquet(path=str(file))
            for file in dir.iterdir()
            if file.is_file() and file.suffix == ".parquet"
        }
    )


def parse_questions_str(questions: str) -> list[int]:
    if questions == "*":
        return list(range(1, 100))

    nums = set()
    for split in filter(lambda str: str, questions.split(",")):
        try:
            num = int(split)
            nums.add(num)
        except ValueError:
            ints = split.split("-")
            assert (
                len(ints) == 2
            ), f"A range must include two numbers split by a dash (i.e., '-'); instead got '{split}'"
            [lower, upper] = ints
            try:
                lower = int(lower)
                upper = int(upper)
                assert lower <= upper
                for index in range(lower, upper + 1):
                    nums.add(index)
            except ValueError:
                raise ValueError(f"Invalid range: {split}")

    return nums
