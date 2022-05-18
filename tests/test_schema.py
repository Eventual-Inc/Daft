from time import time
from typing import Dict, List, Tuple

from typing import Optional
from daft.dataclasses import dataclass

import dataclasses as pydataclasses

import numpy as np

def test_schema() -> None:
    @dataclass
    class Nested:
        z: int
        a: str

    @dataclass
    class TestDC:
        x: int
        y: float
        q: Nested
        nd: Dict[str, int]
    source_data = [TestDC(i, 2.0, Nested(1, 'oh wow'), {f"{i}": 9}) for i in range(10)]
    record_batch = TestDC._daft_schema.serialize(source_data)

    back_to_py = TestDC._daft_schema.deserialize_batch(record_batch)

    for s, t in zip(source_data, back_to_py):
        flattened = pydataclasses.asdict(s)
        assert flattened == t