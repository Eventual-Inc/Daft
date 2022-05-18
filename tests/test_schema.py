import dataclasses as pydataclasses
from time import time
from typing import Dict, List, Optional, Tuple

import numpy as np
import pyarrow as pa

from daft.dataclasses import dataclass
from daft.schema import DaftSchema


def test_simple_schema() -> None:
    @dataclass
    class SimpleClass:
        item: int

    assert hasattr(SimpleClass, "_daft_schema")
    daft_schema: DaftSchema = getattr(SimpleClass, "_daft_schema")
    arrow_schema = daft_schema.arrow_schema()
    assert isinstance(arrow_schema, pa.Schema)
    assert len(arrow_schema.names) == 1
    root = arrow_schema.field(0).type
    assert pa.types.is_struct(root)
    assert root.num_fields == 1
    item_field = root[0]
    assert item_field.name == "item"
    assert pa.types.is_integer(item_field.type)

    to_serialize = [SimpleClass(i) for i in range(5)]
    record_batch = daft_schema.serialize(to_serialize)
    assert record_batch.schema == arrow_schema
    data = record_batch[0].field(0).to_pylist()
    assert data == list(range(5))


def test_schema_nested() -> None:
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

    source_data = [TestDC(i, 2.0, Nested(1, "oh wow"), {f"{i}": i}) for i in range(10)]
    daft_schema: DaftSchema = getattr(TestDC, "_daft_schema")
    record_batch = daft_schema.serialize(source_data)

    back_to_py = daft_schema.deserialize_batch(record_batch)

    for s, t in zip(source_data, back_to_py):
        flattened = pydataclasses.asdict(s)
        assert flattened == t
