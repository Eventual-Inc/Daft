import io
from typing import Dict

import numpy as np
import PIL.Image
import pyarrow as pa
import pytest

from daft.dataclasses import dataclass
from daft.fields import DaftImageField
from daft.schema import DaftSchema
from daft.types import DaftImageType


def test_simple_schema() -> None:
    @dataclass
    class SimpleClass:
        item: int

    assert hasattr(SimpleClass, "_daft_schema")
    daft_schema: DaftSchema = getattr(SimpleClass, "_daft_schema")
    arrow_schema = daft_schema.arrow_schema()
    assert isinstance(arrow_schema, pa.Schema)
    assert len(arrow_schema.names) == 1
    item_field = arrow_schema.field(0)
    assert item_field.name == "item"
    assert pa.types.is_integer(item_field.type)

    to_serialize = [SimpleClass(i) for i in range(5)]
    table = daft_schema.serialize(to_serialize)
    assert table.schema == arrow_schema
    data = table.to_pylist()
    values = [d["item"] for d in data]
    assert values == list(range(5))

    back_to_py = daft_schema.deserialize_batch(table, SimpleClass)
    assert to_serialize == back_to_py


def test_conversion_schema() -> None:
    @dataclass
    class SimpleClass:
        item: np.ndarray

    daft_schema: DaftSchema = getattr(SimpleClass, "_daft_schema")
    arrow_schema = daft_schema.arrow_schema()
    assert isinstance(arrow_schema, pa.Schema)
    assert len(arrow_schema.names) == 1
    item_field = arrow_schema.field(0)
    assert item_field.name == "item"
    assert pa.types.is_binary(item_field.type)

    to_serialize = [SimpleClass(np.full(i + 1, i)) for i in range(5)]
    table = daft_schema.serialize(to_serialize)
    assert table.schema == arrow_schema
    data = table.to_pylist()
    for i, d in enumerate(data):
        v = d["item"]
        assert type(v) == bytes
        with io.BytesIO(v) as f:
            recreated_np = np.load(f)
            assert np.all(recreated_np == np.full(i + 1, i))

    back_to_py = daft_schema.deserialize_batch(table, SimpleClass)
    assert all([np.all(s.item == t.item) for s, t in zip(to_serialize, back_to_py)])


@pytest.mark.skip(reason="not currently supporting nested")
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
    table = daft_schema.serialize(source_data)

    back_to_py = daft_schema.deserialize_batch(table, TestDC)
    assert source_data == back_to_py


def test_schema_daft_field_numpy() -> None:
    @dataclass
    class SimpleDaftField:
        img: np.ndarray = DaftImageField(encoding=DaftImageType.Encoding.JPEG)

    daft_schema: DaftSchema = getattr(SimpleDaftField, "_daft_schema")
    arrow_schema = daft_schema.arrow_schema()
    assert isinstance(arrow_schema, pa.Schema)
    assert len(arrow_schema.names) == 1
    img_field = arrow_schema.field(0)
    assert img_field.name == "img"
    assert pa.types.is_binary(img_field.type)
    to_serialize = [SimpleDaftField(np.ones(i + 1, dtype=np.uint8)) for i in range(5)]
    table = daft_schema.serialize(to_serialize)
    data = table.to_pylist()

    def is_jpeg(buffer):
        # Magic Headers for JPEG
        # https://en.wikipedia.org/wiki/JPEG_File_Interchange_Format#File_format_structure
        assert buffer[:4] == b"\xff\xd8\xff\xe0"
        assert buffer[6:11] == b"JFIF\0"
        return True

    for d in data:
        assert "img" in d
        buffer = d["img"]
        assert is_jpeg(buffer)

    back_to_py = daft_schema.deserialize_batch(table, SimpleDaftField)

    for old, new in zip(to_serialize, back_to_py):
        assert isinstance(new, SimpleDaftField)
        assert isinstance(new.img, np.ndarray)
        assert np.all(old.img == new.img)


def test_schema_daft_field_PIL() -> None:
    @dataclass
    class SimpleDaftField:
        img: PIL.Image.Image = DaftImageField(encoding=DaftImageType.Encoding.JPEG)

    daft_schema: DaftSchema = getattr(SimpleDaftField, "_daft_schema")
    arrow_schema = daft_schema.arrow_schema()
    assert isinstance(arrow_schema, pa.Schema)
    assert len(arrow_schema.names) == 1
    img_field = arrow_schema.field(0)
    assert img_field.name == "img"
    assert pa.types.is_binary(img_field.type)
    to_serialize = [SimpleDaftField(PIL.Image.new("RGB", (i, i))) for i in range(1, 6)]
    table = daft_schema.serialize(to_serialize)
    data = table.to_pylist()

    def is_jpeg(buffer):
        # Magic Headers for JPEG
        # https://en.wikipedia.org/wiki/JPEG_File_Interchange_Format#File_format_structure
        assert buffer[:4] == b"\xff\xd8\xff\xe0"
        assert buffer[6:11] == b"JFIF\0"
        return True

    for d in data:
        assert "img" in d
        buffer = d["img"]
        assert is_jpeg(buffer)

    back_to_py = daft_schema.deserialize_batch(table, SimpleDaftField)
    for old, new in zip(to_serialize, back_to_py):
        assert isinstance(new, SimpleDaftField)
        assert isinstance(new.img, PIL.Image.Image)
        assert np.all(np.array(old.img) == np.array(new.img))
