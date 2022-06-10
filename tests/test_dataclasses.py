from typing import Dict, List

import numpy as np
import PIL
import PIL.Image
from dataclasses import fields, is_dataclass
from daft.dataclasses import DataclassBuilder, dataclass
from daft.fields import DaftImageField
from daft.types import DaftImageType


def test_dataclass_without_args() -> None:
    @dataclass
    class TestDataclass:
        x: int

    assert is_dataclass(TestDataclass) and isinstance(TestDataclass, type)

    obj = TestDataclass(1)

    assert is_dataclass(obj) and not isinstance(obj, type)


def test_dataclass_with_args() -> None:
    @dataclass(frozen=True)
    class TestDataclass:
        x: int

    assert is_dataclass(TestDataclass) and isinstance(TestDataclass, type)

    obj = TestDataclass(1)

    assert is_dataclass(obj) and not isinstance(obj, type)


def test_dataclass_with_nested_structures() -> None:
    from daft.fields import DaftImageField
    from daft.types import DaftImageType

    @dataclass
    class SomeSubClass:
        thing: int
        thing2: float

    @dataclass
    class DataclassWithDaftImage:
        x: int
        y: int
        list_x: List[int]
        dict_x: Dict[str, int]
        array: np.ndarray
        subclass: SomeSubClass
        list_subclass: List[SomeSubClass]

    assert is_dataclass(DataclassWithDaftImage) and isinstance(DataclassWithDaftImage, type)


def test_dataclass_with_daft_type() -> None:
    @dataclass
    class ImageDataClass:
        id: int
        labels: np.ndarray
        image: np.ndarray = DaftImageField(encoding=DaftImageType.Encoding.JPEG)


def test_dataclass_builder_simple() -> None:
    db = DataclassBuilder()
    db.add_field("x", int)
    db.add_field("y", str)
    dc = db.generate()
    values = [(f.name, f.type) for f in fields(dc)]
    assert values == [("x", int), ("y", str)]


def test_dataclass_builder_with_daft_fields() -> None:
    db = DataclassBuilder()
    db.add_field("x", int)
    db.add_field("y", str)
    db.add_field("img", np.ndarray, DaftImageField())
    dc = db.generate()
    values = [(f.name, f.type) for f in fields(dc)]
    assert values == [("x", int), ("y", str), ("img", np.ndarray)]
    img_field = fields(dc)[2]
    assert img_field.name == "img"
    assert img_field.type == np.ndarray
    assert "DaftFieldMetadata" in img_field.metadata


def test_dataclass_builder_from_other_dataclass() -> None:
    db = DataclassBuilder()
    db.add_field("x", int)
    db.add_field("y", str)
    db.add_field("img", np.ndarray, DaftImageField())
    dc = db.generate()
    values = [(f.name, f.type) for f in fields(dc)]
    assert values == [("x", int), ("y", str), ("img", np.ndarray)]

    db2 = DataclassBuilder.from_class(dc)
    db2.add_field("new_value", str)
    dc2 = db2.generate()
    values = [(f.name, f.type) for f in fields(dc2)]

    assert values == [("x", int), ("y", str), ("img", np.ndarray), ("new_value", str)]

    db2.remove_field("new_value")

    dc2 = db2.generate()
    values = [(f.name, f.type) for f in fields(dc2)]

    assert values == [("x", int), ("y", str), ("img", np.ndarray)]


def test_dataclass_builder_with_root_types() -> None:
    builder = DataclassBuilder()
    builder.add_field("foo", List[str])
    dc = builder.generate()
    values = [(f.name, f.type) for f in fields(dc)]

    assert values == [("foo", List[str])]
