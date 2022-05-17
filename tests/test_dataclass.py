from typing import Any, Dict, List
from daft.arrow_extensions import DaftArrowImage
from daft.dataclasses import dataclass

from dataclasses import is_dataclass, field

from dataclasses import dataclass as pydataclass

import PIL
import PIL.Image
from PIL.Image import Image # type: ignore

import numpy as np

from daft.fields import DaftImageField

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
    class SomeSubClass:
        image: np.ndarray = DaftImageField()