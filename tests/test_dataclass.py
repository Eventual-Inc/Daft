from daft.dataclasses import dataclass

from dataclasses import is_dataclass, field

from dataclasses import dataclass as pydataclass



import numpy as np

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

def test_dataclass_with_daft_type() -> None:
    from daft.fields import DaftImageField
    from daft.types import DaftImageType

    @dataclass
    class DataclassWithDaftImage:
        x: int
        img: np.ndarray = DaftImageField(encoding=DaftImageType.Encoding.JPEG)

    assert is_dataclass(DataclassWithDaftImage) and isinstance(DataclassWithDaftImage, type)

    obj = DataclassWithDaftImage(1, np.ones((8,8)))

    assert is_dataclass(obj) and not isinstance(obj, type)
