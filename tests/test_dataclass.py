from re import X
from daft.dataclasses import dataclass

from dataclasses import is_dataclass

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
