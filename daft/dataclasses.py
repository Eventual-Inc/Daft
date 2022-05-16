import dataclasses as pydataclasses
from typing import Any, Type, Callable, Union

from pyparsing import Optional


def __process_class(cls: Type, **kwargs) -> Type:
    cls = pydataclasses.dataclass(cls)
    print(pydataclasses.fields(cls))
    return cls


def dataclass(cls=None, /, *args, **kwargs) -> Union[Type, Callable[[Type[Any]], Type[Any]]]:

    def wrap(cls: Type) -> Type:
        return __process_class(cls, **kwargs)

    if cls is None:
        return wrap

    return wrap(cls)
