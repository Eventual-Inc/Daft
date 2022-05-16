import dataclasses as pydataclasses
from typing import Type


def __process_class(cls, **kwargs) -> Type:
    cls = pydataclasses.dataclass(cls)
    print(pydataclasses.fields(cls))
    return cls


def dataclass(cls=None, /, *args, **kwargs) -> Type:

    def wrap(cls):
        return __process_class(cls, **kwargs)

    if cls is None:
        return wrap

    return wrap(cls)
