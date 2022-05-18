import dataclasses as pydataclasses
from builtins import type
from typing import TYPE_CHECKING, Any, Callable, Type, TypeVar, Union, overload

from pyparsing import Optional
from pyrsistent import b

from daft.schema import DaftSchema

_T = TypeVar("_T")

if TYPE_CHECKING:
    from dataclasses import dataclass
else:

    def dataclass(
        _cls: Type = None,
        *,
        init: bool = True,
        repr: bool = True,
        eq: bool = True,
        order: bool = False,
        unsafe_hash: bool = False,
        frozen: bool = False,
        match_args: bool = True,
        kw_only: bool = False,
        slots: bool = False,
    ) -> Union[Type[_T], Callable[[Type[_T]], Type[_T]]]:
        def wrap(cls: Type) -> Type:
            return __process_class(
                cls,
                init=init,
                repr=repr,
                eq=eq,
                order=order,
                unsafe_hash=unsafe_hash,
                frozen=frozen,
                match_args=match_args,
                kw_only=kw_only,
                slots=slots,
            )

        if _cls is None:
            return wrap
        else:
            return wrap(_cls)


def __process_class(cls: Type[_T], **kwargs) -> Type[_T]:
    cls = pydataclasses.dataclass(cls)
    daft_schema = DaftSchema(cls)
    setattr(cls, "_daft_schema", daft_schema)
    return cls
