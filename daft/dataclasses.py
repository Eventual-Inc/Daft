from __future__ import annotations

import dataclasses as pydataclasses
from typing import TYPE_CHECKING, Callable, Optional, OrderedDict, Type, TypeVar, Union

from daft.schema import DaftSchema
from daft.utils import _patch_class_for_deserialization

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
    if not pydataclasses.is_dataclass(cls):
        cls = pydataclasses.dataclass(cls)
    daft_schema = DaftSchema(cls)
    setattr(cls, "_daft_schema", daft_schema)
    return cls


def is_daft_dataclass(cls: Type[_T]) -> bool:
    return pydataclasses.is_dataclass(cls) and hasattr(cls, "_daft_schema")


class DataclassBuilder:
    def __init__(self) -> None:
        self.fields = OrderedDict()

    def add_field(self, name: str, dtype: Type, field: Optional[pydataclasses.Field] = None) -> None:
        if name in self.fields:
            raise ValueError(f"{name} already in builder")
        assert isinstance(dtype, Type)
        if field is None:
            self.fields[name] = (name, dtype)
        else:
            self.fields[name] = (name, dtype, field)

    def remove_field(self, name: str) -> Optional[str]:
        if name in self.fields:
            del self.fields[name]
            return name
        return None

    def generate(self, cls_name: Optional[str] = None) -> Type:
        if cls_name is None:
            cls_name = "GenDataclass" + "".join(f"_{f}" for f in self.fields.keys())
        return dataclass(
            pydataclasses.make_dataclass(
                cls_name=cls_name,
                fields=self.fields.values(),
            )
        )

    @classmethod
    def from_class(cls, dtype: Type) -> DataclassBuilder:
        db = DataclassBuilder()
        assert pydataclasses.is_dataclass(dtype)
        for field in getattr(dtype, "__dataclass_fields__").values():
            db.add_field(field.name, field.type, field)
        return db

    @classmethod
    def from_schema(cls, schema: DaftSchema) -> DataclassBuilder:
        raise NotImplementedError("from schema is not yet implemented")
