import copy
import dataclasses as pydataclasses
from typing import TYPE_CHECKING, Callable, Type, TypeVar, Union

_T = TypeVar("_T")

__all__ = [
    "dataclass",
    "field",
    "Field",
    "FrozenInstanceError",
    "InitVar",
    "MISSING",
    # Helper functions.
    "fields",
    "asdict",
    "astuple",
    "make_dataclass",
    "replace",
    "is_dataclass",
]

_FIELDS = "__dataclass_fields__"


class _FIELD_BASE:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name


_FIELD = _FIELD_BASE("_FIELD")

from dataclasses import (
    Field,
    FrozenInstanceError,
    InitVar,
    field,
    is_dataclass,
    make_dataclass,
    replace,
)

if TYPE_CHECKING:
    from dataclasses import asdict, astuple, dataclass, fields
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

    def asdict(obj, *, dict_factory=dict):
        if not _is_dataclass_instance(obj):
            raise TypeError("asdict() should be called on dataclass instances")
        return _asdict_inner(obj, dict_factory)

    def astuple(obj, *, tuple_factory=tuple):
        if not _is_dataclass_instance(obj):
            raise TypeError("astuple() should be called on dataclass instances")
        return _astuple_inner(obj, tuple_factory)

    def fields(class_or_instance):

        # Might it be worth caching this, per class?
        try:
            fields = getattr(class_or_instance, _FIELDS)
        except AttributeError:
            raise TypeError("must be called with a dataclass type or instance")

        # Exclude pseudo-fields.  Note that fields is sorted by insertion
        # order, so the order of the tuple is as the fields were defined.
        return tuple(f for f in fields.values() if f._field_type.name == _FIELD.name)


def __process_class(cls: Type[_T], **kwargs) -> Type[_T]:
    cls = pydataclasses.dataclass(cls)
    from daft.schema import DaftSchema

    daft_schema = DaftSchema(cls)
    setattr(cls, "_daft_schema", daft_schema)
    return cls


def _asdict_inner(obj, dict_factory):
    if _is_dataclass_instance(obj):
        result = []
        for f in fields(obj):
            value = _asdict_inner(getattr(obj, f.name), dict_factory)
            result.append((f.name, value))
        return dict_factory(result)
    elif isinstance(obj, tuple) and hasattr(obj, "_fields"):
        # obj is a namedtuple.  Recurse into it, but the returned
        # object is another namedtuple of the same type.  This is
        # similar to how other list- or tuple-derived classes are
        # treated (see below), but we just need to create them
        # differently because a namedtuple's __init__ needs to be
        # called differently (see bpo-34363).

        # I'm not using namedtuple's _asdict()
        # method, because:
        # - it does not recurse in to the namedtuple fields and
        #   convert them to dicts (using dict_factory).
        # - I don't actually want to return a dict here.  The main
        #   use case here is json.dumps, and it handles converting
        #   namedtuples to lists.  Admittedly we're losing some
        #   information here when we produce a json list instead of a
        #   dict.  Note that if we returned dicts here instead of
        #   namedtuples, we could no longer call asdict() on a data
        #   structure where a namedtuple was used as a dict key.

        return type(obj)(*[_asdict_inner(v, dict_factory) for v in obj])
    elif isinstance(obj, (list, tuple)):
        # Assume we can create an object of this type by passing in a
        # generator (which is not true for namedtuples, handled
        # above).
        return type(obj)(_asdict_inner(v, dict_factory) for v in obj)
    elif isinstance(obj, dict):
        return type(obj)((_asdict_inner(k, dict_factory), _asdict_inner(v, dict_factory)) for k, v in obj.items())
    else:
        return copy.deepcopy(obj)


def _astuple_inner(obj, tuple_factory):
    if _is_dataclass_instance(obj):
        result = []
        for f in fields(obj):
            value = _astuple_inner(getattr(obj, f.name), tuple_factory)
            result.append(value)
        return tuple_factory(result)
    elif isinstance(obj, tuple) and hasattr(obj, "_fields"):
        # obj is a namedtuple.  Recurse into it, but the returned
        # object is another namedtuple of the same type.  This is
        # similar to how other list- or tuple-derived classes are
        # treated (see below), but we just need to create them
        # differently because a namedtuple's __init__ needs to be
        # called differently (see bpo-34363).
        return type(obj)(*[_astuple_inner(v, tuple_factory) for v in obj])
    elif isinstance(obj, (list, tuple)):
        # Assume we can create an object of this type by passing in a
        # generator (which is not true for namedtuples, handled
        # above).
        return type(obj)(_astuple_inner(v, tuple_factory) for v in obj)
    elif isinstance(obj, dict):
        return type(obj)((_astuple_inner(k, tuple_factory), _astuple_inner(v, tuple_factory)) for k, v in obj.items())
    else:
        return copy.deepcopy(obj)


def _is_dataclass_instance(obj):
    """Returns True if obj is an instance of a dataclass."""
    return hasattr(type(obj), _FIELDS)
