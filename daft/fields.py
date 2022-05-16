import dataclasses as pydataclasses
from typing import Any, Type, TypeVar

from daft.types import DaftImageType, DaftType

_T = TypeVar("_T")


@pydataclasses.dataclass
class DaftFieldMetadata:
    daft_metatype: DaftType


def DaftField(*, daft_metatype: DaftType, **kwargs) -> _T:
    metadata = {
        'DaftFieldMetadata': DaftFieldMetadata(daft_metatype=daft_metatype)
    }
    if "metadata" in kwargs:
        assert 'DaftFieldMetadata' not in kwargs["metadata"], 'DaftFieldMetadata already defined in field'
        metadata.update(kwargs["metadata"])
    kwargs["metadata"] = metadata

    field: _T = pydataclasses.field(**kwargs)
    return field

def DaftImageField(*, encoding=DaftImageType.Encoding.JPEG, **kwargs) -> _T:
    return DaftField(daft_metatype=DaftImageType(encoding), **kwargs)