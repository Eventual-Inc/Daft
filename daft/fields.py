import dataclasses as pydataclasses
from typing import Any, Type

from daft.types import DaftImageType, DaftType

@pydataclasses.dataclass
class DaftFieldMetadata:
    daft_metatype: DaftType


def DaftField(*, daft_metatype: DaftType, **kwargs) -> pydataclasses.Field:
    metadata = {
        'DaftFieldMetadata': DaftFieldMetadata(daft_metatype=daft_metatype)
    }
    if "metadata" in kwargs:
        assert 'DaftFieldMetadata' not in kwargs["metadata"], 'DaftFieldMetadata already defined in field'
        metadata.update(kwargs["metadata"])
    kwargs["metadata"] = metadata

    return pydataclasses.field(**kwargs)

def DaftImageField(*, encoding=DaftImageType.Encoding.JPEG, **kwargs) -> pydataclasses.Field:
    return DaftField(daft_metatype=DaftImageType(encoding), **kwargs)
