import dataclasses as pydataclasses
from typing import TypeVar

from daft.experimental.types import DaftImageType, DaftType

_T = TypeVar("_T")


@pydataclasses.dataclass
class DaftFieldMetadata:
    daft_type: DaftType


def DaftField(*, daft_type: DaftType, **kwargs) -> _T:
    daft_metadata_key_name = DaftFieldMetadata.__name__
    metadata = {daft_metadata_key_name: DaftFieldMetadata(daft_type=daft_type)}
    if "metadata" in kwargs:
        assert daft_metadata_key_name not in kwargs["metadata"], f"{daft_metadata_key_name} already defined in field"
        metadata.update(kwargs["metadata"])
    kwargs["metadata"] = metadata

    field: _T = pydataclasses.field(**kwargs)
    return field


def DaftImageField(*, encoding=DaftImageType.Encoding.JPEG, **kwargs) -> _T:
    return DaftField(daft_type=DaftImageType(encoding), **kwargs)
