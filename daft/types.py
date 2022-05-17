from __future__ import annotations

from abc import abstractmethod, ABC
from typing import List, Type, Union


from enum import Enum

import pyarrow as pa

from daft.arrow_extensions import DaftArrowImage

class DaftType:
    pass

    @abstractmethod
    def arrow_type(self) -> pa.DataType:
        raise NotImplementedError()

class DaftImageType(DaftType):
    class Encoding(Enum):
        JPEG='jpeg'

    def __init__(self, encoding: DaftImageType.Encoding) -> None:
        self.encoding = encoding

    def arrow_type(self) -> pa.DataType:
        return DaftArrowImage(str(self.encoding))

    # def serialize(obj) -> bytes:
    #     pass

    # def deserialize(b: bytes):
    #     pass