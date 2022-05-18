from __future__ import annotations

import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Type, Union

import pyarrow as pa


class DaftType:
    pass

    @abstractmethod
    def arrow_type(self) -> pa.DataType:
        raise NotImplementedError()

    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def metadata(self) -> Dict[str, str]:
        raise NotImplementedError()

    def serialize_type_info(self) -> str:
        output: dict[str, Any] = {"name": self.name(), "metadata": self.metadata()}
        return json.dumps(output)


class DaftImageType(DaftType):
    class Encoding(Enum):
        JPEG = "jpeg"

    def __init__(self, encoding: DaftImageType.Encoding) -> None:
        self.encoding = encoding

    def arrow_type(self) -> pa.DataType:
        return pa.binary()

    def name(self) -> str:
        return "DaftImageType"

    def metadata(self) -> Dict[str, str]:
        return {"encoding": self.encoding.value}

    # def serialize(obj) -> bytes:
    #     pass

    # def deserialize(b: bytes):
    #     pass
