from __future__ import annotations

import io
import json
from abc import abstractmethod
from enum import Enum
from typing import Any, Dict

import numpy as np
import PIL.Image
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
    def args(self) -> Dict[str, str]:
        raise NotImplementedError()

    def serialize_type_info(self) -> str:
        output: dict[str, Any] = {"name": self.name(), "args": self.args()}
        return json.dumps(output)

    @abstractmethod
    def serialize(self, obj) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def deserialize(self, b: bytes, source_type: str):
        raise NotImplementedError()


class DaftImageType(DaftType):
    class Encoding(Enum):
        JPEG = "jpeg"

    def __init__(self, encoding: DaftImageType.Encoding) -> None:
        self.encoding = encoding

    def arrow_type(self) -> pa.DataType:
        return pa.binary()

    def name(self) -> str:
        return "DaftImageType"

    def args(self) -> Dict[str, str]:
        return {"encoding": self.encoding.value}

    def serialize(self, obj) -> bytes:
        if isinstance(obj, np.ndarray):
            assert obj.dtype == np.uint8, "image np.ndarray must be 8 bit"
            obj = PIL.Image.fromarray(obj)
        else:
            assert isinstance(obj, PIL.Image.Image), f"unsupported image found {type(obj)}"

        with io.BytesIO() as f:
            obj.save(f, format=self.encoding)
            return f.getvalue()

    def deserialize(self, b: bytes, source_type: str):
        with io.BytesIO(b) as f:
            img = PIL.Image.open(f)
            img.load()

        if source_type == np.ndarray.__qualname__:
            return np.array(img)
        elif source_type == PIL.Image.Image.__qualname__:
            return img
        else:
            raise NotImplementedError(f"cant find deserializer for {source_type}")
