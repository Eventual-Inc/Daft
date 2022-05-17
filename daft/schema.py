from __future__ import annotations
from importlib.metadata import metadata

from typing import Any, Dict, NamedTuple, Optional, Type, Callable, get_origin, get_args
import dataclasses as pydataclasses

from daft.fields import DaftFieldMetadata

import pyarrow as pa
import numpy as np
import PIL
import PIL.Image

class TypeBuilderTuple(NamedTuple) :
    arg_count: int
    func: Callable[[], pa.Datatype]
    source_pytype: Type

TBT = TypeBuilderTuple
class DaftSchema:

    def __init__(self, root: pa.DataType) -> None:
        self.schema = pa.schema(root)
        print(self.schema)

    primative_translation = {
        int: TBT(0, pa.int64, int),
        float: TBT(0, pa.float32, float),
        str: TBT(0, pa.string, str),
        bool: TBT(0, pa.bool_, bool),
        bytes: TBT(0, pa.binary, bytes),
        np.ndarray: TBT(0, pa.binary, np.ndarray),
        PIL.Image.Image: TBT(0, pa.binary, PIL.Image.Image)
    }

    origin_translation = {
        list: (1, pa.list_, list),
        tuple: (1, pa.list_, tuple),
        dict: (2, pa.map_, dict),
    }

    @classmethod
    def parse_dataclass(cls, t: Type):
        assert pydataclasses.is_dataclass(t) and isinstance(t, type)
        schema_so_far = []
        for field in pydataclasses.fields(t):
            name = field.name
            pytype = field.type
            daft_field_metadata: Optional[DaftFieldMetadata] = field.metadata.get(DaftFieldMetadata.__name__, None)
            if daft_field_metadata is not None:
                arrow_type = daft_field_metadata.daft_type.arrow_type()
                type_info = daft_field_metadata.daft_type.serialize_type_info()

                metadata = cls.type_to_metadata(pytype)
                metadata['daft_type_info'] = type_info.encode()
                arrow_field = pa.field(name, arrow_type, metadata=metadata)
            else:
                arrow_field = cls.parse_type(name, pytype)
                assert isinstance(arrow_field, pa.Field)

            schema_so_far.append(arrow_field)
            
        return pa.struct(schema_so_far)

            # daft_field_metadata: Optional[DaftFieldMetadata] = None
            # daft_metadata_key_name = DaftFieldMetadata.__name__
            
            # if daft_metadata_key_name in field.metadata:
            #     daft_field_metadata = field.metadata[daft_metadata_key_name]


    @classmethod
    def type_to_metadata(cls, t: Type) -> Dict[str, bytes]:
        if hasattr(t, "__qualname__"):
            return {"source_type": t.__qualname__.encode()}
        elif hasattr(t, "_name"):
            return {"source_type": t._name.encode()}
        raise NotImplementedError(f"{t} doesn't have __qualname__ or _name")

    @classmethod
    def parse_type(cls, name:str, t: Type) -> pa.Field:
        if t in cls.primative_translation:
            nargs, f, source_type = cls.primative_translation[t]
            return pa.field(name, f()).with_metadata(cls.type_to_metadata(source_type))
        
        if get_origin(t) is not None:
            origin = get_origin(t)
            if origin in cls.origin_translation:
                arg_count, f, source_type = cls.origin_translation[origin]
                raw_args = get_args(t)
                assert arg_count == len(raw_args)
                args = [cls.parse_type(f'{name}/{i}', arg) for i, arg in enumerate(raw_args)]
                if f == pa.map_:
                    args[0] = args[0].with_nullable(False)
                return pa.field(name, f(*args)).with_metadata(cls.type_to_metadata(t))
        
        if pydataclasses.is_dataclass(t):
            assert isinstance(t, type), "unable to parse instances of dataclasses"
            return pa.field(name, cls.parse_dataclass(t)).with_metadata(cls.type_to_metadata(t))
        raise NotImplementedError(f"Could not parse {t}")

    @classmethod
    def create(cls, t: Type) -> DaftSchema:
        assert pydataclasses.is_dataclass(t) and isinstance(t, type)
        return DaftSchema([cls.parse_type('root', t)])     
        