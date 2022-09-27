from __future__ import annotations

import copy
import dataclasses as pydataclasses
import io
import json
import sys
from collections import defaultdict
from inspect import getmembers
from typing import Callable, Dict, Generic, List, NamedTuple, Optional, Type, TypeVar

if sys.version_info < (3, 8):
    from typing_extensions import get_args, get_origin
else:
    from typing import get_origin, get_args

import numpy as np
import PIL
import PIL.Image
import pyarrow as pa

from daft.experimental import types
from daft.experimental.fields import DaftFieldMetadata
from daft.experimental.types import DaftType

_DaftTypeLookup = dict(getmembers(types))


def numpy_encoder(x: np.ndarray) -> bytes:
    with io.BytesIO() as f:
        np.save(f, x)
        return f.getvalue()


def numpy_decoder(b: bytes) -> np.ndarray:
    with io.BytesIO(b) as f:
        arr: np.ndarray = np.load(f)
        return arr


class PyConverter:
    class SerDes(NamedTuple):
        to_arrow: Callable
        from_arrow: Callable

    binary_converters = {np.ndarray.__qualname__: SerDes(numpy_encoder, numpy_decoder)}

    def __init__(self, to_arrow=True):
        self.to_arrow = to_arrow

    def _handle_map(self, field, obj):
        if self.to_arrow and isinstance(obj, dict):
            return list(obj.items())
        else:
            return obj

    def _handle_binary(self, field, obj):
        source_type = field.metadata[b"source_type"].decode()
        assert source_type in self.binary_converters
        ser_des = self.binary_converters[source_type]
        if self.to_arrow:
            return ser_des.to_arrow(obj)
        else:
            return ser_des.from_arrow(obj)

    def convert(self, field, obj):
        assert isinstance(field, pa.Field)
        daft_type = self._daft_type(field)
        if daft_type is not None:
            daft_type = self._daft_type(field)
            if self.to_arrow:
                return daft_type.serialize(obj)
            else:
                source_type = field.metadata[b"source_type"].decode()
                return daft_type.deserialize(obj, source_type)
        elif pa.types.is_map(field.type):
            return self._handle_map(field, obj)
        elif pa.types.is_binary(field.type):
            return self._handle_binary(field, obj)

        raise NotImplementedError(f"could not find conversion for {type(obj)} to {field}")

    def _daft_type(self, field) -> Optional[DaftType]:
        if b"daft_type_info" in field.metadata:
            json_string = field.metadata[b"daft_type_info"].decode()
            daft_type_data = json.loads(json_string)
            daft_type_name = daft_type_data["name"]
            daft_type_args = daft_type_data["args"]
            assert daft_type_name in _DaftTypeLookup, f"{daft_type_name} not in {_DaftTypeLookup.keys()}"
            cls: Type[DaftType] = _DaftTypeLookup[daft_type_name]
            return cls(**daft_type_args)

        return None


class SchemaParser:
    def __init__(self, to_arrow=True):
        self.to_arrow = to_arrow
        self.py_converter = PyConverter(to_arrow)

    def parse_schema(self, schema, pydict):
        assert isinstance(schema, pa.Schema)
        n_fields = len(schema.names)

        for i in range(n_fields):
            field = schema.field(i)
            name = field.name
            pydict[name] = self.parse_field(field, pydict[name]) if name in pydict else None

        return pydict

    def parse_field(self, field, pydict):
        assert isinstance(field, pa.Field)

        if pa.types.is_nested(field.type):
            if pa.types.is_struct(field.type):
                struct = field.type
                for i in range(struct.num_fields):
                    subfield = struct[i]
                    subname = subfield.name
                    assert subname in pydict, f"{subname} not in {pydict.keys()}"
                    pydict[subname] = self.parse_field(subfield, pydict[subname])
            elif pa.types.is_map(field.type):
                maptype = field.type
                key_field = maptype.key_field

                if self.to_arrow:
                    assert isinstance(pydict, dict), f"{pydict}"
                    keys = list(pydict.keys())
                    items = list(pydict.values())
                else:
                    assert isinstance(pydict, list)
                    keys, items = zip(*pydict)
                new_keys = [self.parse_field(key_field, key) for key in keys]

                item_field = maptype.item_field
                new_items = [self.parse_field(item_field, item) for item in items]
                zipped = list(zip(new_keys, new_items))
                pydict = dict(zipped)
            else:
                raise NotImplementedError(str(field))

        convert = self._require_conversion(field)
        if convert:
            return self.py_converter.convert(field, pydict)
        return pydict

    def _require_conversion(self, field) -> bool:
        assert isinstance(field, pa.Field)
        metadata = field.metadata
        assert b"requires_conversion" in metadata
        val = metadata[b"requires_conversion"]
        return bool(int(val))


class TypeBuilderTuple(NamedTuple):
    arg_count: int
    func: Callable[[], pa.Datatype]
    source_pytype: Type
    requires_conversion: bool = False


TBT = TypeBuilderTuple

_T = TypeVar("_T")


class DaftSchema(Generic[_T]):
    primative_translation = {
        int: TBT(0, pa.int64, int),
        float: TBT(0, pa.float32, float),
        str: TBT(0, pa.string, str),
        bool: TBT(0, pa.bool_, bool),
        bytes: TBT(0, pa.binary, bytes),
        np.ndarray: TBT(0, pa.binary, np.ndarray, True),
        PIL.Image.Image: TBT(0, pa.binary, PIL.Image.Image, True),
    }

    origin_translation = {
        list: TBT(1, pa.list_, list),
        tuple: TBT(1, pa.list_, tuple),
        dict: TBT(2, pa.map_, dict, True),
    }

    def __init__(self, pytype: Type[_T]) -> None:
        assert pydataclasses.is_dataclass(pytype) and isinstance(pytype, type)
        root = DaftSchema.parse_type("root", pytype)
        self.schema = pa.schema(root.flatten())
        self.pytype = pytype

    def arrow_schema(self) -> pa.Schema:
        return self.schema

    def serialize(self, objs: List[_T]) -> pa.Table:
        sp = SchemaParser(to_arrow=True)
        values = defaultdict(list)
        # if len(objs) != 0:
        #     _patch_class_for_deserialization(objs[0])
        for o in objs:
            assert pydataclasses.is_dataclass(o)
            obj_dict = {f"root.{key}": val for key, val in copy.deepcopy(o.__dict__).items()}
            obj_dict = sp.parse_schema(self.schema, obj_dict)
            # obj_dict = self.resolve_conversions(self.schema, obj_dict)
            for k, v in obj_dict.items():
                values[k].append(v)
        return pa.Table.from_pydict(values, schema=self.schema)

    def deserialize_batch(self, batch: pa.Table, target_type: Type[_T]) -> List[_T]:
        assert pydataclasses.is_dataclass(target_type) and isinstance(target_type, type)

        # _patch_class_for_deserialization(target_type)

        sp = SchemaParser(to_arrow=False)

        objs = batch.to_pydict()
        size = len(batch)
        values = []
        for i in range(size):
            o = {key: val[i] for key, val in objs.items()}
            post_obj = sp.parse_schema(self.schema, o)
            post_obj = {key[len(f"root.") :]: val for key, val in post_obj.items() if key.startswith("root.")}
            # py_obj = dacite.from_dict(data_class=target_type, data=post_obj)
            py_obj = target_type(**post_obj)
            values.append(py_obj)
        return values

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

                metadata = cls.type_to_metadata(pytype, True)
                metadata["daft_type_info"] = type_info.encode()
                arrow_field = pa.field(name, arrow_type, metadata=metadata)
            else:
                arrow_field = cls.parse_type(name, pytype)
                assert isinstance(arrow_field, pa.Field)

            schema_so_far.append(arrow_field)

        return pa.struct(schema_so_far)

    @classmethod
    def type_to_metadata(cls, t: Type, conversion: bool) -> Dict[str, bytes]:

        metadata = {}
        metadata["requires_conversion"] = b"1" if conversion else b"0"

        if hasattr(t, "__qualname__"):
            metadata["source_type"] = t.__qualname__.encode()
        elif hasattr(t, "_name"):
            metadata["source_type"] = t._name.encode()
        else:
            raise NotImplementedError(f"{t} doesn't have __qualname__ or _name")
        return metadata

    @classmethod
    def parse_type(cls, name: str, t: Type) -> pa.Field:
        if t in cls.primative_translation:
            nargs, f, source_type, conversion = cls.primative_translation[t]
            return pa.field(name, f()).with_metadata(cls.type_to_metadata(source_type, conversion))

        if get_origin(t) is not None:
            origin = get_origin(t)
            if origin in cls.origin_translation:
                arg_count, f, source_type, conversion = cls.origin_translation[origin]
                raw_args = get_args(t)
                assert arg_count == len(raw_args)
                if f == pa.map_:
                    args = [
                        cls.parse_type("key", raw_args[0]).with_nullable(False),
                        cls.parse_type("value", raw_args[1]),
                    ]
                else:
                    args = [cls.parse_type(f"{name}/{i}", arg) for i, arg in enumerate(raw_args)]

                return pa.field(name, f(*args)).with_metadata(cls.type_to_metadata(t, conversion))

        if pydataclasses.is_dataclass(t):
            assert isinstance(t, type), "unable to parse instances of dataclasses"
            return pa.field(name, cls.parse_dataclass(t)).with_metadata(cls.type_to_metadata(t, False))
        raise NotImplementedError(f"Could not parse {t}")
