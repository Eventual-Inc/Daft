from typing import TypeVar, Generic, Dict, Optional
import typing
import io
import dataclasses


import pyarrow as pa
import PIL
import PIL.Image

import numpy as np

T = TypeVar('T')


class DaftType(Generic[T]):
    pass

class DaftImage(DaftType[T]):
    pass


class DaftPILImage(pa.ExtensionType):
    def __init__(self, encoding='jpeg'):
        self._encoding = encoding
        pa.ExtensionType.__init__(self, pa.binary(), f"Daft.PIL.Image.{encoding}")
        
    def __arrow_ext_serialize__(self):
        # since we don't have a parameterized type, we don't need extra
        # metadata to be deserialized
        return f'encoding={self._encoding}'.encode()

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        # return an instance of this subclass given the serialized
        # metadata.
        serialized = serialized.decode()
        assert serialized.startswith("encoding=")
        encoding = serialized.split('=')[1]
        return DaftPILImage(encoding)



pil_type = DaftPILImage()
try:
    pa.register_extension_type(pil_type)
except pa.ArrowKeyError:
    pass
    
np_arrow_type = pa.struct(
    [
        ("shape", pa.list_(pa.int64())),
        ("type", pa.string()),
        ("data", pa.binary())
    ])

translation: Dict[type, pa.lib.DataType] = {
    PIL.Image.Image: DaftPILImage(),
    np.ndarray: pa.binary()
}
    

def pytype_to_arrow_type(t: type) -> pa.DataType:
    
    if typing.get_origin(t) is not None:
        origin = typing.get_origin(t)
        args = typing.get_args(t)
        t = args[0]
    arrow_type: Optional[pa.DataType] = None
    try: 
        arrow_type = pa.from_numpy_dtype(t)
    except pa.ArrowNotImplementedError:
        supported = False
    else: 
        supported = True
        
    if supported and t not in translation:
        return arrow_type
    elif t in translation:
        return translation[t]
    
        
    raise NotImplementedError(f"{t} not supported type")
        
    
def to_arrow_schema(cls: type):
    annotations: Dict[str, type] = typing.get_type_hints(cls)
    
    arrow_schema = dict()
    for k, pytype in annotations.items():
        
        pa_type = pytype_to_arrow_type(pytype)
        arrow_schema[k] = pa_type        
    
    return pa.schema(arrow_schema)

def encode_pil(img: PIL.Image.Image) -> bytes:
    with io.BytesIO() as img_bytes:
        img.save(img_bytes, 'jpeg')
        return img_bytes.getvalue()

reduce_type = {
    PIL.Image.Image: encode_pil,
    np.ndarray: lambda x: x.tobytes()
}


def arrow_serializer(obj):
    ty = type(obj)
    if ty in reduce_type:
        return reduce_type[ty](obj)
    return obj


def dataclass(cls=None):
    
    schema = to_arrow_schema(cls)
    print(schema)
    @classmethod
    def _arrow_schema(cls) -> pa.Schema:
        return schema
    
    def _serialize(self):
        return {k: arrow_serializer(v) for k, v in dataclasses.asdict(self).items()}
    
    new_data_class = dataclasses.dataclass(cls)
    setattr(new_data_class, '_arrow_schema', _arrow_schema)
    setattr(new_data_class, '_serialize', _serialize)
    return new_data_class