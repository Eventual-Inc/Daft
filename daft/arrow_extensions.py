import pyarrow as pa

class DaftArrowImage(pa.ExtensionType):
    def __init__(self, encoding='jpeg'):
        self._encoding = encoding
        pa.ExtensionType.__init__(self, pa.binary(), f"Daft.Image.{encoding}")
        
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
        return DaftArrowImage(encoding)