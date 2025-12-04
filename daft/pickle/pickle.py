from __future__ import annotations

import io
from typing import Any

from daft.pickle._colab_compat import IS_COLAB, clean_pydantic_model
from daft.pickle.cloudpickle import Pickler as CloudPickler  # type: ignore
from daft.pickle.cloudpickle import dumps as cloudpickle_dumps  # type: ignore
from daft.pickle.cloudpickle import loads as cloudpickle_loads  # type: ignore


class _ColabSafePickler(CloudPickler):
    """Custom pickler that cleans Pydantic models for Colab compatibility.

    This intercepts Pydantic model classes during serialization and replaces
    them with cleaned versions that don't capture Colab's polluted globals.
    Works for models nested at any depth in the object tree.
    """

    def reducer_override(self, obj: Any) -> Any:
        from pydantic import BaseModel

        if isinstance(obj, type) and issubclass(obj, BaseModel) and obj is not BaseModel:
            cleaned = clean_pydantic_model(obj)
            cleaned_bytes = cloudpickle_dumps(cleaned)
            return (cloudpickle_loads, (cleaned_bytes,))
        return NotImplemented


def dumps(obj: Any) -> bytes:
    if IS_COLAB:
        buffer = io.BytesIO()
        _ColabSafePickler(buffer).dump(obj)
        return buffer.getvalue()
    return cloudpickle_dumps(obj)


def loads(data: bytes) -> Any:
    return cloudpickle_loads(data)
