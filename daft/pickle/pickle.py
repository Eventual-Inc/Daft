from __future__ import annotations

from typing import Any

from daft.pickle._colab_compat import IS_COLAB, clean_pydantic_model
from daft.pickle.cloudpickle import dumps as cloudpickle_dumps  # type: ignore
from daft.pickle.cloudpickle import loads as cloudpickle_loads  # type: ignore


def _is_pydantic_model_class(obj: Any) -> bool:
    """Check if obj is a Pydantic model class (not instance)."""
    try:
        from pydantic import BaseModel

        return isinstance(obj, type) and issubclass(obj, BaseModel)
    except ImportError:
        return False


def dumps(obj: Any) -> bytes:
    if IS_COLAB and _is_pydantic_model_class(obj):
        obj = clean_pydantic_model(obj)
    return cloudpickle_dumps(obj)


def loads(data: bytes) -> Any:
    return cloudpickle_loads(data)
