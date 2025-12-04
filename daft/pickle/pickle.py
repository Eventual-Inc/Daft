from __future__ import annotations

from typing import Any

from daft.pickle._colab_compat import IS_COLAB, colab_safe_dumps
from daft.pickle.cloudpickle import dumps as cloudpickle_dumps  # type: ignore
from daft.pickle.cloudpickle import loads as cloudpickle_loads  # type: ignore


def dumps(obj: Any) -> bytes:
    if IS_COLAB:
        return colab_safe_dumps(obj)
    return cloudpickle_dumps(obj)


def loads(data: bytes) -> Any:
    return cloudpickle_loads(data)
