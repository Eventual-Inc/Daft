from __future__ import annotations

from daft.pickle.cloudpickle_fast import dumps as cloudpickle_dumps
from daft.pickle.cloudpickle_fast import loads as cloudpickle_loads


def dumps(obj: object) -> bytes:
    return cloudpickle_dumps(obj)


def loads(data: bytes) -> object:
    return cloudpickle_loads(data)
