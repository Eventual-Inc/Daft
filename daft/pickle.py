from __future__ import annotations

import pickle


def dumps(obj: object) -> bytes:
    return pickle.dumps(obj)


def loads(data: bytes) -> object:
    return pickle.loads(data)
