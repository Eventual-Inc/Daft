from __future__ import annotations

import pickle


def serialize(obj: object) -> bytes:
    return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)


def deserialize(data: bytes) -> object:
    return pickle.loads(data)
