from __future__ import annotations

from typing import ClassVar, Dict

import threading


class Session:
    """Global session for the current python environment
    
    Environmnet Variables
    ------------------------------------------
    DAFT_SESSION_TMP
    DAFT_SESSION_USER
    DAFT_SESSION_DEFAULT_CATALOG_NAME
    DAFT_SESSION_DEFAULT_NAMESPACE_NAME
    
    """

    _instance: ClassVar[Session | None] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    _properties: Dict[str,str]

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                # check _instance is still None
                if not cls._instance:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        self._properties = dict()

    def set_default_catalog(self, name: str):
        self._properties["default_catalog"] = name

    def set_default_namespace(self, name: str):
        self._properties["default_namespace"] = name

    def __repr__(self) -> str:
        return f"session({repr(self._properties)})"

_Session = Session()


def session() -> Session:
    return _Session
