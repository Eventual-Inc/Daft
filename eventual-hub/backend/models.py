from enum import Enum

from pydantic import BaseModel

from typing import Optional

class NotebookState(str, Enum):
    not_started = "not_started"
    pending = "pending"
    ready = "ready"

class UserNotebookDetails(BaseModel):
    url: Optional[str]
    ready: bool
    pending: Optional[str]
