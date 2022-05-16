from abc import abstractmethod
from typing import List, Type, Union

import dataclasses as pydataclasses

from enum import Enum

@pydataclasses.dataclass
class DaftType:
    pass

@pydataclasses.dataclass
class DaftImageType(DaftType):
    class Encoding(Enum):
        JPEG='jpeg'

    encoding: Encoding
