from __future__ import annotations

import pyarrow as pa

ARROW_INT_TYPES = [pa.int8(), pa.uint8(), pa.int16(), pa.uint16(), pa.int32(), pa.uint32(), pa.int64(), pa.uint64()]
ARROW_STRING_TYPES = [pa.string(), pa.large_string()]
ARROW_FLOAT_TYPES = [pa.float32(), pa.float64()]
