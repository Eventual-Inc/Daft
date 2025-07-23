from __future__ import annotations

from typing import Annotated

from fastapi import Header


def parse_range_from_header(range_header: Annotated[str, Header()], length: int) -> tuple[int, int]:
    """Parse the range header into a tuple of (start, end)."""
    start, end = (i for i in range_header[len("bytes=") :].split("-"))

    if start == "":  # suffix range: "bytes=-128"
        range_len = int(end)
        start = length - range_len if length > range_len else 0
        end = length - 1
    elif end == "":  # offset range: "bytes=128-"
        end = length - 1
        start = int(start)
    else:
        start = int(start)
        end = int(end)

    return start, end
