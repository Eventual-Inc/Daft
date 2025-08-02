from __future__ import annotations

import json
from typing import Any, Callable

from mcap.reader import DecoderFactory


class JsonDecoderFactory(DecoderFactory):  # type: ignore
    def decoder_for(self, message_encoding: str, schema: str | None = None) -> Callable[[bytes], Any] | None:
        if message_encoding == "json":
            return lambda data: json.loads(data.decode("utf-8"))
        return None
