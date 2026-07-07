from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from daft.dependencies import mcap as _mcap_mod

if TYPE_CHECKING:
    from collections.abc import Callable


class JsonDecoderFactory(_mcap_mod.reader.DecoderFactory):  # type: ignore
    def decoder_for(self, message_encoding: str, schema: str | None = None) -> Callable[[bytes], Any] | None:
        if message_encoding == "json":
            return lambda data: json.loads(data.decode("utf-8"))
        return None
