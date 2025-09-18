from __future__ import annotations

def estimate_in_memory_size_bytes(
    uri: str, file_size: int, columns: list[str] | None = None, has_metadata: bool = False
) -> int: ...
