"""Utilities for deserializing Paimon BlobDescriptor bytes into Arrow struct arrays."""

from __future__ import annotations

import struct

import pyarrow as pa  # noqa: TID253

BLOB_STRUCT_TYPE = pa.struct([
    pa.field("url", pa.utf8()),
    pa.field("offset", pa.int64()),
    pa.field("length", pa.int64()),
])

_BLOB_MAGIC = 0x424C4F4244455343  # "BLOBDESC"


def _deserialize_one(data: bytes) -> tuple[str, int, int]:
    """Deserialize a single BlobDescriptor → (url, offset, length)."""
    pos = 0
    version = data[pos]
    pos += 1

    if version > 1:
        pos += 8  # skip magic

    uri_len = struct.unpack_from("<I", data, pos)[0]
    pos += 4

    uri = data[pos : pos + uri_len].decode("utf-8")
    pos += uri_len

    offset = struct.unpack_from("<q", data, pos)[0]
    pos += 8

    length = struct.unpack_from("<q", data, pos)[0]
    return uri, offset, length


def blob_column_to_struct(column: pa.Array) -> pa.Array:
    """Convert a large_binary column of serialized BlobDescriptors to a struct array."""
    urls: list[str | None] = []
    offsets: list[int | None] = []
    lengths: list[int | None] = []

    for value in column:
        if value is None or not value.is_valid:
            urls.append(None)
            offsets.append(None)
            lengths.append(None)
        else:
            raw = value.as_py()
            uri, off, length = _deserialize_one(raw)
            urls.append(uri)
            offsets.append(off)
            lengths.append(length)

    return pa.StructArray.from_arrays(
        [pa.array(urls, type=pa.utf8()), pa.array(offsets, type=pa.int64()), pa.array(lengths, type=pa.int64())],
        names=["url", "offset", "length"],
    )
