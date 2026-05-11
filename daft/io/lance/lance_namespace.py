"""Shared helpers for Lance Partitioned Namespace support.

Implements the bits of the V2 Directory Namespace and Partitioning specs that
the partitioned write sink and namespace scan operator both need:

- Manifest table column names and Arrow schema.
- Random base36 namespace identifiers and the ``<hash>_<object_id>`` table
  directory naming convention.
- ``object_id`` construction (``v<N>$<id_1>$...$<id_k>[$dataset]``).
- JSON serialization for ``partition_spec_v<N>`` and the ``schema`` metadata
  property (the [JsonArrowSchema] format with ``lance:field_id`` per field).

[JsonArrowSchema]: https://github.com/lance-format/lance-namespace/blob/main/docs/src/client/operations/models/JsonArrowSchema.md
"""

from __future__ import annotations

import json
import secrets
import string
from dataclasses import dataclass
from typing import Any

from daft.dependencies import pa

# Manifest table column names (V2 directory base + partitioning extension).
COL_OBJECT_ID = "object_id"
COL_OBJECT_TYPE = "object_type"
COL_LOCATION = "location"
COL_METADATA = "metadata"
COL_BASE_OBJECTS = "base_objects"
COL_READ_VERSION = "read_version"
COL_READ_BRANCH = "read_branch"
COL_READ_TAG = "read_tag"
PARTITION_FIELD_PREFIX = "partition_field_"

# Object id segment constants.
TABLE_LEAF_NAME = "dataset"
OBJECT_ID_SEP = "$"

# Object types stored in the manifest.
OBJECT_TYPE_NAMESPACE = "namespace"
OBJECT_TYPE_TABLE = "table"

# Schema metadata keys on the __manifest table.
METADATA_KEY_SCHEMA = "schema"
METADATA_KEY_PARTITION_SPEC_PREFIX = "partition_spec_v"
# JsonArrowField metadata key carrying the Lance field id.
LANCE_FIELD_ID_KEY = "lance:field_id"
# Field metadata key for the unenforced primary key position (composite-key position; "0" for first).
# Used on the manifest's `object_id` column so Lance treats it as a primary key for
# merge-insert conflict detection. Matches `LANCE_UNENFORCED_PRIMARY_KEY_POSITION` in
# `lance-core/src/datatypes/field.rs`.
LANCE_UNENFORCED_PRIMARY_KEY_POSITION_KEY = "lance-schema:unenforced-primary-key:position"

_BASE36_ALPHABET = string.ascii_lowercase + string.digits


@dataclass(frozen=True)
class PartitionFieldSpec:
    """One entry in a `partition_spec_v<N>.fields` array.

    Attributes:
        field_id: Stable string identifier for this partition field. Used as the
            ``partition_field_{field_id}`` column suffix in the manifest.
        source_field_name: The source column name in the user-facing schema (for
            error messages and Daft's ``PartitionField`` source_field).
        source_id: The ``lance:field_id`` of the source column in the namespace
            schema. Recorded in the spec's ``source_ids`` array.
        transform: Well-known transform name (e.g. ``"identity"``, ``"year"``).
            The initial implementation only writes ``"identity"``.
        result_type: Arrow type of the partition value.
    """

    field_id: str
    source_field_name: str
    source_id: int
    transform: str
    result_type: pa.DataType


def random_namespace_id() -> str:
    """Generate a 16-character base36 namespace identifier (a-z0-9).

    Per the Partitioning spec, partition namespace names are random 16-char
    base36 strings, giving ~83 bits of entropy so collisions are practically
    impossible.
    """
    return "".join(secrets.choice(_BASE36_ALPHABET) for _ in range(16))


def random_dir_hash() -> str:
    """Generate the 8-char lowercase hex prefix for a leaf table directory.

    The V2 directory namespace requires table directories to use the format
    ``<hash>_<object_id>``. The hash is for object-store prefix spreading and
    conflict prevention on create/delete/recreate; it does not need to be
    derivable from ``object_id``.
    """
    return secrets.token_hex(4)


def make_object_id(spec_version: str, ns_ids: list[str], *, is_table: bool) -> str:
    """Construct an ``object_id`` from its segments.

    Args:
        spec_version: The spec version segment, e.g. ``"v1"``.
        ns_ids: The list of namespace identifiers (random base36 strings) along
            the path from the spec-version namespace to the leaf.
        is_table: When True, append the literal ``"dataset"`` segment.
    """
    segments = [spec_version, *ns_ids]
    if is_table:
        segments.append(TABLE_LEAF_NAME)
    return OBJECT_ID_SEP.join(segments)


def make_location(object_id: str) -> str:
    """Construct a relative table directory name as ``<hash>_<object_id>``.

    The hash is freshly randomized on every call; callers must persist the
    returned location into the manifest's ``location`` column so that the same
    table is later opened at the same physical path.
    """
    return f"{random_dir_hash()}_{object_id}"


def manifest_arrow_schema(
    partition_field_specs: list[PartitionFieldSpec],
    schema_metadata: dict[str, str] | None = None,
) -> pa.Schema:
    """Build the Arrow schema for the ``__manifest`` Lance table.

    The columns follow the V2 directory base spec (object_id, object_type,
    location, metadata, base_objects), plus the partitioning extension's
    read_version / read_branch / read_tag, plus one
    ``partition_field_{field_id}`` column per known partition field.
    """
    fields: list[pa.Field] = [
        pa.field(
            COL_OBJECT_ID,
            pa.utf8(),
            nullable=False,
            metadata={LANCE_UNENFORCED_PRIMARY_KEY_POSITION_KEY: "0"},
        ),
        pa.field(COL_OBJECT_TYPE, pa.utf8(), nullable=False),
        pa.field(COL_LOCATION, pa.utf8(), nullable=True),
        pa.field(COL_METADATA, pa.utf8(), nullable=True),
        # base_objects: List<Utf8> with inner field named "object_id" to match the
        # reference implementation in lance-namespace-impls.
        pa.field(
            COL_BASE_OBJECTS,
            pa.list_(pa.field("object_id", pa.utf8(), nullable=True)),
            nullable=True,
        ),
        pa.field(COL_READ_VERSION, pa.uint64(), nullable=True),
        pa.field(COL_READ_BRANCH, pa.utf8(), nullable=True),
        pa.field(COL_READ_TAG, pa.utf8(), nullable=True),
    ]
    for spec in partition_field_specs:
        fields.append(pa.field(f"{PARTITION_FIELD_PREFIX}{spec.field_id}", spec.result_type, nullable=True))

    return pa.schema(fields, metadata=schema_metadata or {})


# --------------------------------------------------------------------------
# JsonArrowDataType <-> pa.DataType
#
# The Partitioning spec references the JsonArrowDataType model:
#   { "type": "<type-name>", "length": <int?>, "fields": [<JsonArrowField>?] }
#
# We cover the data types Daft users are likely to partition on (numerics,
# booleans, strings, binary, dates, timestamps, decimals) and a few container
# types. Unknown / unsupported types raise so the manifest never silently
# drops type information.
# --------------------------------------------------------------------------

_PRIMITIVE_NAME_TO_ARROW: dict[str, pa.DataType] = {
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float16": pa.float16(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "utf8": pa.utf8(),
    "string": pa.utf8(),
    "large_utf8": pa.large_utf8(),
    "large_string": pa.large_utf8(),
    "binary": pa.binary(),
    "large_binary": pa.large_binary(),
    "date32": pa.date32(),
    "date64": pa.date64(),
    "null": pa.null(),
}


def arrow_type_to_json_arrow(dtype: pa.DataType) -> dict[str, Any]:
    """Serialize a PyArrow DataType into JsonArrowDataType form."""
    if pa.types.is_boolean(dtype):
        return {"type": "bool"}
    if pa.types.is_int8(dtype):
        return {"type": "int8"}
    if pa.types.is_int16(dtype):
        return {"type": "int16"}
    if pa.types.is_int32(dtype):
        return {"type": "int32"}
    if pa.types.is_int64(dtype):
        return {"type": "int64"}
    if pa.types.is_uint8(dtype):
        return {"type": "uint8"}
    if pa.types.is_uint16(dtype):
        return {"type": "uint16"}
    if pa.types.is_uint32(dtype):
        return {"type": "uint32"}
    if pa.types.is_uint64(dtype):
        return {"type": "uint64"}
    if pa.types.is_float16(dtype):
        return {"type": "float16"}
    if pa.types.is_float32(dtype):
        return {"type": "float32"}
    if pa.types.is_float64(dtype):
        return {"type": "float64"}
    if pa.types.is_string(dtype):
        return {"type": "utf8"}
    if pa.types.is_large_string(dtype):
        return {"type": "large_utf8"}
    if pa.types.is_binary(dtype):
        return {"type": "binary"}
    if pa.types.is_large_binary(dtype):
        return {"type": "large_binary"}
    if pa.types.is_fixed_size_binary(dtype):
        return {"type": "fixed_size_binary", "length": dtype.byte_width}
    if pa.types.is_date32(dtype):
        return {"type": "date32"}
    if pa.types.is_date64(dtype):
        return {"type": "date64"}
    if pa.types.is_timestamp(dtype):
        tz = dtype.tz
        return {"type": f"timestamp[{dtype.unit}{(',tz=' + tz) if tz else ''}]"}
    if pa.types.is_decimal128(dtype):
        return {"type": f"decimal128({dtype.precision},{dtype.scale})"}
    if pa.types.is_list(dtype):
        return {
            "type": "list",
            "fields": [_arrow_field_to_json_arrow(dtype.value_field)],
        }
    if pa.types.is_large_list(dtype):
        return {
            "type": "large_list",
            "fields": [_arrow_field_to_json_arrow(dtype.value_field)],
        }
    if pa.types.is_struct(dtype):
        return {
            "type": "struct",
            "fields": [_arrow_field_to_json_arrow(dtype.field(i)) for i in range(dtype.num_fields)],
        }
    if pa.types.is_null(dtype):
        return {"type": "null"}
    raise ValueError(f"Cannot encode Arrow type {dtype!r} as JsonArrowDataType")


def json_arrow_to_arrow_type(obj: dict[str, Any]) -> pa.DataType:
    """Deserialize a JsonArrowDataType back into a PyArrow DataType."""
    type_name = obj["type"]
    if type_name in _PRIMITIVE_NAME_TO_ARROW:
        return _PRIMITIVE_NAME_TO_ARROW[type_name]
    if type_name == "fixed_size_binary":
        return pa.binary(int(obj["length"]))
    if type_name.startswith("timestamp["):
        inner = type_name[len("timestamp[") : -1]
        if ",tz=" in inner:
            unit, tz = inner.split(",tz=", 1)
            return pa.timestamp(unit, tz=tz)
        return pa.timestamp(inner)
    if type_name.startswith("decimal128("):
        inner = type_name[len("decimal128(") : -1]
        precision, scale = inner.split(",", 1)
        return pa.decimal128(int(precision), int(scale))
    if type_name == "list":
        inner = obj["fields"][0]
        return pa.list_(_json_arrow_to_arrow_field(inner))
    if type_name == "large_list":
        inner = obj["fields"][0]
        return pa.large_list(_json_arrow_to_arrow_field(inner))
    if type_name == "struct":
        return pa.struct([_json_arrow_to_arrow_field(f) for f in obj["fields"]])
    raise ValueError(f"Unsupported JsonArrowDataType: {obj!r}")


def _arrow_field_to_json_arrow(field: pa.Field) -> dict[str, Any]:
    out: dict[str, Any] = {
        "name": field.name,
        "nullable": field.nullable,
        "type": arrow_type_to_json_arrow(field.type),
    }
    if field.metadata:
        out["metadata"] = {
            (k.decode() if isinstance(k, (bytes, bytearray)) else k): (
                v.decode() if isinstance(v, (bytes, bytearray)) else v
            )
            for k, v in field.metadata.items()
        }
    return out


def _json_arrow_to_arrow_field(obj: dict[str, Any]) -> pa.Field:
    name = obj["name"]
    dtype = json_arrow_to_arrow_type(obj["type"])
    nullable = bool(obj.get("nullable", True))
    metadata = obj.get("metadata")
    return pa.field(name, dtype, nullable=nullable, metadata=metadata)


def make_partition_spec_json(spec_id: int, fields: list[PartitionFieldSpec]) -> str:
    """Serialize a list of `PartitionFieldSpec`s as a `partition_spec_v<N>` value."""
    payload: dict[str, Any] = {
        "id": spec_id,
        "fields": [
            {
                "field_id": spec.field_id,
                "source_ids": [spec.source_id],
                "transform": {"type": spec.transform},
                "result_type": arrow_type_to_json_arrow(spec.result_type),
            }
            for spec in fields
        ],
    }
    return json.dumps(payload)


def make_namespace_schema_json(arrow_schema: pa.Schema, field_ids: dict[str, int]) -> str:
    """Serialize a namespace schema as the `schema` JSON property.

    Each field carries a ``lance:field_id`` metadata entry. Field IDs are
    immutable across a namespace's lifetime; ``field_ids`` maps column name to
    its id.
    """
    out_fields: list[dict[str, Any]] = []
    for f in arrow_schema:
        fid = field_ids[f.name]
        out_fields.append(
            {
                "name": f.name,
                "nullable": f.nullable,
                "type": arrow_type_to_json_arrow(f.type),
                "metadata": {LANCE_FIELD_ID_KEY: str(fid)},
            }
        )
    return json.dumps({"fields": out_fields})


def parse_partition_spec_json(payload: str | bytes) -> dict[str, Any]:
    """Parse a `partition_spec_v<N>` JSON value, accepting bytes or str."""
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode()
    return json.loads(payload)


def parse_namespace_schema_json(payload: str | bytes) -> pa.Schema:
    """Parse the `schema` JSON metadata value back into a PyArrow schema."""
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode()
    obj = json.loads(payload)
    return pa.schema([_json_arrow_to_arrow_field(f) for f in obj["fields"]])


def parse_object_id(object_id: str) -> tuple[str, list[str], bool]:
    """Split an object_id into (spec_version, namespace_ids, is_table).

    For ``v1$abc$def$dataset`` returns ``("v1", ["abc", "def"], True)``.
    For ``v1$abc$def`` returns ``("v1", ["abc", "def"], False)``.
    For ``v1`` returns ``("v1", [], False)``.
    """
    parts = object_id.split(OBJECT_ID_SEP)
    if not parts:
        raise ValueError(f"Empty object_id: {object_id!r}")
    spec_version = parts[0]
    rest = parts[1:]
    is_table = bool(rest) and rest[-1] == TABLE_LEAF_NAME
    ns_ids = rest[:-1] if is_table else rest
    return spec_version, ns_ids, is_table


def latest_partition_spec_key(schema_metadata: dict[Any, Any] | None) -> tuple[int, str] | None:
    """Find the highest-numbered `partition_spec_v<N>` key in schema metadata.

    Returns ``(n, key_as_str)`` for the latest spec, or ``None`` if no spec key
    is present. ``schema_metadata`` may be a PyArrow metadata mapping with
    either str or bytes keys.
    """
    if not schema_metadata:
        return None
    best: tuple[int, str] | None = None
    for key in schema_metadata:
        key_str = key.decode() if isinstance(key, (bytes, bytearray)) else key
        if not key_str.startswith(METADATA_KEY_PARTITION_SPEC_PREFIX):
            continue
        try:
            n = int(key_str[len(METADATA_KEY_PARTITION_SPEC_PREFIX) :])
        except ValueError:
            continue
        if best is None or n > best[0]:
            best = (n, key_str)
    return best


def get_schema_metadata_value(schema_metadata: dict[Any, Any] | None, key: str) -> bytes | str | None:
    """Look up a metadata entry by key, tolerating both str and bytes keys."""
    if not schema_metadata:
        return None
    if key in schema_metadata:
        return schema_metadata[key]
    key_b = key.encode()
    if key_b in schema_metadata:
        return schema_metadata[key_b]
    return None
