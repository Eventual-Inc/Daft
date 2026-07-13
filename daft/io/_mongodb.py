# isort: dont-add-import: from __future__ import annotations

from __future__ import annotations

import base64
import binascii
import json
import math
import string
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import TYPE_CHECKING, Any

from daft import Schema, context, runners
from daft.api_annotations import PublicAPI
from daft.daft import (
    IOConfig,
    MongoSourceConfig,
    PyPartitionField,
    PyPushdowns,
    ScanOperatorHandle,
    ScanTask,
    StorageConfig,
)
from daft.dataframe import DataFrame
from daft.datatype import DataType
from daft.io._mongodb_predicate import convert_filters_to_mongo
from daft.io.scan import ScanOperator
from daft.logical.builder import LogicalPlanBuilder

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft.daft import PyExpr

MongoJson = Mapping[str, Any]
MongoPartitionRange = tuple[Any | None, Any | None]
_I64_MIN = -(2**63)
_I64_MAX = 2**63 - 1
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_HEX_DIGITS = frozenset(string.hexdigits)


@PublicAPI
def read_mongodb(
    uri: str,
    database: str,
    collection: str,
    schema: Schema | dict[str, DataType],
    *,
    filter: MongoJson | None = None,
    projection: MongoJson | None = None,
    hint: MongoJson | str | None = None,
    max_time_ms: int | None = None,
    partition_field: str | None = None,
    partition_ranges: Sequence[MongoPartitionRange] | None = None,
    batch_size: int = 1000,
    io_config: IOConfig | None = None,
    _multithreaded_io: bool | None = None,
) -> DataFrame:
    """Create a DataFrame from a MongoDB collection.

    This connector requires an explicit schema and does not inspect the collection
    during planning. Provide ``filter`` for server-side predicate pushdown and
    ``partition_ranges`` for distributed range reads. For operational clusters,
    prefer indexed filters, ``hint``, bounded ``partition_ranges``, and
    ``max_time_ms`` over unbounded collection scans.

    Args:
        uri: MongoDB connection string.
        database: Database name.
        collection: Collection name.
        schema: Expected Daft schema. BSON values are converted to this schema.
        filter: MongoDB filter document, encoded with JSON-compatible values. Use
            MongoDB Extended JSON for ObjectIds, e.g. ``{"_id": {"$oid": "..."}}``.
        projection: Optional MongoDB projection document. Daft projection pushdown
            is also applied at execution time.
        hint: Optional index hint, either an index key document or an index name.
        max_time_ms: Optional MongoDB server-side max time for each ``find``.
        partition_field: Field to use for range partitioning. This field does
            not need to be present in ``schema``.
        partition_ranges: Half-open ``(lower, upper)`` ranges for ``partition_field``.
            ``None`` on either side means unbounded. If omitted, one scan task is
            created and no hidden collection metadata query is issued.
        batch_size: MongoDB cursor batch size and Daft task batch hint.
        io_config: Reserved for consistency with other readers.
        _multithreaded_io: Whether to use multithreaded IO runtime. Defaults to
            non-Ray behavior used by other readers.

    Note:
        Enable ``daft.context.set_planning_config(enable_strict_filter_pushdown=True)``
        when combining this reader with arbitrary Daft ``where`` predicates so
        unsupported predicates remain above the MongoDB scan.

    Returns:
        DataFrame: Data read from MongoDB.

    Examples:
        >>> import daft
        >>> df = daft.read_mongodb(
        ...     "mongodb://localhost:27017",
        ...     "app",
        ...     "events",
        ...     {"_id": daft.DataType.string(), "created_at": daft.DataType.int64()},
        ...     filter={"kind": "click"},
        ...     hint={"created_at": 1},
        ...     partition_field="created_at",
        ...     partition_ranges=[(0, 1_000), (1_000, 2_000)],
        ...     max_time_ms=30_000,
        ... )
    """
    for arg_name, value in (("uri", uri), ("database", database), ("collection", collection)):
        if not isinstance(value, str):
            raise TypeError(f"read_mongodb {arg_name} must be a string")
        if not value:
            raise ValueError(f"read_mongodb {arg_name} must be non-empty")

    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise TypeError("read_mongodb batch_size must be an integer")
    if batch_size <= 0:
        raise ValueError("read_mongodb batch_size must be > 0")
    if batch_size > 2**32 - 1:
        raise ValueError("read_mongodb batch_size must fit in u32")

    if max_time_ms is not None and (isinstance(max_time_ms, bool) or not isinstance(max_time_ms, int)):
        raise TypeError("read_mongodb max_time_ms must be an integer")
    if max_time_ms is not None and max_time_ms <= 0:
        raise ValueError("read_mongodb max_time_ms must be > 0")
    if max_time_ms is not None and max_time_ms > 2**64 - 1:
        raise ValueError("read_mongodb max_time_ms must fit in u64")

    if partition_ranges is not None and partition_field is None:
        raise ValueError("read_mongodb partition_field is required when partition_ranges is provided")

    daft_schema = schema if isinstance(schema, Schema) else Schema.from_pydict(schema)
    if partition_field is not None and not isinstance(partition_field, str):
        raise TypeError("read_mongodb partition_field must be a string")
    if partition_field is not None and not partition_field:
        raise ValueError("read_mongodb partition_field must be non-empty")

    io_config = context.get_context().daft_planning_config.default_io_config if io_config is None else io_config
    multithreaded_io = (
        (runners.get_or_create_runner().name != "ray") if _multithreaded_io is None else _multithreaded_io
    )
    storage_config = StorageConfig(multithreaded_io, io_config)

    operator = MongoScanOperator(
        uri=uri,
        database=database,
        collection=collection,
        schema=daft_schema,
        filter_json=_json_dumps_document(filter, "filter") if filter is not None else None,
        projection_json=_json_dumps_projection(projection) if projection is not None else None,
        hint_json=_json_dumps_hint(hint) if hint is not None else None,
        max_time_ms=max_time_ms,
        partition_field=partition_field,
        partition_ranges=_normalize_partition_ranges(partition_ranges),
        batch_size=batch_size,
        storage_config=storage_config,
    )
    handle = ScanOperatorHandle.from_python_scan_operator(operator)
    builder = LogicalPlanBuilder.from_tabular_scan(scan_operator=handle)
    return DataFrame(builder)


class MongoScanOperator(ScanOperator):
    def __init__(
        self,
        *,
        uri: str,
        database: str,
        collection: str,
        schema: Schema,
        filter_json: str | None,
        projection_json: str | None,
        hint_json: str | None,
        max_time_ms: int | None,
        partition_field: str | None,
        partition_ranges: list[MongoPartitionRange] | None,
        batch_size: int,
        storage_config: StorageConfig,
    ) -> None:
        self._uri = uri
        self._database = database
        self._collection = collection
        self._schema = schema
        self._filter_json = filter_json
        self._projection_json = projection_json
        self._hint_json = hint_json
        self._max_time_ms = max_time_ms
        self._partition_field = partition_field
        self._partition_ranges = partition_ranges if partition_ranges is not None else [(None, None)]
        self._batch_size = batch_size
        self._storage_config = storage_config
        self._partitioning_keys: list[PyPartitionField] = []
        self._pushed_filter_json: str | None = None
        self._strict_filter_pushdown_seen = False

    def name(self) -> str:
        return "MongoScanOperator"

    def schema(self) -> Schema:
        return self._schema

    def display_name(self) -> str:
        return f"MongoScanOperator({self._database}.{self._collection})"

    def partitioning_keys(self) -> list[PyPartitionField]:
        return self._partitioning_keys

    def multiline_display(self) -> list[str]:
        return [
            self.display_name(),
            f"Schema = {self._schema}",
            f"Partition field = {self._partition_field}",
            f"Num scan tasks = {len(self._partition_ranges)}",
            f"Batch size = {self._batch_size}",
            f"Hint = {'<provided>' if self._hint_json is not None else None}",
            f"Max time = {self._max_time_ms} ms" if self._max_time_ms is not None else "Max time = None",
        ]

    def to_scan_tasks(self, pushdowns: PyPushdowns) -> Iterator[ScanTask]:
        filter_json = self._combined_filter_json(self._unconverted_filter_pushdown(pushdowns))
        partition_ranges = self._partition_ranges_for_filter(filter_json)
        task_pushdowns = _pushdowns_for_scan_tasks(pushdowns, len(partition_ranges))
        for lower, upper in partition_ranges:
            config = MongoSourceConfig(
                uri=self._uri,
                database=self._database,
                collection=self._collection,
                filter_json=filter_json,
                projection_json=self._projection_json,
                hint_json=self._hint_json,
                max_time_ms=self._max_time_ms,
                partition_field=self._partition_field,
                partition_lower_json=_json_dumps_value(lower) if lower is not None else None,
                partition_upper_json=_json_dumps_value(upper) if upper is not None else None,
                batch_size=self._batch_size,
            )
            yield ScanTask.mongo_scan_task(
                url=f"mongodb://{self._database}/{self._collection}",
                config=config,
                schema=self._schema._schema,
                storage_config=self._storage_config,
                num_rows=None,
                size_bytes=None,
                pushdowns=task_pushdowns,
                stats=None,
            )

    def can_absorb_filter(self) -> bool:
        return True

    def can_absorb_limit(self) -> bool:
        return len(self._partition_ranges) == 1

    def can_absorb_select(self) -> bool:
        return True

    def push_filters(self, filters: list[PyExpr]) -> tuple[list[PyExpr], list[PyExpr]]:
        self._strict_filter_pushdown_seen = True
        pushed_filters, remaining_filters, mongo_filter = convert_filters_to_mongo(filters)
        if mongo_filter is not None:
            self._pushed_filter_json = _merge_filter_json(self._pushed_filter_json, mongo_filter)
        return pushed_filters, remaining_filters

    def _unconverted_filter_pushdown(self, pushdowns: PyPushdowns) -> MongoJson | None:
        if pushdowns.filters is None or self._strict_filter_pushdown_seen:
            return None

        _, remaining_filters, mongo_filter = convert_filters_to_mongo(pushdowns.filters)
        if remaining_filters:
            raise ValueError(
                "read_mongodb received a filter pushdown that cannot be represented as a MongoDB filter. "
                "Use daft.context.set_planning_config(enable_strict_filter_pushdown=True) so unsupported "
                "predicates remain above the scan."
            )
        return mongo_filter

    def _combined_filter_json(self, extra_filter: MongoJson | None = None) -> str | None:
        filters = []
        if self._filter_json is not None:
            filters.append(json.loads(self._filter_json))
        if self._pushed_filter_json is not None:
            filters.append(json.loads(self._pushed_filter_json))
        if extra_filter is not None:
            filters.append(extra_filter)
        return _dump_combined_filter_documents(filters, "filter")

    def _partition_ranges_for_filter(self, filter_json: str | None) -> list[MongoPartitionRange]:
        if self._partition_field is None or filter_json is None:
            return self._partition_ranges

        comparable_types = _partition_range_comparable_types(self._partition_ranges)
        constraints = _partition_constraints_from_filter(
            json.loads(filter_json), self._partition_field, comparable_types
        )
        if constraints is None:
            return self._partition_ranges

        return [
            (lower, upper)
            for lower, upper in self._partition_ranges
            if any(_task_range_overlaps_constraint(lower, upper, constraint) for constraint in constraints)
        ]


def _merge_filter_json(existing_json: str | None, new_filter: MongoJson) -> str:
    if existing_json is None:
        return _json_dumps_document(new_filter, "pushed filter")

    existing_filter = json.loads(existing_json)
    return _dump_combined_filter_documents([existing_filter, new_filter], "pushed filter") or existing_json


def _pushdowns_for_scan_tasks(pushdowns: PyPushdowns, num_scan_tasks: int) -> PyPushdowns:
    limit = pushdowns.limit if num_scan_tasks == 1 else None
    if (
        pushdowns.filters is None
        and pushdowns.partition_filters is None
        and pushdowns.aggregation is None
        and pushdowns.limit == limit
    ):
        return pushdowns
    return PyPushdowns(
        columns=pushdowns.columns,
        limit=limit,
    )


def _dump_combined_filter_documents(filters: list[MongoJson], arg_name: str) -> str | None:
    filters = _dedupe_filter_documents([part for filter_doc in filters for part in _and_filter_parts(filter_doc)])
    if not filters:
        return None
    if len(filters) == 1:
        return _json_dumps_document(filters[0], arg_name)
    return _json_dumps_document({"$and": filters}, arg_name)


def _and_filter_parts(filter_doc: MongoJson) -> list[MongoJson]:
    if set(filter_doc.keys()) == {"$and"} and isinstance(filter_doc.get("$and"), list):
        parts = filter_doc["$and"]
        if parts and all(isinstance(part, Mapping) for part in parts):
            return parts
    return [filter_doc]


def _dedupe_filter_documents(filters: list[MongoJson]) -> list[MongoJson]:
    deduped: list[MongoJson] = []
    seen: set[str] = set()
    for filter_doc in filters:
        if not filter_doc:
            continue
        key = json.dumps(filter_doc, allow_nan=False, separators=(",", ":"), sort_keys=True)
        if key not in seen:
            seen.add(key)
            deduped.append(filter_doc)
    return deduped


def _json_dumps_document(value: MongoJson, arg_name: str = "document") -> str:
    if not isinstance(value, Mapping):
        raise TypeError(f"read_mongodb {arg_name} must be a mapping")
    return json.dumps(_json_ready_value(value), allow_nan=False, separators=(",", ":"), sort_keys=True)


def _json_dumps_projection(value: MongoJson) -> str:
    if not isinstance(value, Mapping):
        raise TypeError("read_mongodb projection must be a mapping")
    projection = _json_ready_value(value)
    _validate_projection_modes(projection)
    return json.dumps(projection, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _validate_projection_modes(projection: MongoJson) -> None:
    has_inclusion = False
    has_exclusion = False
    for field, value in projection.items():
        if field == "_id":
            continue
        if value in (1, True):
            has_inclusion = True
        elif value in (0, False):
            has_exclusion = True
    if has_inclusion and has_exclusion:
        raise ValueError("read_mongodb projection cannot mix inclusion and exclusion fields except for _id")


def _json_dumps_hint(value: MongoJson | str) -> str:
    if not isinstance(value, str | Mapping):
        raise TypeError("read_mongodb hint must be a mapping or index name string")
    return json.dumps(_json_ready_value(value), allow_nan=False, separators=(",", ":"))


def _normalize_partition_ranges(
    partition_ranges: Sequence[MongoPartitionRange] | None,
) -> list[MongoPartitionRange] | None:
    if partition_ranges is None:
        return None

    ranges: list[MongoPartitionRange] = []
    for partition_range in partition_ranges:
        if (
            isinstance(partition_range, str | bytes)
            or not isinstance(partition_range, Sequence)
            or len(partition_range) != 2
        ):
            raise TypeError("read_mongodb partition_ranges must contain (lower, upper) pairs")
        lower, upper = partition_range
        if lower is not None:
            _json_ready_value(lower)
        if upper is not None:
            _json_ready_value(upper)
        if _is_reversed_or_empty_range(lower, upper):
            raise ValueError("read_mongodb partition_ranges must be half-open ranges with lower < upper")
        ranges.append((lower, upper))
    return ranges


def _json_dumps_value(value: Any) -> str:
    return json.dumps(_json_ready_value(value), allow_nan=False, separators=(",", ":"), sort_keys=True)


def _json_ready_value(value: Any) -> Any:
    if value is None or isinstance(value, str | bool):
        return value
    if isinstance(value, int):
        if value < _I64_MIN or value > _I64_MAX:
            raise ValueError("read_mongodb integer JSON values must fit in i64")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("read_mongodb JSON values must be finite")
        return value
    if isinstance(value, datetime):
        return {"$date": {"$numberLong": str(_datetime_to_epoch_millis(value))}}
    if isinstance(value, date):
        value = datetime.combine(value, time(), tzinfo=timezone.utc)
        return {"$date": {"$numberLong": str(_datetime_to_epoch_millis(value))}}
    if isinstance(value, bytes):
        raise TypeError("read_mongodb bytes values must be encoded explicitly, for example with MongoDB Extended JSON")
    if isinstance(value, Mapping):
        _validate_supported_extended_json(value)
        converted: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("read_mongodb JSON document keys must be strings")
            converted[key] = _json_ready_value(item)
        return converted
    if isinstance(value, Sequence):
        return [_json_ready_value(item) for item in value]
    raise TypeError(f"read_mongodb value is not JSON serializable: {type(value).__name__}")


def _validate_supported_extended_json(value: Mapping[Any, Any]) -> None:
    extended_json_keys = {
        "$oid",
        "$numberLong",
        "$numberInt",
        "$numberDouble",
        "$date",
        "$binary",
    }
    if len(value) != 1:
        extra_keys = extended_json_keys.intersection(value.keys())
        if extra_keys:
            key = sorted(extra_keys)[0]
            raise ValueError(f"read_mongodb MongoDB {key} extended JSON object cannot contain sibling keys")
        return

    if "$oid" in value:
        oid = value["$oid"]
        if not isinstance(oid, str):
            raise TypeError("read_mongodb MongoDB $oid extended JSON value must be a string")
        if _comparable_object_id_value(oid) is None:
            raise ValueError("read_mongodb MongoDB $oid value must be a 24-character hex string")
    elif "$numberLong" in value:
        _parse_sized_int_string(value["$numberLong"], "$numberLong", _I64_MIN, _I64_MAX)
    elif "$numberInt" in value:
        _parse_sized_int_string(value["$numberInt"], "$numberInt", -(2**31), 2**31 - 1)
    elif "$numberDouble" in value:
        number_double = value["$numberDouble"]
        if not isinstance(number_double, str):
            raise TypeError("read_mongodb MongoDB $numberDouble extended JSON value must be a string")
        try:
            parsed = float(number_double)
        except ValueError as exc:
            raise ValueError("read_mongodb MongoDB $numberDouble value must be a valid float") from exc
        if not math.isfinite(parsed):
            raise ValueError("read_mongodb MongoDB $numberDouble value must be finite")
    elif "$date" in value:
        _validate_extended_json_date(value["$date"])
    elif "$binary" in value:
        _validate_extended_json_binary(value["$binary"])


def _parse_sized_int_string(value: Any, name: str, lower: int, upper: int) -> int:
    if not isinstance(value, str):
        raise TypeError(f"read_mongodb MongoDB {name} extended JSON value must be a string")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"read_mongodb MongoDB {name} value must be an integer string") from exc
    if parsed < lower or parsed > upper:
        raise ValueError(f"read_mongodb MongoDB {name} value is outside the supported range")
    return parsed


def _validate_extended_json_date(value: Any) -> None:
    if isinstance(value, bool):
        raise TypeError("read_mongodb MongoDB $date milliseconds must be an integer")
    if isinstance(value, int):
        if value < _I64_MIN or value > _I64_MAX:
            raise ValueError("read_mongodb MongoDB $date milliseconds must fit in i64")
        return
    if isinstance(value, Mapping):
        if set(value.keys()) != {"$numberLong"}:
            raise ValueError("read_mongodb MongoDB $date object must only contain $numberLong")
        _parse_sized_int_string(value.get("$numberLong"), "$date $numberLong", _I64_MIN, _I64_MAX)
        return
    raise TypeError("read_mongodb MongoDB $date value must be integer milliseconds or a $numberLong object")


def _validate_extended_json_binary(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise TypeError("read_mongodb MongoDB $binary value must be an object")
    if set(value.keys()) != {"base64", "subType"}:
        raise ValueError("read_mongodb MongoDB $binary object must only contain base64 and subType")
    base64_value = value.get("base64")
    if not isinstance(base64_value, str):
        raise TypeError("read_mongodb MongoDB $binary object must contain a string base64 value")
    try:
        base64.b64decode(base64_value, validate=True)
    except binascii.Error as exc:
        raise ValueError("read_mongodb MongoDB $binary base64 value must be valid base64") from exc
    subtype_value = value.get("subType")
    if not isinstance(subtype_value, str):
        raise TypeError("read_mongodb MongoDB $binary object must contain a string subType value")
    if len(subtype_value) != 2 or any(char not in _HEX_DIGITS for char in subtype_value):
        raise ValueError("read_mongodb MongoDB $binary subType value must be two hex characters")


def _datetime_to_epoch_millis(value: datetime) -> int:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    delta = value - _EPOCH
    return ((delta.days * 86_400 + delta.seconds) * 1_000) + (delta.microseconds // 1_000)


def _is_reversed_or_empty_range(lower: Any | None, upper: Any | None) -> bool:
    if lower is None or upper is None:
        return False
    lower_value = _comparable_mongo_value(lower)
    upper_value = _comparable_mongo_value(upper)
    if lower_value is None or upper_value is None:
        return False
    cmp = _compare_comparable_values(lower_value, upper_value)
    return False if cmp is None else cmp >= 0


@dataclass(frozen=True, slots=True)
class _Bound:
    value: tuple[str, Any]
    inclusive: bool


@dataclass(frozen=True, slots=True)
class _PartitionConstraint:
    lower: _Bound | None = None
    upper: _Bound | None = None
    values: tuple[tuple[str, Any], ...] | None = None
    impossible: bool = False


def _partition_constraints_from_filter(
    filter_doc: MongoJson, partition_field: str, comparable_types: set[str]
) -> list[_PartitionConstraint] | None:
    constraints = [_PartitionConstraint()]
    found_constraint = False

    field_constraint = _partition_constraint_from_field_filter(filter_doc.get(partition_field), comparable_types)
    if field_constraint is not None:
        constraints = _intersect_constraint_unions(constraints, [field_constraint])
        found_constraint = True

    and_filters = filter_doc.get("$and")
    if isinstance(and_filters, list):
        for child in and_filters:
            if isinstance(child, Mapping):
                child_constraints = _partition_constraints_from_filter(child, partition_field, comparable_types)
                if child_constraints is not None:
                    constraints = _intersect_constraint_unions(constraints, child_constraints)
                    found_constraint = True

    or_filters = filter_doc.get("$or")
    if isinstance(or_filters, list):
        child_constraints_union: list[_PartitionConstraint] = []
        all_children_supported = True
        for child in or_filters:
            if not isinstance(child, Mapping):
                all_children_supported = False
                break
            child_constraints = _partition_constraints_from_filter(child, partition_field, comparable_types)
            if child_constraints is None:
                all_children_supported = False
                break
            child_constraints_union.extend(child_constraints)
        if all_children_supported and child_constraints_union:
            constraints = _intersect_constraint_unions(constraints, child_constraints_union)
            found_constraint = True

    if not found_constraint:
        return None

    constraints = [constraint for constraint in constraints if not constraint.impossible]
    return constraints or [_PartitionConstraint(impossible=True)]


def _partition_constraint_from_field_filter(value: Any, comparable_types: set[str]) -> _PartitionConstraint | None:
    if value is None:
        return None

    if not isinstance(value, Mapping):
        comparable = _comparable_mongo_value(value)
        if comparable is None or not _comparable_type_matches_range(comparable, comparable_types):
            return None
        return _PartitionConstraint(values=(comparable,))

    query_ops = {"$eq", "$in", "$gt", "$gte", "$lt", "$lte"}
    if not any(op in value for op in query_ops):
        comparable = _comparable_mongo_value(value)
        if comparable is None or not _comparable_type_matches_range(comparable, comparable_types):
            return None
        return _PartitionConstraint(values=(comparable,))

    constraints: list[_PartitionConstraint] = []
    if "$eq" in value:
        comparable = _comparable_mongo_value(value["$eq"])
        if comparable is None or not _comparable_type_matches_range(comparable, comparable_types):
            return None
        constraints.append(_PartitionConstraint(values=(comparable,)))
    if "$in" in value:
        if not isinstance(value["$in"], list):
            return None
        if not value["$in"]:
            constraints.append(_PartitionConstraint(impossible=True))
        else:
            values: list[tuple[str, Any]] = []
            for item in value["$in"]:
                comparable = _comparable_mongo_value(item)
                if comparable is None or not _comparable_type_matches_range(comparable, comparable_types):
                    return None
                values.append(comparable)
            if values:
                constraints.append(_PartitionConstraint(values=tuple(values)))
    for op, inclusive, is_lower in (
        ("$gt", False, True),
        ("$gte", True, True),
        ("$lt", False, False),
        ("$lte", True, False),
    ):
        if op in value:
            comparable = _comparable_mongo_value(value[op])
            if comparable is None:
                return None
            if not _comparable_type_matches_range(comparable, comparable_types):
                continue
            bound = _Bound(comparable, inclusive)
            constraints.append(_PartitionConstraint(lower=bound) if is_lower else _PartitionConstraint(upper=bound))

    if not constraints:
        return None

    constraint = _PartitionConstraint()
    for next_constraint in constraints:
        constraint = _intersect_partition_constraints(constraint, next_constraint)
        if constraint.impossible:
            return constraint
    return constraint


def _partition_range_comparable_types(partition_ranges: list[MongoPartitionRange]) -> set[str]:
    comparable_types: set[str] = set()
    for lower, upper in partition_ranges:
        if lower is not None and (lower_value := _comparable_mongo_value(lower)) is not None:
            comparable_types.add(lower_value[0])
        if upper is not None and (upper_value := _comparable_mongo_value(upper)) is not None:
            comparable_types.add(upper_value[0])
    return comparable_types


def _comparable_type_matches_range(comparable: tuple[str, Any], comparable_types: set[str]) -> bool:
    return not comparable_types or comparable[0] in comparable_types


def _intersect_partition_constraints(left: _PartitionConstraint, right: _PartitionConstraint) -> _PartitionConstraint:
    if left.impossible or right.impossible:
        return _PartitionConstraint(impossible=True)

    lower = _max_lower_bound(left.lower, right.lower)
    upper = _min_upper_bound(left.upper, right.upper)
    values = _intersect_constraint_values(left.values, right.values)

    constraint = _PartitionConstraint(lower=lower, upper=upper, values=values)
    if values is not None:
        values = tuple(value for value in values if _value_satisfies_constraint(value, constraint))
        constraint = _PartitionConstraint(lower=lower, upper=upper, values=values, impossible=not values)
    elif lower is not None and upper is not None and _upper_is_before_or_equal_lower(upper, lower):
        constraint = _PartitionConstraint(impossible=True)
    return constraint


def _intersect_constraint_unions(
    left: list[_PartitionConstraint], right: list[_PartitionConstraint]
) -> list[_PartitionConstraint]:
    return [
        constraint
        for left_constraint in left
        for right_constraint in right
        if not (constraint := _intersect_partition_constraints(left_constraint, right_constraint)).impossible
    ]


def _intersect_constraint_values(
    left: tuple[tuple[str, Any], ...] | None, right: tuple[tuple[str, Any], ...] | None
) -> tuple[tuple[str, Any], ...] | None:
    if left is None:
        return right
    if right is None:
        return left
    right_set = set(right)
    return tuple(value for value in left if value in right_set)


def _max_lower_bound(left: _Bound | None, right: _Bound | None) -> _Bound | None:
    if left is None:
        return right
    if right is None:
        return left
    cmp = _compare_comparable_values(left.value, right.value)
    if cmp is None:
        return None
    if cmp > 0:
        return left
    if cmp < 0:
        return right
    return _Bound(left.value, left.inclusive and right.inclusive)


def _min_upper_bound(left: _Bound | None, right: _Bound | None) -> _Bound | None:
    if left is None:
        return right
    if right is None:
        return left
    cmp = _compare_comparable_values(left.value, right.value)
    if cmp is None:
        return None
    if cmp < 0:
        return left
    if cmp > 0:
        return right
    return _Bound(left.value, left.inclusive and right.inclusive)


def _task_range_overlaps_constraint(lower: Any | None, upper: Any | None, constraint: _PartitionConstraint) -> bool:
    if constraint.impossible:
        return False

    task_lower_value = _comparable_mongo_value(lower) if lower is not None else None
    task_upper_value = _comparable_mongo_value(upper) if upper is not None else None
    if (lower is not None and task_lower_value is None) or (upper is not None and task_upper_value is None):
        return True

    task_lower = _Bound(task_lower_value, True) if task_lower_value is not None else None
    task_upper = _Bound(task_upper_value, False) if task_upper_value is not None else None

    if constraint.values is not None:
        return any(_value_in_task_range(value, task_lower, task_upper) for value in constraint.values)

    if (
        task_upper is not None
        and constraint.lower is not None
        and _upper_is_before_or_equal_lower(task_upper, constraint.lower)
    ):
        return False
    return not (
        constraint.upper is not None
        and task_lower is not None
        and _upper_is_before_or_equal_lower(constraint.upper, task_lower)
    )


def _value_in_task_range(value: tuple[str, Any], lower: _Bound | None, upper: _Bound | None) -> bool:
    if lower is not None and not _value_satisfies_lower(value, lower):
        return False
    return upper is None or _value_satisfies_upper(value, upper)


def _value_satisfies_constraint(value: tuple[str, Any], constraint: _PartitionConstraint) -> bool:
    if constraint.lower is not None and not _value_satisfies_lower(value, constraint.lower):
        return False
    return constraint.upper is None or _value_satisfies_upper(value, constraint.upper)


def _value_satisfies_lower(value: tuple[str, Any], lower: _Bound) -> bool:
    cmp = _compare_comparable_values(value, lower.value)
    return True if cmp is None else cmp > 0 or (cmp == 0 and lower.inclusive)


def _value_satisfies_upper(value: tuple[str, Any], upper: _Bound) -> bool:
    cmp = _compare_comparable_values(value, upper.value)
    return True if cmp is None else cmp < 0 or (cmp == 0 and upper.inclusive)


def _upper_is_before_or_equal_lower(upper: _Bound, lower: _Bound) -> bool:
    cmp = _compare_comparable_values(upper.value, lower.value)
    if cmp is None:
        return False
    if cmp < 0:
        return True
    if cmp > 0:
        return False
    return not (upper.inclusive and lower.inclusive)


def _compare_comparable_values(left: tuple[str, Any], right: tuple[str, Any]) -> int | None:
    if left[0] != right[0]:
        return None
    if left[1] < right[1]:
        return -1
    if left[1] > right[1]:
        return 1
    return 0


def _comparable_mongo_value(value: Any) -> tuple[str, Any] | None:
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, datetime):
        return ("date", _datetime_to_epoch_millis(value))
    if isinstance(value, date):
        value = datetime.combine(value, time(), tzinfo=timezone.utc)
        return ("date", _datetime_to_epoch_millis(value))
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, int | float):
        return ("number", value)
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, Mapping):
        if set(value.keys()) == {"$oid"} and isinstance(value.get("$oid"), str):
            return _comparable_object_id_value(value["$oid"])
        if set(value.keys()) == {"$numberLong"} and isinstance(value.get("$numberLong"), str):
            return _parse_number(value["$numberLong"])
        if set(value.keys()) == {"$numberInt"} and isinstance(value.get("$numberInt"), str):
            return _parse_number(value["$numberInt"])
        if set(value.keys()) == {"$numberDouble"} and isinstance(value.get("$numberDouble"), str):
            return _parse_number(value["$numberDouble"])
        if set(value.keys()) == {"$date"}:
            return _comparable_date_value(value["$date"])
    return None


def _parse_number(value: str) -> tuple[str, int | float] | None:
    try:
        if any(part in value.lower() for part in (".", "e")):
            parsed = float(value)
            return ("number", parsed) if math.isfinite(parsed) else None
        parsed = int(value)
        return ("number", parsed) if _I64_MIN <= parsed <= _I64_MAX else None
    except ValueError:
        return None


def _comparable_object_id_value(value: str) -> tuple[str, str] | None:
    if len(value) != 24 or any(char not in _HEX_DIGITS for char in value):
        return None
    return ("oid", value.lower())


def _comparable_date_value(value: Any) -> tuple[str, int] | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return ("date", value) if _I64_MIN <= value <= _I64_MAX else None
    if (
        isinstance(value, Mapping)
        and set(value.keys()) == {"$numberLong"}
        and isinstance(value.get("$numberLong"), str)
    ):
        try:
            parsed = int(value["$numberLong"])
            return ("date", parsed) if _I64_MIN <= parsed <= _I64_MAX else None
        except ValueError:
            return None
    return None
