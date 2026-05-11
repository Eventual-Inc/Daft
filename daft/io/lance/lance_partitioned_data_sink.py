"""Partitioned write sink for Lance V2 directory namespaces with partitioning.

This implements the write side of:

- The Lance V2 Directory Namespace catalog spec (manifest table with
  ``object_id`` / ``object_type`` / ``location`` / ``metadata`` /
  ``base_objects`` columns, hash-prefixed ``<hash>_<object_id>`` directories
  for the leaf table data, root-namespace properties stored in the
  ``__manifest`` table's schema metadata).
- The Lance Partitioning spec (``partition_spec_v<N>`` JSON, ``schema`` JSON
  describing the namespace schema with ``lance:field_id`` per field,
  ``partition_field_{field_id}`` columns extending the manifest, hierarchical
  random 16-char base36 namespace ids in object_ids, ``dataset`` leaf naming).

Initial scope: identity transform only, single partition spec (``v1``).
``read_version`` is populated on every table row after a successful commit;
``read_branch`` / ``read_tag`` are always NULL.
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Any, Literal

from daft.context import get_context
from daft.datatype import DataType
from daft.dependencies import pa
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

from .lance_namespace import (
    COL_BASE_OBJECTS,
    COL_LOCATION,
    COL_METADATA,
    COL_OBJECT_ID,
    COL_OBJECT_TYPE,
    COL_READ_BRANCH,
    COL_READ_TAG,
    COL_READ_VERSION,
    METADATA_KEY_SCHEMA,
    OBJECT_TYPE_NAMESPACE,
    OBJECT_TYPE_TABLE,
    PARTITION_FIELD_PREFIX,
    PartitionFieldSpec,
    make_location,
    make_namespace_schema_json,
    make_object_id,
    make_partition_spec_json,
    manifest_arrow_schema,
    parse_object_id,
    random_namespace_id,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType

    from daft.daft import IOConfig


# write() yields a list of (partition_values, table_object_id, fragments) triples per micropartition.
_WriteResultPayload = list[tuple[dict[str, Any], str, list[Any]]]

# Always v1 for the initial implementation — no partition spec evolution yet.
_SPEC_ID = 1
_SPEC_VERSION = f"v{_SPEC_ID}"


class LancePartitionedDataSink(DataSink[_WriteResultPayload]):
    """DataSink for writing a Daft DataFrame to a partitioned Lance namespace.

    The sink:

    1. Groups each incoming micropartition by partition column values.
    2. Writes a Lance fragment for each (group, partition) pair into the
       partition table at ``<root>/<hash>_<object_id>/``.
    3. Commits accumulated fragments per partition table.
    4. Rewrites the ``__manifest`` table with the full namespace + table row
       inventory and the partitioning metadata.
    """

    def _import_lance(self) -> ModuleType:
        try:
            import lance

            return lance
        except ImportError:
            raise ImportError("lance is not installed. Please install lance using `pip install daft[lance]`")

    def __init__(
        self,
        uri: str | pathlib.Path,
        data_schema: Schema | pa.Schema,
        mode: Literal["create", "append", "overwrite"],
        io_config: IOConfig | None = None,
        partition_cols: list[str] | None = None,
        **kwargs: Any,
    ) -> None:
        from daft.io.object_store_options import io_config_to_storage_options

        lance = self._import_lance()

        if not isinstance(uri, (str, pathlib.Path)):
            raise TypeError(f"Expected URI to be str or pathlib.Path, got {type(uri)}")
        if not partition_cols:
            raise ValueError("partition_cols must be a non-empty list of column names")
        if mode not in {"create", "append", "overwrite"}:
            raise ValueError(f"Unsupported mode for partitioned writes: {mode!r}")

        self._table_uri = str(uri).rstrip("/")
        self._mode = mode
        self._io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
        self._kwargs = dict(kwargs)
        # Strip merge-mode kwargs that don't apply here.
        self._kwargs.pop("left_on", None)
        self._kwargs.pop("right_on", None)
        self._storage_options = io_config_to_storage_options(self._io_config, self._table_uri)
        self._partition_col_names = list(partition_cols)

        if isinstance(data_schema, Schema):
            self._namespace_pa_schema = data_schema.to_pyarrow_schema()
        elif isinstance(data_schema, pa.Schema):
            self._namespace_pa_schema = data_schema
        else:
            raise TypeError(f"Expected schema to be Schema or pa.Schema, got {type(data_schema)}")

        # Validate partition columns exist in the schema and assign field ids.
        partition_set = set(self._partition_col_names)
        for col in self._partition_col_names:
            if col not in self._namespace_pa_schema.names:
                raise ValueError(f"Partition column {col!r} not found in schema")

        # Field IDs follow position in the user schema and are immutable.
        self._field_ids: dict[str, int] = {f.name: i for i, f in enumerate(self._namespace_pa_schema)}

        # Build partition field specs (identity transform only in v1).
        self._partition_field_specs: list[PartitionFieldSpec] = [
            PartitionFieldSpec(
                field_id=col,
                source_field_name=col,
                source_id=self._field_ids[col],
                transform="identity",
                result_type=self._namespace_pa_schema.field(col).type,
            )
            for col in self._partition_col_names
        ]

        # Leaf Lance datasets store everything except partition columns.
        self._leaf_pa_schema = pa.schema([f for f in self._namespace_pa_schema if f.name not in partition_set])

        # State that may be reused across writes when appending.
        # prefix-of-partition-values -> 16-char base36 namespace id
        self._namespace_ids: dict[tuple[Any, ...], str] = {}
        # table object_id -> physical "location" (relative dir name <hash>_<object_id>)
        self._locations: dict[str, str] = {}
        # table object_id -> previously-committed Lance dataset version (for re-emitting unchanged rows)
        self._existing_read_versions: dict[str, int] = {}
        # table object_id -> partition values dict (so we can re-emit on append even if no new data hits)
        self._existing_table_pvs: dict[str, dict[str, Any]] = {}

        if mode == "append":
            self._load_existing_manifest(lance)
        elif mode == "create":
            self._check_no_existing_manifest(lance)
        # overwrite: ignore existing state entirely.

        self._schema = Schema._from_field_name_and_types(
            [
                ("num_partitions", DataType.int64()),
                ("num_fragments", DataType.int64()),
                ("version", DataType.int64()),
            ]
        )

    # ---- DataSink protocol ------------------------------------------------

    def name(self) -> str:
        return "Lance Partitioned Write"

    def schema(self) -> Schema:
        return self._schema

    def write(self, micropartitions: Iterator[MicroPartition]) -> Iterator[WriteResult[_WriteResultPayload]]:
        lance = self._import_lance()

        for micropartition in micropartitions:
            arrow_table = micropartition.to_arrow()
            if arrow_table.num_rows == 0:
                continue
            groups = self._group_by_partition(arrow_table)

            batch_results: _WriteResultPayload = []
            for partition_values, group_table in groups:
                table_oid = self._table_object_id(partition_values)
                location = self._location_for(table_oid)
                dataset_path = f"{self._table_uri}/{location}"

                # Drop partition columns — they're inherited from manifest.
                write_table = group_table.drop_columns(
                    [c for c in self._partition_col_names if c in group_table.column_names]
                )

                fragments = lance.fragment.write_fragments(
                    write_table,
                    dataset_uri=dataset_path,
                    mode="append",
                    storage_options=self._storage_options,
                    **self._kwargs,
                )
                batch_results.append((partition_values, table_oid, list(fragments)))

            yield WriteResult(
                result=batch_results,
                bytes_written=arrow_table.nbytes,
                rows_written=arrow_table.num_rows,
            )

    def finalize(self, write_results: list[WriteResult[_WriteResultPayload]]) -> MicroPartition:
        lance = self._import_lance()

        # Collapse fragments by table object_id across all WriteResults.
        partition_map: dict[str, tuple[dict[str, Any], list[Any]]] = {}
        for wr in write_results:
            for partition_values, table_oid, fragments in wr.result:
                _, frags = partition_map.setdefault(table_oid, (partition_values, []))
                frags.extend(fragments)

        # Commit each affected leaf Lance dataset and capture its committed version.
        partition_versions: dict[str, int] = {}
        for table_oid, (_, fragments) in partition_map.items():
            location = self._location_for(table_oid)
            dataset_path = f"{self._table_uri}/{location}"
            partition_versions[table_oid] = self._commit_leaf(lance, dataset_path, fragments)

        self._write_manifest(lance, partition_map, partition_versions)

        total_fragments = sum(len(frags) for _, frags in partition_map.values())
        manifest_version = self._latest_manifest_version(lance)
        return MicroPartition.from_pydict(
            {
                "num_partitions": pa.array([len(partition_map)], type=pa.int64()),
                "num_fragments": pa.array([total_fragments], type=pa.int64()),
                "version": pa.array([manifest_version], type=pa.int64()),
            }
        )

    # ---- Internals --------------------------------------------------------

    def _check_no_existing_manifest(self, lance_module: ModuleType) -> None:
        manifest_uri = f"{self._table_uri}/__manifest"
        try:
            lance_module.dataset(manifest_uri, storage_options=self._storage_options)
        except (ValueError, FileNotFoundError, OSError):
            return
        raise ValueError(
            f"Cannot create partitioned Lance namespace: manifest already exists at {manifest_uri}. "
            "Use mode='append' or mode='overwrite' instead."
        )

    def _load_existing_manifest(self, lance_module: ModuleType) -> None:
        """For append mode: reconstruct namespace ids, locations, and existing tables."""
        manifest_uri = f"{self._table_uri}/__manifest"
        try:
            manifest_ds = lance_module.dataset(manifest_uri, storage_options=self._storage_options)
        except (ValueError, FileNotFoundError, OSError):
            raise ValueError(
                f"Cannot append to partitioned Lance namespace: no manifest at {manifest_uri}. Use mode='create' first."
            )

        manifest_table = manifest_ds.to_table()

        for i in range(manifest_table.num_rows):
            oid = manifest_table.column(COL_OBJECT_ID)[i].as_py()
            otype = manifest_table.column(COL_OBJECT_TYPE)[i].as_py()
            spec_version, ns_ids, _ = parse_object_id(oid)

            if spec_version != _SPEC_VERSION:
                # We only know how to round-trip v1 in this implementation.
                continue

            if otype == OBJECT_TYPE_NAMESPACE and ns_ids:
                # Reconstruct the prefix -> id mapping from this namespace row's
                # partition_field_* columns truncated to its depth.
                depth = len(ns_ids)
                prefix: list[Any] = []
                for col_name in self._partition_col_names[:depth]:
                    pf_col = f"{PARTITION_FIELD_PREFIX}{col_name}"
                    if pf_col in manifest_table.column_names:
                        prefix.append(manifest_table.column(pf_col)[i].as_py())
                    else:
                        prefix.append(None)
                # The full prefix tuple at depth `depth` maps to ns_ids[-1].
                self._namespace_ids[tuple(prefix)] = ns_ids[-1]

            elif otype == OBJECT_TYPE_TABLE:
                location = manifest_table.column(COL_LOCATION)[i].as_py()
                if location is not None:
                    self._locations[oid] = location

                pv: dict[str, Any] = {}
                for col_name in self._partition_col_names:
                    pf_col = f"{PARTITION_FIELD_PREFIX}{col_name}"
                    if pf_col in manifest_table.column_names:
                        pv[col_name] = manifest_table.column(pf_col)[i].as_py()
                self._existing_table_pvs[oid] = pv

                # Also re-populate the namespace prefix map from the table row.
                # E.g. v1$abc$def$dataset → prefix (pv[c0],) → abc; (pv[c0], pv[c1]) → def
                for depth in range(1, len(ns_ids) + 1):
                    prefix_tuple = tuple(pv[c] for c in self._partition_col_names[:depth])
                    self._namespace_ids.setdefault(prefix_tuple, ns_ids[depth - 1])

                rv = manifest_table.column(COL_READ_VERSION)[i].as_py()
                if rv is not None:
                    self._existing_read_versions[oid] = int(rv)

    def _table_object_id(self, partition_values: dict[str, Any]) -> str:
        ns_ids: list[str] = []
        for i in range(len(self._partition_col_names)):
            prefix = tuple(partition_values[c] for c in self._partition_col_names[: i + 1])
            if prefix not in self._namespace_ids:
                self._namespace_ids[prefix] = random_namespace_id()
            ns_ids.append(self._namespace_ids[prefix])
        return make_object_id(_SPEC_VERSION, ns_ids, is_table=True)

    def _location_for(self, table_oid: str) -> str:
        loc = self._locations.get(table_oid)
        if loc is None:
            loc = make_location(table_oid)
            self._locations[table_oid] = loc
        return loc

    def _group_by_partition(self, table: pa.Table) -> list[tuple[dict[str, Any], pa.Table]]:
        # Compute unique partition value tuples.
        partition_only = table.select(self._partition_col_names)
        unique_combos = partition_only.group_by(self._partition_col_names).aggregate([])

        groups: list[tuple[dict[str, Any], pa.Table]] = []
        for i in range(unique_combos.num_rows):
            mask: Any = None
            pv: dict[str, Any] = {}
            for col_name in self._partition_col_names:
                val = unique_combos.column(col_name)[i]
                pv[col_name] = val.as_py()
                col_mask = pa.compute.equal(table.column(col_name), val)
                mask = col_mask if mask is None else pa.compute.and_(mask, col_mask)
            filtered = table.filter(mask)
            groups.append((pv, filtered))
        return groups

    def _commit_leaf(self, lance_module: ModuleType, dataset_path: str, fragments: list[Any]) -> int:
        """Commit fragments into the leaf Lance dataset and return its new version.

        Uses ``LanceOperation.Append`` if the dataset already exists on disk
        (regardless of write mode — the same partition value can be touched
        twice in a single ``mode="create"`` job via multiple micropartitions),
        otherwise ``LanceOperation.Overwrite`` to create the dataset.
        """
        existing_version: int | None = None
        try:
            existing_ds = lance_module.dataset(dataset_path, storage_options=self._storage_options)
            existing_version = existing_ds.latest_version
        except (ValueError, FileNotFoundError, OSError):
            pass

        if existing_version is not None:
            op = lance_module.LanceOperation.Append(fragments)
            committed = lance_module.LanceDataset.commit(
                dataset_path,
                op,
                read_version=existing_version,
                storage_options=self._storage_options,
            )
        else:
            op = lance_module.LanceOperation.Overwrite(self._leaf_pa_schema, fragments)
            committed = lance_module.LanceDataset.commit(
                dataset_path,
                op,
                storage_options=self._storage_options,
            )
        return committed.version

    def _write_manifest(
        self,
        lance_module: ModuleType,
        partition_map: dict[str, tuple[dict[str, Any], list[Any]]],
        partition_versions: dict[str, int],
    ) -> None:
        # Merge newly-written tables with previously-known tables (append path).
        all_tables: dict[str, tuple[dict[str, Any], int | None]] = {}
        for oid, pv in self._existing_table_pvs.items():
            all_tables[oid] = (pv, self._existing_read_versions.get(oid))
        for oid, (pv, _) in partition_map.items():
            # Newly committed tables use their fresh version; existing ones get bumped if we touched them.
            all_tables[oid] = (pv, partition_versions.get(oid, self._existing_read_versions.get(oid)))

        rows = self._build_manifest_rows(all_tables)
        schema_metadata = self._build_schema_metadata()
        manifest_pa_schema = manifest_arrow_schema(self._partition_field_specs, schema_metadata)

        # Assemble arrays column-by-column from the row dicts.
        columns: dict[str, list[Any]] = {field.name: [] for field in manifest_pa_schema}
        for row in rows:
            for field in manifest_pa_schema:
                columns[field.name].append(row.get(field.name))

        arrays = []
        for field in manifest_pa_schema:
            arrays.append(pa.array(columns[field.name], type=field.type))
        manifest_table = pa.Table.from_arrays(arrays, schema=manifest_pa_schema)

        manifest_uri = f"{self._table_uri}/__manifest"
        lance_module.write_dataset(
            manifest_table,
            manifest_uri,
            mode="overwrite",
            storage_options=self._storage_options,
        )

    def _build_manifest_rows(self, all_tables: dict[str, tuple[dict[str, Any], int | None]]) -> list[dict[str, Any]]:
        """Assemble the full list of manifest rows (root + intermediate + leaf).

        Order: parents before children. The spec doesn't require row ordering,
        but emitting in dependency order makes reads simpler and the file
        easier to inspect.
        """
        # The intermediate-namespace inventory derives from the union of
        # _namespace_ids (live writes + reloaded append state) and the prefixes
        # implied by every known table's partition values.
        prefix_to_id: dict[tuple[Any, ...], str] = dict(self._namespace_ids)
        for oid, (pv, _) in all_tables.items():
            _, ns_ids, is_table = parse_object_id(oid)
            assert is_table
            for depth in range(1, len(ns_ids) + 1):
                prefix_tuple = tuple(pv[c] for c in self._partition_col_names[:depth])
                prefix_to_id.setdefault(prefix_tuple, ns_ids[depth - 1])

        rows: list[dict[str, Any]] = []

        # 1. Root namespace row (v1).
        rows.append(self._row(make_object_id(_SPEC_VERSION, [], is_table=False), OBJECT_TYPE_NAMESPACE, {}))

        # 2. Intermediate namespace rows, sorted by depth so parents precede children.
        for prefix in sorted(prefix_to_id.keys(), key=lambda p: (len(p), p)):
            ns_segments: list[str] = []
            for depth in range(1, len(prefix) + 1):
                ns_segments.append(prefix_to_id[prefix[:depth]])
            oid = make_object_id(_SPEC_VERSION, ns_segments, is_table=False)
            ns_pv = {c: prefix[i] for i, c in enumerate(self._partition_col_names[: len(prefix)])}
            rows.append(self._row(oid, OBJECT_TYPE_NAMESPACE, ns_pv))

        # 3. Table rows.
        for oid, (pv, rv) in all_tables.items():
            row = self._row(oid, OBJECT_TYPE_TABLE, pv, read_version=rv, location=self._locations.get(oid))
            rows.append(row)

        return rows

    def _row(
        self,
        object_id: str,
        object_type: str,
        partition_values: dict[str, Any],
        *,
        location: str | None = None,
        read_version: int | None = None,
    ) -> dict[str, Any]:
        row: dict[str, Any] = {
            COL_OBJECT_ID: object_id,
            COL_OBJECT_TYPE: object_type,
            COL_LOCATION: location,
            COL_METADATA: "{}",
            COL_BASE_OBJECTS: None,
            COL_READ_VERSION: read_version,
            COL_READ_BRANCH: None,
            COL_READ_TAG: None,
        }
        for spec in self._partition_field_specs:
            row[f"{PARTITION_FIELD_PREFIX}{spec.field_id}"] = partition_values.get(spec.field_id)
        return row

    def _build_schema_metadata(self) -> dict[str, str]:
        return {
            f"partition_spec_{_SPEC_VERSION}": make_partition_spec_json(_SPEC_ID, self._partition_field_specs),
            METADATA_KEY_SCHEMA: make_namespace_schema_json(self._namespace_pa_schema, self._field_ids),
        }

    def _latest_manifest_version(self, lance_module: ModuleType) -> int:
        try:
            ds = lance_module.dataset(f"{self._table_uri}/__manifest", storage_options=self._storage_options)
        except (ValueError, FileNotFoundError, OSError):
            return 0
        return int(ds.latest_version)
