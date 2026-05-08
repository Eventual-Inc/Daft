from __future__ import annotations

import json
import pathlib
import random
import string
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from daft.context import get_context
from daft.datatype import DataType
from daft.dependencies import pa
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import ModuleType

    from daft.daft import IOConfig

LanceFragmentMetadata = Any


@dataclass
class _PartitionFieldSpec:
    field_id: str
    source_field: str
    source_id: int
    transform: str
    result_type: pa.DataType


def _generate_random_id() -> str:
    chars = string.ascii_lowercase + string.digits
    return "".join(random.choices(chars, k=16))


class LancePartitionedDataSink(DataSink[list[tuple[dict[str, Any], str, list[Any]]]]):
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

        self._table_uri = str(uri)
        self._mode = mode
        self._io_config = get_context().daft_planning_config.default_io_config if io_config is None else io_config
        self._kwargs = kwargs
        self._storage_options = io_config_to_storage_options(self._io_config, self._table_uri)
        self._partition_col_names = list(partition_cols)

        if isinstance(data_schema, Schema):
            self._pyarrow_schema = data_schema.to_pyarrow_schema()
        elif isinstance(data_schema, pa.Schema):
            self._pyarrow_schema = data_schema
        else:
            raise TypeError(f"Expected schema to be Schema or pa.Schema, got {type(data_schema)}")

        self._data_schema = pa.schema([f for f in self._pyarrow_schema if f.name not in set(self._partition_col_names)])

        self._partition_field_specs = self._build_partition_field_specs()

        self._spec_id = 1
        self._spec_version = f"v{self._spec_id}"
        self._existing_version: int = 0

        # Hierarchical namespace ID trie: maps a prefix tuple of partition
        # values (ordered by _partition_col_names) to its random 16-char ID.
        # e.g. (2024,) -> "abc123..." and (2024, "US") -> "def456..."
        self._namespace_ids: dict[tuple[Any, ...], str] = {}
        self._existing_read_versions: dict[str, int] = {}

        if mode == "append":
            self._load_existing_manifest(lance)
        elif mode == "create":
            self._check_no_existing_manifest(lance)

        self._schema = Schema._from_field_name_and_types(
            [
                ("num_partitions", DataType.int64()),
                ("num_fragments", DataType.int64()),
                ("version", DataType.int64()),
            ]
        )

    def _build_partition_field_specs(self) -> list[_PartitionFieldSpec]:
        specs = []
        for col_name in self._partition_col_names:
            idx = self._pyarrow_schema.get_field_index(col_name)
            if idx < 0:
                raise ValueError(f"Partition column '{col_name}' not found in schema")
            arrow_field = self._pyarrow_schema.field(idx)
            specs.append(
                _PartitionFieldSpec(
                    field_id=col_name,
                    source_field=col_name,
                    source_id=idx,
                    transform="identity",
                    result_type=arrow_field.type,
                )
            )
        return specs

    def _build_partition_spec_json(self) -> str:
        fields = []
        for spec in self._partition_field_specs:
            fields.append(
                {
                    "field_id": spec.field_id,
                    "source_ids": [spec.source_id],
                    "transform": {"type": spec.transform},
                    "result_type": {"type": str(spec.result_type)},
                }
            )
        return json.dumps({"id": 1, "fields": fields})

    def _build_namespace_schema_json(self) -> str:
        fields = []
        for field in self._pyarrow_schema:
            fields.append({"name": field.name, "type": str(field.type)})
        return json.dumps({"fields": fields})

    def _check_no_existing_manifest(self, lance_module: ModuleType) -> None:
        manifest_uri = self._table_uri.rstrip("/") + "/__manifest"
        manifest_exists = False
        try:
            lance_module.dataset(manifest_uri, storage_options=self._storage_options)
            manifest_exists = True
        except (ValueError, FileNotFoundError, OSError):
            pass
        if manifest_exists:
            raise ValueError(
                f"Cannot create partitioned Lance dataset: manifest already exists at {manifest_uri}. "
                "Use mode='append' or mode='overwrite' instead."
            )

    def _load_existing_manifest(self, lance_module: ModuleType) -> None:
        manifest_uri = self._table_uri.rstrip("/") + "/__manifest"
        try:
            manifest_ds = lance_module.dataset(manifest_uri, storage_options=self._storage_options)
        except (ValueError, FileNotFoundError, OSError):
            raise ValueError(
                f"Cannot append to partitioned Lance dataset: no manifest found at {manifest_uri}. "
                "Use mode='create' to create a new partitioned dataset."
            )
        manifest_table = manifest_ds.to_table()
        has_read_version = "read_version" in manifest_table.column_names

        for i in range(manifest_table.num_rows):
            object_id = manifest_table.column("object_id")[i].as_py()
            object_type = manifest_table.column("object_type")[i].as_py()
            parts = object_id.split("$")
            self._spec_version = parts[0]

            if object_type == "namespace" and len(parts) >= 2:
                # Reconstruct namespace ID trie from intermediate rows.
                # parts = ["v1", id1, id2, ...] for namespace at depth len(parts)-1
                depth = len(parts) - 1
                partition_prefix = tuple(
                    manifest_table.column(f"partition_field_{self._partition_col_names[j]}")[i].as_py()
                    for j in range(depth)
                )
                self._namespace_ids[partition_prefix] = parts[depth]

            elif object_type == "table":
                table_object_id = object_id
                if has_read_version:
                    rv = manifest_table.column("read_version")[i].as_py()
                    if rv is not None:
                        self._existing_read_versions[table_object_id] = rv

        self._existing_version = manifest_ds.version

    def name(self) -> str:
        return "Lance Partitioned Write"

    def schema(self) -> Schema:
        return self._schema

    def _table_object_id(self, partition_values: dict[str, Any]) -> str:
        segments = [self._spec_version]
        for i in range(len(self._partition_col_names)):
            prefix = tuple(partition_values[c] for c in self._partition_col_names[: i + 1])
            if prefix not in self._namespace_ids:
                self._namespace_ids[prefix] = _generate_random_id()
            segments.append(self._namespace_ids[prefix])
        segments.append("dataset")
        return "$".join(segments)

    def _physical_path(self, object_id: str) -> str:
        from daft.io.lance.utils import namespace_physical_path

        return namespace_physical_path(self._table_uri, object_id)

    def write(
        self, micropartitions: Iterator[MicroPartition]
    ) -> Iterator[WriteResult[list[tuple[dict[str, Any], str, list[Any]]]]]:
        lance = self._import_lance()

        for micropartition in micropartitions:
            arrow_table = micropartition.to_arrow()
            groups = self._group_by_partition(arrow_table)
            batch_results = []

            for partition_values, group_table in groups:
                table_oid = self._table_object_id(partition_values)
                dataset_path = self._physical_path(table_oid)

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
                batch_results.append((partition_values, table_oid, fragments))

            yield WriteResult(
                result=batch_results,
                bytes_written=arrow_table.nbytes,
                rows_written=arrow_table.num_rows,
            )

    def _group_by_partition(self, table: pa.Table) -> list[tuple[dict[str, Any], pa.Table]]:
        if not self._partition_col_names:
            return [({}, table)]

        partition_table = table.select(self._partition_col_names)

        unique_combos = partition_table.group_by(self._partition_col_names).aggregate([])
        groups = []
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

    def finalize(self, write_results: list[WriteResult[list[tuple[dict[str, Any], str, list[Any]]]]]) -> MicroPartition:
        lance = self._import_lance()

        # partition_map keyed by table object_id (e.g. "v1$abc$def$dataset")
        partition_map: dict[str, tuple[dict[str, Any], list[Any]]] = {}
        for wr in write_results:
            for partition_values, table_oid, fragments in wr.result:
                if table_oid not in partition_map:
                    partition_map[table_oid] = (partition_values, [])
                partition_map[table_oid][1].extend(fragments)

        partition_versions: dict[str, int] = {}
        for table_oid, (partition_values, fragments) in partition_map.items():
            dataset_path = self._physical_path(table_oid)

            existing_version: int | None = None
            if self._mode == "append":
                try:
                    existing_ds = lance.dataset(dataset_path, storage_options=self._storage_options)
                    existing_version = existing_ds.latest_version
                except (ValueError, FileNotFoundError, OSError):
                    pass

            if existing_version is not None:
                operation = lance.LanceOperation.Append(fragments)
                committed_ds = lance.LanceDataset.commit(
                    dataset_path,
                    operation,
                    read_version=existing_version,
                    storage_options=self._storage_options,
                )
                partition_versions[table_oid] = committed_ds.version
            else:
                operation = lance.LanceOperation.Overwrite(self._data_schema, fragments)
                committed_ds = lance.LanceDataset.commit(
                    dataset_path,
                    operation,
                    storage_options=self._storage_options,
                )
                partition_versions[table_oid] = committed_ds.version

        self._write_manifest(lance, partition_map, partition_versions)

        total_fragments = sum(len(frags) for _, frags in partition_map.values())
        return MicroPartition.from_pydict(
            {
                "num_partitions": pa.array([len(partition_map)], type=pa.int64()),
                "num_fragments": pa.array([total_fragments], type=pa.int64()),
                "version": pa.array([1], type=pa.int64()),
            }
        )

    def _write_manifest(
        self,
        lance_module: ModuleType,
        partition_map: dict[str, tuple[dict[str, Any], list[Any]]],
        partition_versions: dict[str, int],
    ) -> None:
        manifest_uri = self._table_uri.rstrip("/") + "/__manifest"

        # Collect all table-level entries: existing (from append) + new writes.
        # Keyed by table object_id.
        all_tables: dict[str, tuple[dict[str, Any], int | None]] = {}

        if self._mode == "append":
            try:
                existing_ds = lance_module.dataset(manifest_uri, storage_options=self._storage_options)
                existing_table = existing_ds.to_table()
                for i in range(existing_table.num_rows):
                    oid = existing_table.column("object_id")[i].as_py()
                    otype = existing_table.column("object_type")[i].as_py()
                    if otype != "table":
                        continue
                    pv: dict[str, Any] = {}
                    for spec in self._partition_field_specs:
                        col = f"partition_field_{spec.field_id}"
                        if col in existing_table.column_names:
                            pv[spec.source_field] = existing_table.column(col)[i].as_py()
                    rv = self._existing_read_versions.get(oid)
                    all_tables[oid] = (pv, rv)
            except (ValueError, FileNotFoundError, OSError):
                pass

        for table_oid, (partition_values, _) in partition_map.items():
            all_tables[table_oid] = (partition_values, partition_versions.get(table_oid))

        # Build the full manifest with namespace + table rows.
        columns: dict[str, list[Any]] = {
            "object_id": [],
            "object_type": [],
            "metadata": [],
            "read_version": [],
            "read_branch": [],
            "read_tag": [],
        }
        for spec in self._partition_field_specs:
            columns[f"partition_field_{spec.field_id}"] = []

        def _add_row(
            oid: str,
            otype: str,
            pv: dict[str, Any],
            rv: int | None = None,
        ) -> None:
            columns["object_id"].append(oid)
            columns["object_type"].append(otype)
            columns["metadata"].append("{}")
            columns["read_version"].append(rv)
            columns["read_branch"].append(None)
            columns["read_tag"].append(None)
            for spec in self._partition_field_specs:
                columns[f"partition_field_{spec.field_id}"].append(pv.get(spec.source_field))

        # Root namespace row: "v1" with all partition fields NULL
        _add_row(self._spec_version, "namespace", {})

        # Collect all intermediate namespaces from the namespace_ids trie.
        # Sort by depth so parents come before children.
        emitted_ns: set[str] = {self._spec_version}
        sorted_prefixes = sorted(self._namespace_ids.keys(), key=len)
        for prefix in sorted_prefixes:
            segments = [self._spec_version]
            for depth in range(len(prefix)):
                sub_prefix = prefix[: depth + 1]
                segments.append(self._namespace_ids[sub_prefix])
            ns_oid = "$".join(segments)
            if ns_oid in emitted_ns:
                continue
            emitted_ns.add(ns_oid)
            ns_pv: dict[str, Any] = {}
            for j, val in enumerate(prefix):
                ns_pv[self._partition_col_names[j]] = val
            _add_row(ns_oid, "namespace", ns_pv)

        # Table rows
        for table_oid, (partition_values, rv) in all_tables.items():
            _add_row(table_oid, "table", partition_values, rv)

        manifest_fields: list[pa.Field] = [
            pa.field("object_id", pa.utf8()),
            pa.field("object_type", pa.utf8()),
            pa.field("metadata", pa.utf8()),
            pa.field("read_version", pa.uint64()),
            pa.field("read_branch", pa.utf8()),
            pa.field("read_tag", pa.utf8()),
        ]
        for spec in self._partition_field_specs:
            manifest_fields.append(pa.field(f"partition_field_{spec.field_id}", spec.result_type))

        schema_metadata: dict[bytes | str, bytes | str] = {
            f"partition_spec_{self._spec_version}": self._build_partition_spec_json(),
            "schema": self._build_namespace_schema_json(),
        }
        manifest_schema = pa.schema(manifest_fields, metadata=schema_metadata)

        arrays = []
        for field in manifest_fields:
            arrays.append(pa.array(columns[field.name], type=field.type))

        manifest_table = pa.table(
            {field.name: arr for field, arr in zip(manifest_fields, arrays)},
            schema=manifest_schema,
        )

        lance_module.write_dataset(
            manifest_table,
            manifest_uri,
            mode="overwrite",
            storage_options=self._storage_options,
        )
