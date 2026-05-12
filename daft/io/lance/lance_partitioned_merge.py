"""Fast-path column merge into a partitioned Lance namespace.

Adds new columns to an existing partitioned Lance namespace without rewriting
the base data files. For each `(partition_values, fragment_id)` group, the
new column values are written as a raw `.lance` data file and the resulting
path is stitched into the fragment's metadata as an additional data file.
A `LanceOperation.Merge` then commits the new fragment list against each
touched leaf; the `__manifest` is finally rewritten with the bumped
`read_version` per leaf and a `schema` JSON that includes the new columns.

Requirements on the input DataFrame:

- includes ``_rowaddr`` and ``fragment_id`` columns;
- includes every partition column declared on the target namespace;
- represents a positional-aligned read of each target partition (the input's
  row count per ``(partition_values, fragment_id)`` group must equal the
  matching leaf fragment's physical row count).

Algorithm cribbed from daft-lance's ``FastPathFragmentWriter`` — the same
approach used for non-partitioned Lance fast-path merge. We inline it here
because ``daft-lance`` depends on ``daft`` and so cannot be a Daft
dependency.
"""

from __future__ import annotations

import json
import os
import uuid
from collections import defaultdict
from typing import TYPE_CHECKING, Any

import daft
import daft.pickle
from daft.datatype import DataType
from daft.dependencies import pa
from daft.udf import cls as daft_cls
from daft.udf import method

from .lance_namespace import (
    COL_LOCATION,
    COL_OBJECT_ID,
    COL_OBJECT_TYPE,
    COL_READ_VERSION,
    LANCE_FIELD_ID_KEY,
    METADATA_KEY_SCHEMA,
    OBJECT_TYPE_TABLE,
    PARTITION_FIELD_PREFIX,
    PartitionFieldSpec,
    get_schema_metadata_value,
    latest_partition_spec_key,
    make_namespace_schema_json,
    make_partition_spec_json,
    manifest_arrow_schema,
    parse_namespace_schema_json,
    parse_partition_spec_json,
)

if TYPE_CHECKING:
    import pathlib

    import lance


_FAST_PATH_RETURN_DTYPE = DataType.struct(
    {
        "fragment_meta": DataType.binary(),
        "schema": DataType.binary(),
        "leaf_uri": DataType.string(),
    }
)


@daft_cls
class _PartitionedFastPathWriter:
    """Per-(partition, fragment) UDF that writes a raw `.lance` file.

    Each `map_groups` call receives one (partition_values, fragment_id) group.
    The new column values are sorted by `_rowaddr` to restore positional order,
    written to a fresh `.lance` file under the leaf's `data/` directory, and
    stitched into a copy of the existing fragment metadata.
    """

    def __init__(
        self,
        partition_to_uri: dict[tuple[Any, ...], str],
        partition_col_names: list[str],
        new_column_names: list[str],
        new_column_field_ids: dict[str, int],
        storage_options: dict[str, str] | None = None,
    ):
        self.partition_to_uri = partition_to_uri
        self.partition_col_names = partition_col_names
        self.new_column_names = new_column_names
        self.new_column_field_ids = new_column_field_ids
        self.storage_options = storage_options

    @method.batch(return_dtype=_FAST_PATH_RETURN_DTYPE)
    def __call__(self, *cols: Any) -> list[dict[str, Any]]:
        import lance
        from lance.file import LanceFileWriter
        from lance.fragment import FragmentMetadata

        if len(cols) == 0:
            return []

        # Argument layout: [<new_col_1..n>, _rowaddr, fragment_id, <part_col_1..k>]
        n_new = len(self.new_column_names)
        data_cols = cols[:n_new]
        rowaddr_col = cols[n_new]
        fragment_id_col = cols[n_new + 1]
        partition_value_cols = cols[n_new + 2 :]

        # Extract partition values from the first row of this group (all rows
        # in the group share the same partition values + fragment_id).
        partition_values: list[Any] = []
        for pc in partition_value_cols:
            vals = pc.to_pylist() if hasattr(pc, "to_pylist") else list(pc)
            if len(vals) == 0:
                return []
            partition_values.append(vals[0])
        partition_key = tuple(partition_values)

        leaf_uri = self.partition_to_uri.get(partition_key)
        if leaf_uri is None:
            raise ValueError(
                f"No leaf table for partition "
                f"{dict(zip(self.partition_col_names, partition_values))}. "
                f"Known partitions: {len(self.partition_to_uri)}"
            )

        frag_ids = fragment_id_col.to_pylist() if hasattr(fragment_id_col, "to_pylist") else list(fragment_id_col)
        if len(frag_ids) == 0:
            return []
        frag_id = frag_ids[0]

        rowaddrs = rowaddr_col.to_pylist() if hasattr(rowaddr_col, "to_pylist") else list(rowaddr_col)

        # Build the new-column table; sort by _rowaddr to align with fragment
        # row order. Pin ``lance:field_id`` Arrow metadata on each new column
        # using the namespace-wide ids the planner computed, so the stored
        # leaf data agrees with the namespace's schema declaration.
        arrays = []
        for s in data_cols:
            arr = pa.array(s.to_pylist() if hasattr(s, "to_pylist") else list(s))
            arrays.append(arr)
        tbl_schema = pa.schema(
            [
                pa.field(
                    name,
                    arr.type,
                    metadata={LANCE_FIELD_ID_KEY: str(self.new_column_field_ids[name])},
                )
                for name, arr in zip(self.new_column_names, arrays)
            ]
        )
        tbl = pa.Table.from_arrays(arrays, schema=tbl_schema)

        sort_indices = pa.compute.sort_indices(
            pa.table({"_rowaddr": pa.array(rowaddrs, type=pa.uint64())}),
            sort_keys=[("_rowaddr", "ascending")],
        )
        tbl = tbl.take(sort_indices)

        # Write the new columns as a raw .lance file under the leaf's data/ dir.
        filename = uuid.uuid4().hex + ".lance"
        filepath = os.path.join(leaf_uri, "data", filename)
        with LanceFileWriter(filepath, tbl.schema, storage_options=self.storage_options) as writer:
            for b in tbl.to_batches():
                writer.write_batch(b)
        file_size = os.path.getsize(filepath)

        # Stitch the new file into fragment metadata. The ``fields`` array in
        # a fragment-file entry uses *Lance-internal* field ids (what Lance
        # needs to resolve the file against its lance_schema), NOT the
        # namespace-wide ``lance:field_id`` from the spec. Those two id
        # systems can diverge per-leaf and that's fine — Lance preserves the
        # spec-level ``lance:field_id`` separately in Arrow field metadata
        # (pinned on ``tbl_schema`` above), which is what spec-strict readers
        # resolve through.
        leaf_ds = lance.dataset(leaf_uri, storage_options=self.storage_options)
        fragment = leaf_ds.get_fragment(frag_id)
        next_fid = max(f.id() for f in leaf_ds.lance_schema.fields()) + 1

        meta = dict(fragment.metadata.to_json())
        meta["files"] = list(meta["files"]) + [
            {
                "path": filename,
                "fields": list(range(next_fid, next_fid + len(self.new_column_names))),
                "column_indices": list(range(len(self.new_column_names))),
                "file_major_version": 2,
                "file_minor_version": 0,
                "file_size_bytes": file_size,
                "base_id": None,
            }
        ]
        new_frag_meta = FragmentMetadata.from_json(json.dumps(meta))

        new_schema = leaf_ds.schema
        for col_name in self.new_column_names:
            col_idx = tbl.schema.get_field_index(col_name)
            new_schema = new_schema.append(
                pa.field(
                    col_name,
                    tbl.schema.field(col_idx).type,
                    metadata={LANCE_FIELD_ID_KEY: str(self.new_column_field_ids[col_name])},
                )
            )

        return [
            {
                "fragment_meta": daft.pickle.dumps(new_frag_meta),
                "schema": daft.pickle.dumps(new_schema),
                "leaf_uri": leaf_uri,
            }
        ]


def _read_manifest_inventory(
    namespace_uri: str,
    partition_cols: list[str],
    storage_options: dict[str, str] | None,
) -> tuple[
    lance.LanceDataset,
    pa.Table,
    dict[tuple[Any, ...], str],
    dict[tuple[Any, ...], str],
    dict[tuple[Any, ...], int],
    pa.Schema,
    list[PartitionFieldSpec],
    int,
]:
    """Open the namespace's __manifest table and extract everything we need.

    Returns:
        (manifest_ds, manifest_table, partition_to_uri, partition_to_oid,
         partition_to_read_version, namespace_schema, partition_field_specs,
         spec_id)
    """
    import lance

    manifest_uri = namespace_uri.rstrip("/") + "/__manifest"
    manifest_ds = lance.dataset(manifest_uri, storage_options=storage_options)
    manifest_table = manifest_ds.to_table()

    schema_metadata = manifest_ds.schema.metadata or {}
    latest = latest_partition_spec_key(schema_metadata)
    if latest is None:
        raise ValueError(
            f"No partition_spec_v<N> key in {manifest_uri} metadata. Is this a partitioned Lance namespace?"
        )
    spec_id, spec_key = latest
    spec_payload = get_schema_metadata_value(schema_metadata, spec_key)
    if spec_payload is None:
        raise ValueError(f"Missing value for {spec_key!r} in manifest metadata")
    partition_spec = parse_partition_spec_json(spec_payload)

    schema_payload = get_schema_metadata_value(schema_metadata, METADATA_KEY_SCHEMA)
    if schema_payload is None:
        raise ValueError(
            f"Missing `schema` metadata on {manifest_uri}; cannot perform merge without the namespace schema."
        )
    namespace_schema = parse_namespace_schema_json(schema_payload)

    # Resolve partition_field_specs (preserving field_id assignments from the schema).
    field_ids: dict[str, int] = {}
    for f in namespace_schema:
        md = f.metadata or {}
        raw = md.get(LANCE_FIELD_ID_KEY.encode(), md.get(LANCE_FIELD_ID_KEY))
        if raw is None:
            continue
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        field_ids[f.name] = int(raw)

    spec_field_specs: list[PartitionFieldSpec] = []
    for fdef in partition_spec.get("fields", []):
        fid = fdef["field_id"]
        source_ids = fdef.get("source_ids", [])
        source_id = int(source_ids[0]) if source_ids else field_ids.get(fid, 0)
        # Find the source column's Arrow type from the namespace schema.
        source_name = fid
        try:
            source_name = next(name for name, idx in field_ids.items() if idx == source_id)
        except StopIteration:
            pass
        transform = fdef.get("transform") or {"type": "identity"}
        transform_name = transform.get("type") if isinstance(transform, dict) else transform
        result_type_field = namespace_schema.field(fid) if fid in namespace_schema.names else None
        result_type = result_type_field.type if result_type_field is not None else pa.utf8()
        spec_field_specs.append(
            PartitionFieldSpec(
                field_id=fid,
                source_field_name=source_name,
                source_id=source_id,
                transform=transform_name or "identity",
                result_type=result_type,
            )
        )

    # Build partition_values -> (leaf_uri, object_id, read_version) maps.
    partition_to_uri: dict[tuple[Any, ...], str] = {}
    partition_to_oid: dict[tuple[Any, ...], str] = {}
    partition_to_read_version: dict[tuple[Any, ...], int] = {}

    has_object_type = COL_OBJECT_TYPE in manifest_table.column_names
    has_read_version = COL_READ_VERSION in manifest_table.column_names

    for i in range(manifest_table.num_rows):
        if has_object_type and manifest_table.column(COL_OBJECT_TYPE)[i].as_py() != OBJECT_TYPE_TABLE:
            continue
        loc = manifest_table.column(COL_LOCATION)[i].as_py()
        if loc is None:
            continue
        key = tuple(manifest_table.column(f"{PARTITION_FIELD_PREFIX}{c}")[i].as_py() for c in partition_cols)
        leaf_uri = namespace_uri.rstrip("/") + "/" + loc
        partition_to_uri[key] = leaf_uri
        partition_to_oid[key] = manifest_table.column(COL_OBJECT_ID)[i].as_py()
        if has_read_version:
            rv = manifest_table.column(COL_READ_VERSION)[i].as_py()
            if rv is not None:
                partition_to_read_version[key] = int(rv)

    return (
        manifest_ds,
        manifest_table,
        partition_to_uri,
        partition_to_oid,
        partition_to_read_version,
        namespace_schema,
        spec_field_specs,
        spec_id,
    )


def merge_columns_partitioned(
    df: daft.DataFrame,
    namespace_uri: str | pathlib.Path,
    partition_cols: list[str],
    *,
    storage_options: dict[str, str] | None = None,
) -> daft.DataFrame:
    """Fast-path partitioned column merge.

    The input DataFrame must include ``_rowaddr``, ``fragment_id``, and every
    partition column. The DataFrame represents a positional-aligned read of
    the touched partitions; existing columns are preserved unchanged, new
    columns are appended to each touched leaf via raw `.lance` file stitching.
    """
    import lance

    namespace_uri = str(namespace_uri).rstrip("/")

    # Input validation.
    required = {"_rowaddr", "fragment_id"} | set(partition_cols)
    missing = required - set(df.column_names)
    if missing:
        hints = []
        if "fragment_id" in missing:
            hints.append("read with `include_fragment_id=True`")
        if "_rowaddr" in missing:
            hints.append("read with `default_scan_options={'with_row_address': True}`")
        hint = "; ".join(hints)
        raise ValueError(
            f"DataFrame must contain {sorted(required)} for partitioned merge; missing {sorted(missing)}. "
            + (hint + "." if hint else "")
        )

    (
        _,
        _,
        partition_to_uri,
        partition_to_oid,
        partition_to_read_version,
        namespace_schema,
        partition_field_specs,
        spec_id,
    ) = _read_manifest_inventory(namespace_uri, partition_cols, storage_options)

    if not partition_to_uri:
        raise ValueError(f"No table rows in {namespace_uri}/__manifest; nothing to merge into.")

    # Detect new columns: anything in the DF not already in the namespace
    # schema, excluding metadata columns.
    existing = set(namespace_schema.names)
    metadata_cols = {"_rowaddr", "fragment_id"} | set(partition_cols)
    new_cols = [c for c in df.column_names if c not in existing and c not in metadata_cols]
    if not new_cols:
        raise ValueError(
            "DataFrame has no new columns to merge into the namespace. "
            "The merge path appends new columns; for plain re-writes use mode='append'."
        )

    # Validate every partition value in the input maps to a known leaf.
    input_partition_values = df.select(*partition_cols).distinct().to_pydict()
    n = len(next(iter(input_partition_values.values())))
    for i in range(n):
        key = tuple(input_partition_values[c][i] for c in partition_cols)
        if key not in partition_to_uri:
            raise ValueError(
                f"Input partition {dict(zip(partition_cols, key))} has no matching table in the manifest; "
                f"known partitions: {len(partition_to_uri)}."
            )

    # Compute the namespace-wide ``lance:field_id`` for each new column UP
    # FRONT. The per-(partition, fragment) UDF needs to pin these ids on the
    # raw .lance file it writes — if instead each leaf computes its own next
    # field id from ``lance_schema.fields()``, siblings under one parent can
    # disagree about the same column's id.
    existing_field_ids: dict[str, int] = {}
    for f in namespace_schema:
        md = f.metadata or {}
        raw = md.get(LANCE_FIELD_ID_KEY.encode(), md.get(LANCE_FIELD_ID_KEY))
        if raw is None:
            continue
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode()
        existing_field_ids[f.name] = int(raw)
    _next_fid_seed = (max(existing_field_ids.values()) + 1) if existing_field_ids else 0

    df_arrow_schema = df.to_arrow().schema
    new_column_field_ids: dict[str, int] = {}
    extended_field_ids = dict(existing_field_ids)
    extended_fields = list(namespace_schema)
    for c in new_cols:
        idx = df_arrow_schema.get_field_index(c)
        if idx < 0:
            continue
        fid = _next_fid_seed
        _next_fid_seed += 1
        new_column_field_ids[c] = fid
        extended_field_ids[c] = fid
        extended_fields.append(df_arrow_schema.field(idx).with_metadata({LANCE_FIELD_ID_KEY: str(fid)}))
    extended_schema = pa.schema(extended_fields, metadata=namespace_schema.metadata)

    # Run per-(partition, fragment) merge in a single distributed pass.
    writer = _PartitionedFastPathWriter(
        partition_to_uri=partition_to_uri,
        partition_col_names=partition_cols,
        new_column_names=new_cols,
        new_column_field_ids=new_column_field_ids,
        storage_options=storage_options,
    )

    group_keys = partition_cols + ["fragment_id"]
    grouped = df.groupby(*group_keys).map_groups(
        writer(
            *(df[c] for c in new_cols),
            df["_rowaddr"],
            df["fragment_id"],
            *(df[c] for c in partition_cols),
        ).alias("commit_message")  # type: ignore[attr-defined]
    )
    commit_messages = grouped.collect().to_pydict()["commit_message"]

    # Group commit messages by leaf_uri so we can commit each leaf once.
    leaf_to_fragment_metas: dict[str, list[Any]] = defaultdict(list)
    leaf_to_new_schema: dict[str, Any] = {}
    for msg in commit_messages:
        fmeta_bytes = msg["fragment_meta"]
        schema_bytes = msg["schema"]
        leaf_uri = msg["leaf_uri"]
        if not fmeta_bytes or not schema_bytes:
            continue
        leaf_to_fragment_metas[leaf_uri].append(daft.pickle.loads(fmeta_bytes))
        leaf_to_new_schema.setdefault(leaf_uri, daft.pickle.loads(schema_bytes))

    if not leaf_to_fragment_metas:
        raise ValueError("Partitioned merge produced no fragment metadata.")

    # Commit each touched leaf with LanceOperation.Merge. Untouched fragments
    # must be included unmodified so the merge replaces the full fragment list.
    new_read_versions: dict[str, int] = {}
    uri_to_partition_key = {uri: key for key, uri in partition_to_uri.items()}
    for leaf_uri, enriched_metas in leaf_to_fragment_metas.items():
        ds = lance.dataset(leaf_uri, storage_options=storage_options)
        enriched_ids = {fm.id for fm in enriched_metas}
        all_metas = list(enriched_metas)
        for frag in ds.get_fragments():
            if frag.fragment_id not in enriched_ids:
                all_metas.append(frag.metadata)
        new_schema = leaf_to_new_schema[leaf_uri]
        op = lance.LanceOperation.Merge(all_metas, new_schema)
        committed = lance.LanceDataset.commit(
            leaf_uri,
            op,
            read_version=ds.version,
            storage_options=storage_options,
        )
        new_read_versions[leaf_uri] = int(committed.version)

    # Rewrite the manifest with bumped read_versions and an updated schema JSON.
    updated_read_versions: dict[tuple[Any, ...], int] = dict(partition_to_read_version)
    for leaf_uri, version in new_read_versions.items():
        key = uri_to_partition_key[leaf_uri]
        updated_read_versions[key] = version

    _rewrite_manifest(
        namespace_uri=namespace_uri,
        partition_cols=partition_cols,
        partition_field_specs=partition_field_specs,
        spec_id=spec_id,
        partition_to_oid=partition_to_oid,
        partition_to_uri=partition_to_uri,
        read_versions=updated_read_versions,
        namespace_schema=extended_schema,
        field_ids=extended_field_ids,
        storage_options=storage_options,
    )

    # Return a small stats DataFrame mirroring write_lance(mode="merge") shape.
    final_manifest = lance.dataset(namespace_uri + "/__manifest", storage_options=storage_options)
    total_fragments = sum(
        len(lance.dataset(uri, storage_options=storage_options).get_fragments()) for uri in new_read_versions.keys()
    )
    return daft.from_pydict(
        {
            "num_partitions": [len(new_read_versions)],
            "num_fragments": [total_fragments],
            "version": [int(final_manifest.latest_version)],
        }
    )


def _rewrite_manifest(
    *,
    namespace_uri: str,
    partition_cols: list[str],
    partition_field_specs: list[PartitionFieldSpec],
    spec_id: int,
    partition_to_oid: dict[tuple[Any, ...], str],
    partition_to_uri: dict[tuple[Any, ...], str],
    read_versions: dict[tuple[Any, ...], int],
    namespace_schema: pa.Schema,
    field_ids: dict[str, int],
    storage_options: dict[str, str] | None,
) -> None:
    """Rewrite ``__manifest`` after a partitioned merge.

    Keeps the existing namespace + table inventory; updates ``read_version``
    for touched leaves and refreshes the ``schema`` metadata to include the
    newly-added columns. Field IDs from before the merge are preserved.
    """
    import lance

    spec_version = f"v{spec_id}"

    new_schema_metadata = {
        f"partition_spec_{spec_version}": make_partition_spec_json(spec_id, partition_field_specs),
        METADATA_KEY_SCHEMA: make_namespace_schema_json(namespace_schema, field_ids),
    }
    new_manifest_schema = manifest_arrow_schema(partition_field_specs, new_schema_metadata)

    # Read the existing manifest so we can keep namespace rows verbatim.
    manifest_ds = lance.dataset(namespace_uri + "/__manifest", storage_options=storage_options)
    old_table = manifest_ds.to_table()

    columns: dict[str, list[Any]] = {f.name: [] for f in new_manifest_schema}

    oid_to_key = {oid: key for key, oid in partition_to_oid.items()}
    oid_to_loc = {partition_to_oid[key]: partition_to_uri[key].split("/")[-1] for key in partition_to_uri}

    for i in range(old_table.num_rows):
        oid = old_table.column(COL_OBJECT_ID)[i].as_py()
        otype = old_table.column(COL_OBJECT_TYPE)[i].as_py()
        is_table_row = otype == OBJECT_TYPE_TABLE

        location: str | None = None
        read_version: int | None = None
        if is_table_row:
            location = oid_to_loc.get(oid)
            if location is None:
                # Unknown table row — keep its original location.
                location = old_table.column(COL_LOCATION)[i].as_py()
            key = oid_to_key.get(oid)
            if key is not None and key in read_versions:
                read_version = read_versions[key]

        row = {
            COL_OBJECT_ID: oid,
            COL_OBJECT_TYPE: otype,
            COL_LOCATION: location,
            "metadata": "{}",
            "base_objects": None,
            COL_READ_VERSION: read_version,
            "read_branch": None,
            "read_tag": None,
        }
        # Partition value columns: copy from the existing manifest.
        for c in partition_cols:
            colname = f"{PARTITION_FIELD_PREFIX}{c}"
            row[colname] = old_table.column(colname)[i].as_py() if colname in old_table.column_names else None

        for fname in columns:
            columns[fname].append(row.get(fname))

    arrays = [pa.array(columns[f.name], type=f.type) for f in new_manifest_schema]
    new_manifest = pa.Table.from_arrays(arrays, schema=new_manifest_schema)
    lance.write_dataset(
        new_manifest,
        namespace_uri + "/__manifest",
        mode="overwrite",
        storage_options=storage_options,
    )
