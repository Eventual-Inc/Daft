"""Spec-compliance tests for Lance Partitioned Namespace support.

These tests are written first (TDD) and assert that Daft's partitioned
read/write path conforms exactly to:

- Lance V2 Directory Namespace spec — manifest table schema, hash-prefixed
  table directories, root-namespace properties stored in manifest metadata.
- Lance Partitioning spec — partition_spec_v<N> + schema metadata,
  partition_field_{field_id} columns, hierarchical object_ids with
  random 16-char base36 namespace segments, ``dataset`` leaf naming, and
  per-row inheritance of partition values from parent namespaces.

Each test asserts a single spec property; failures should point directly at
the spec rule that was violated.
"""

from __future__ import annotations

import json
import os
import re
import tempfile

import pyarrow as pa
import pytest

import daft

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------

# 16-char base36 segment (a-z, 0-9). Used to validate object_id segments.
_NS_ID_RE = re.compile(r"^[a-z0-9]{16}$")

# 8-char lowercase hex prefix on physical table directories.
_HASH_PREFIX_RE = re.compile(r"^[a-f0-9]{8}$")


@pytest.fixture
def namespace_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield os.path.join(tmpdir, "test_namespace")


@pytest.fixture
def sample_df():
    return daft.from_pydict(
        {
            "year": [2024, 2024, 2025, 2025, 2024, 2025],
            "country": ["US", "UK", "US", "UK", "US", "US"],
            "value": [10, 20, 30, 40, 50, 60],
            "name": ["alice", "bob", "charlie", "diana", "eve", "frank"],
        }
    )


def _open_manifest(namespace_dir: str):
    """Open the __manifest Lance dataset and return (dataset, table)."""
    import lance

    ds = lance.dataset(os.path.join(namespace_dir, "__manifest"))
    return ds, ds.to_table()


def _table_row_indices(manifest_table: pa.Table) -> list[int]:
    return [i for i in range(manifest_table.num_rows) if manifest_table.column("object_type")[i].as_py() == "table"]


def _namespace_row_indices(manifest_table: pa.Table) -> list[int]:
    return [i for i in range(manifest_table.num_rows) if manifest_table.column("object_type")[i].as_py() == "namespace"]


def _decode_meta(metadata: dict | None, key: str) -> str | None:
    """Look up a metadata key tolerating str/bytes."""
    if metadata is None:
        return None
    if key in metadata:
        v = metadata[key]
    elif key.encode() in metadata:
        v = metadata[key.encode()]
    else:
        return None
    return v.decode() if isinstance(v, (bytes, bytearray)) else v


# ===========================================================================
# Public API surface
# ===========================================================================


class TestPublicAPI:
    def test_write_lance_accepts_partition_cols(self, namespace_dir, sample_df):
        # smoke: keyword should be accepted, write should produce a __manifest
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        assert os.path.isdir(os.path.join(namespace_dir, "__manifest"))

    def test_read_lance_accepts_namespace_partitioning(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        assert df.count_rows() == 6

    def test_partition_cols_with_merge_mode_raises(self, namespace_dir, sample_df):
        with pytest.raises(ValueError, match="merge"):
            sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="merge")

    def test_empty_partition_cols_raises(self, namespace_dir, sample_df):
        with pytest.raises(ValueError):
            sample_df.write_lance(namespace_dir, partition_cols=[], mode="create")

    def test_unknown_partition_col_raises(self, namespace_dir, sample_df):
        with pytest.raises(ValueError):
            sample_df.write_lance(namespace_dir, partition_cols=["nope"], mode="create")


# ===========================================================================
# Write modes
# ===========================================================================


class TestWriteModes:
    def test_basic_write_returns_stats(self, namespace_dir, sample_df):
        result = sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        pd = result.to_pydict()
        # 4 distinct (year, country) combinations across 6 rows
        assert pd["num_partitions"] == [4]
        assert pd["num_fragments"] == [4]
        # version is a single int64
        assert isinstance(pd["version"][0], int)

    def test_create_mode_fails_if_exists(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        with pytest.raises(ValueError, match="exists"):
            sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")

    def test_overwrite_mode(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df2 = daft.from_pydict({"year": [2026], "country": ["JP"], "value": [100], "name": ["kenji"]})
        df2.write_lance(namespace_dir, partition_cols=["year", "country"], mode="overwrite")
        result = daft.read_lance(namespace_dir, namespace_partitioning=True).to_pydict()
        assert result["year"] == [2026]
        assert result["country"] == ["JP"]
        assert result["value"] == [100]

    def test_append_existing_partition(self, namespace_dir):
        df1 = daft.from_pydict({"key": ["a", "b"], "val": [1, 2]})
        df1.write_lance(namespace_dir, partition_cols=["key"], mode="create")

        df2 = daft.from_pydict({"key": ["a"], "val": [3]})
        df2.write_lance(namespace_dir, partition_cols=["key"], mode="append")

        _, manifest = _open_manifest(namespace_dir)
        # Existing table rows must not be duplicated when appending to the same partition.
        table_oids = [manifest.column("object_id")[i].as_py() for i in _table_row_indices(manifest)]
        assert sorted(table_oids) == sorted(set(table_oids)), table_oids
        # Still 2 partitions (a, b), not 3.
        assert len(table_oids) == 2

        result = daft.read_lance(namespace_dir, namespace_partitioning=True).to_pydict()
        assert sorted(result["val"]) == [1, 2, 3]

    def test_append_new_partition(self, namespace_dir):
        df1 = daft.from_pydict({"key": ["a"], "val": [1]})
        df1.write_lance(namespace_dir, partition_cols=["key"], mode="create")

        df2 = daft.from_pydict({"key": ["b"], "val": [2]})
        df2.write_lance(namespace_dir, partition_cols=["key"], mode="append")

        _, manifest = _open_manifest(namespace_dir)
        table_oids = [manifest.column("object_id")[i].as_py() for i in _table_row_indices(manifest)]
        assert len(table_oids) == 2

        result = daft.read_lance(namespace_dir, namespace_partitioning=True).to_pydict()
        assert sorted(zip(result["key"], result["val"])) == [("a", 1), ("b", 2)]

    def test_append_missing_manifest_raises(self, namespace_dir):
        df = daft.from_pydict({"k": ["a"], "v": [1]})
        with pytest.raises(ValueError):
            df.write_lance(namespace_dir, partition_cols=["k"], mode="append")

    def test_read_lance_warns_on_ignored_args_under_namespace_partitioning(self, namespace_dir, sample_df):
        """Single-dataset args should warn rather than silently no-op.

        Args targeting a single Lance dataset have no effect on a partitioned
        namespace; surfacing this as a warning prevents silent no-ops.
        """
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            daft.read_lance(namespace_dir, namespace_partitioning=True, version=5).count_rows()
        messages = [str(w.message) for w in caught]
        assert any("version" in m and "ignores" in m for m in messages), messages

    def test_read_lance_no_warning_when_args_default(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        import warnings as _warnings

        with _warnings.catch_warnings(record=True) as caught:
            _warnings.simplefilter("always")
            daft.read_lance(namespace_dir, namespace_partitioning=True).count_rows()
        # No ignored-arg warning should appear when the args are all defaults.
        assert not any("ignores argument" in str(w.message) for w in caught)

    def test_null_partition_values_preserved(self, namespace_dir):
        """NULL partition values land in their own partition, not dropped.

        Rows with NULL partition values should land in their own NULL partition,
        not be silently dropped during the partition grouping.
        """
        df = daft.from_pydict({"key": ["a", None, "b", None], "val": [1, 2, 3, 4]})
        df.write_lance(namespace_dir, partition_cols=["key"], mode="create")
        result = daft.read_lance(namespace_dir, namespace_partitioning=True).to_pydict()
        # All 4 rows must round-trip.
        assert len(result["val"]) == 4
        assert sorted(v for v in result["val"]) == [1, 2, 3, 4]
        # The NULL-keyed values land in their own partition.
        null_vals = sorted(v for k, v in zip(result["key"], result["val"]) if k is None)
        assert null_vals == [2, 4]


# ===========================================================================
# Manifest table schema
# ===========================================================================


class TestManifestSchema:
    """V2 base columns + partitioning extension columns are all present and typed correctly."""

    def test_v2_base_columns_present(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        for col in ("object_id", "object_type", "location", "metadata", "base_objects"):
            assert col in manifest.column_names, f"missing V2 base column {col!r}"

    def test_partitioning_extension_columns_present(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        for col in ("read_version", "read_branch", "read_tag"):
            assert col in manifest.column_names, f"missing partitioning ext column {col!r}"

    def test_partition_field_columns_present(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        assert "partition_field_year" in manifest.column_names
        assert "partition_field_country" in manifest.column_names

    def test_column_types(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        schema = manifest.schema
        assert pa.types.is_string(schema.field("object_id").type)
        assert pa.types.is_string(schema.field("object_type").type)
        assert pa.types.is_string(schema.field("location").type)
        assert pa.types.is_string(schema.field("metadata").type)
        assert pa.types.is_list(schema.field("base_objects").type)
        assert pa.types.is_string(schema.field("base_objects").type.value_type)
        # read_version: uint64 per spec
        assert pa.types.is_uint64(schema.field("read_version").type)
        assert pa.types.is_string(schema.field("read_branch").type)
        assert pa.types.is_string(schema.field("read_tag").type)

    def test_nullability(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        schema = manifest.schema
        # Per spec: object_id and object_type are required; rest are nullable.
        assert not schema.field("object_id").nullable
        assert not schema.field("object_type").nullable
        assert schema.field("location").nullable
        assert schema.field("metadata").nullable
        assert schema.field("base_objects").nullable
        assert schema.field("read_version").nullable
        assert schema.field("read_branch").nullable
        assert schema.field("read_tag").nullable
        assert schema.field("partition_field_year").nullable

    def test_object_id_has_unenforced_primary_key_metadata(self, namespace_dir, sample_df):
        """Verify object_id carries the unenforced-primary-key metadata.

        The reference `lance-namespace-impls` marks `object_id` with
        `lance-schema:unenforced-primary-key:position = "0"` so Lance treats it
        as a primary key for merge-insert conflict detection.
        """
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        oid_field = manifest.schema.field("object_id")
        md = oid_field.metadata or {}
        # Lance round-trips Arrow field metadata as bytes.
        key = b"lance-schema:unenforced-primary-key:position"
        assert key in md, dict(md)
        assert md[key] == b"0", md[key]

    def test_base_objects_inner_field_name(self, namespace_dir, sample_df):
        """Verify the `base_objects` inner List<Utf8> field is named `object_id`.

        The reference impl uses `"object_id"` as the inner field of the
        `base_objects` List<Utf8>. PyArrow's default would be `"item"`.
        """
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        base_objects_type = manifest.schema.field("base_objects").type
        assert pa.types.is_list(base_objects_type)
        inner = base_objects_type.value_field
        assert inner.name == "object_id", inner.name
        assert pa.types.is_string(inner.type)

    def test_leaf_field_ids_match_namespace(self, namespace_dir, sample_df):
        """Leaf-stored ``lance:field_id`` agrees with namespace declaration.

        Per spec, each leaf's ``lance:field_id`` Arrow metadata must agree
        with the namespace's ``schema`` JSON. Without pinning, Lance assigns
        field ids sequentially per-leaf and two leaves can disagree about the
        same column's id.
        """
        import lance

        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        ds, manifest = _open_manifest(namespace_dir)

        ns_ids = {
            f["name"]: f["metadata"]["lance:field_id"]
            for f in json.loads(_decode_meta(ds.schema.metadata, "schema"))["fields"]
        }

        for i in _table_row_indices(manifest):
            location = manifest.column("location")[i].as_py()
            leaf = lance.dataset(os.path.join(namespace_dir, location))
            for f in leaf.schema:
                md = dict(f.metadata or {})
                raw = md.get(b"lance:field_id") or md.get("lance:field_id")
                assert raw is not None, f"leaf {location} field {f.name!r} missing lance:field_id"
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode()
                assert raw == ns_ids[f.name], (
                    f"leaf {location} field {f.name!r}: lance:field_id={raw} but namespace says {ns_ids[f.name]}"
                )


# ===========================================================================
# Root namespace properties (schema metadata)
# ===========================================================================


class TestRootNamespaceMetadata:
    """`partition_spec_v<N>` and `schema` live on the __manifest dataset's schema metadata."""

    def test_partition_spec_v1_present(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        raw = _decode_meta(ds.schema.metadata, "partition_spec_v1")
        assert raw is not None, "partition_spec_v1 must be in __manifest's schema metadata"
        spec = json.loads(raw)
        assert spec["id"] == 1
        assert isinstance(spec["fields"], list)
        assert len(spec["fields"]) == 1

    def test_partition_spec_field_shape(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        spec = json.loads(_decode_meta(ds.schema.metadata, "partition_spec_v1"))

        assert spec["id"] == 1
        names = [f["field_id"] for f in spec["fields"]]
        assert names == ["year", "country"]

        for field in spec["fields"]:
            # Per spec: transform is a JSON object with "type", NOT a bare string.
            assert isinstance(field["transform"], dict), field
            assert field["transform"] == {"type": "identity"}
            # Per spec: result_type is a JsonArrowDataType, NOT a bare string.
            assert isinstance(field["result_type"], dict), field
            assert "type" in field["result_type"]
            # Per spec: source_ids is an int array.
            assert isinstance(field["source_ids"], list)
            assert all(isinstance(x, int) for x in field["source_ids"])

    def test_partition_spec_result_types(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        spec = json.loads(_decode_meta(ds.schema.metadata, "partition_spec_v1"))
        by_id = {f["field_id"]: f for f in spec["fields"]}
        assert by_id["year"]["result_type"] == {"type": "int64"}
        # Daft coerces utf8 -> large_utf8 on ingest; both are valid JsonArrowDataType values.
        assert by_id["country"]["result_type"]["type"] in ("utf8", "large_utf8")

    def test_schema_metadata_is_jsonarrowschema(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        raw = _decode_meta(ds.schema.metadata, "schema")
        assert raw is not None
        schema_obj = json.loads(raw)
        assert "fields" in schema_obj
        # Every field has name, nullable, type-as-object, and lance:field_id metadata.
        for f in schema_obj["fields"]:
            assert "name" in f
            assert "nullable" in f
            assert isinstance(f["type"], dict)
            assert "metadata" in f
            assert "lance:field_id" in f["metadata"]
            # field_id values are strings per JsonArrowField shape
            assert isinstance(f["metadata"]["lance:field_id"], str)

    def test_schema_includes_partition_columns(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        schema_obj = json.loads(_decode_meta(ds.schema.metadata, "schema"))
        names = [f["name"] for f in schema_obj["fields"]]
        # The namespace schema is the user-facing schema and includes partition cols.
        assert set(names) == {"year", "country", "value", "name"}

    def test_field_ids_unique_and_immutable_across_append(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        before = {
            f["name"]: f["metadata"]["lance:field_id"]
            for f in json.loads(_decode_meta(ds.schema.metadata, "schema"))["fields"]
        }
        # field ids must be unique
        assert len(set(before.values())) == len(before)

        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="append")
        ds2, _ = _open_manifest(namespace_dir)
        after = {
            f["name"]: f["metadata"]["lance:field_id"]
            for f in json.loads(_decode_meta(ds2.schema.metadata, "schema"))["fields"]
        }
        # field ids must not change on append
        assert before == after


# ===========================================================================
# Object ID structure
# ===========================================================================


class TestObjectIds:
    def test_root_namespace_row_exists(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        oids = manifest.column("object_id").to_pylist()
        assert "v1" in oids

    def test_single_partition_col_depth(self, namespace_dir):
        df = daft.from_pydict({"region": ["east", "west", "east"], "val": [1, 2, 3]})
        df.write_lance(namespace_dir, partition_cols=["region"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        for i in _table_row_indices(manifest):
            oid = manifest.column("object_id")[i].as_py()
            parts = oid.split("$")
            # v1$<ns_id>$dataset
            assert len(parts) == 3, oid
            assert parts[0] == "v1"
            assert parts[-1] == "dataset"
            assert _NS_ID_RE.match(parts[1]), parts[1]

    def test_two_partition_col_depth(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        for i in _table_row_indices(manifest):
            oid = manifest.column("object_id")[i].as_py()
            parts = oid.split("$")
            # v1$<ns1>$<ns2>$dataset
            assert len(parts) == 4, oid
            assert parts[0] == "v1"
            assert parts[-1] == "dataset"
            for seg in parts[1:-1]:
                assert _NS_ID_RE.match(seg), seg

    def test_siblings_share_parent_segment(self, namespace_dir, sample_df):
        """Two tables under the same year must share the same first namespace id."""
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        year_to_seg: dict[int, str] = {}
        for i in _table_row_indices(manifest):
            oid = manifest.column("object_id")[i].as_py()
            year = manifest.column("partition_field_year")[i].as_py()
            seg1 = oid.split("$")[1]
            if year in year_to_seg:
                assert year_to_seg[year] == seg1, f"tables with year={year} must share first segment"
            else:
                year_to_seg[year] = seg1
        assert len(year_to_seg) == 2

    def test_intermediate_namespace_rows(self, namespace_dir, sample_df):
        """Verify the manifest row inventory for a 2x2 partitioning.

        For [year, country] with 2 years × 2 countries × shared parents:
        manifest must contain 1 root + 2 year ns + 4 (year,country) ns + 4 tables = 11 rows.
        """
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        ns_count = len(_namespace_row_indices(manifest))
        tbl_count = len(_table_row_indices(manifest))
        assert ns_count == 7, ns_count  # 1 root + 2 year + 4 (year,country)
        assert tbl_count == 4, tbl_count
        assert manifest.num_rows == 11


# ===========================================================================
# Per-row content (table rows vs namespace rows)
# ===========================================================================


class TestRowContent:
    def test_table_rows_have_location(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        for i in _table_row_indices(manifest):
            loc = manifest.column("location")[i].as_py()
            assert loc is not None
            # location format: <8hex>_<object_id>
            assert "_" in loc
            prefix, _, rest = loc.partition("_")
            assert _HASH_PREFIX_RE.match(prefix), prefix
            assert rest == manifest.column("object_id")[i].as_py()

    def test_namespace_rows_have_null_location(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        for i in _namespace_row_indices(manifest):
            assert manifest.column("location")[i].as_py() is None

    def test_table_rows_have_read_version(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        for i in _table_row_indices(manifest):
            rv = manifest.column("read_version")[i].as_py()
            assert isinstance(rv, int) and rv >= 1

    def test_namespace_rows_have_null_read_version(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        for i in _namespace_row_indices(manifest):
            assert manifest.column("read_version")[i].as_py() is None

    def test_read_branch_and_tag_always_null(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        assert manifest.column("read_branch").null_count == manifest.num_rows
        assert manifest.column("read_tag").null_count == manifest.num_rows

    def test_base_objects_always_null(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        assert manifest.column("base_objects").null_count == manifest.num_rows

    def test_partition_values_inherited(self, namespace_dir, sample_df):
        """Per spec: intermediate ns at depth k carries partition values for fields[:k], NULL for fields[k:]."""
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        year_col = manifest.column("partition_field_year")
        country_col = manifest.column("partition_field_country")
        for i in range(manifest.num_rows):
            oid = manifest.column("object_id")[i].as_py()
            otype = manifest.column("object_type")[i].as_py()
            parts = oid.split("$")
            # Spec version root: both NULL.
            if oid == "v1":
                assert year_col[i].as_py() is None
                assert country_col[i].as_py() is None
                continue
            # Strip trailing "dataset" to compute namespace depth.
            depth_segs = parts[1:]
            if otype == "table":
                depth_segs = depth_segs[:-1]
            depth = len(depth_segs)
            # depth 1 = year ns; depth 2 = (year, country) ns or table
            assert depth >= 1
            assert year_col[i].as_py() is not None
            if depth >= 2:
                assert country_col[i].as_py() is not None
            else:
                assert country_col[i].as_py() is None


# ===========================================================================
# Physical layout
# ===========================================================================


class TestPhysicalLayout:
    def test_only_tables_have_dirs(self, namespace_dir, sample_df):
        """Per spec: only tables have physical directories. Namespaces do not."""
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)

        on_disk = {d for d in os.listdir(namespace_dir) if os.path.isdir(os.path.join(namespace_dir, d))}
        # Manifest is always present.
        assert "__manifest" in on_disk
        on_disk.discard("__manifest")

        # Every remaining dir must correspond to a table row's location.
        table_locations = {manifest.column("location")[i].as_py() for i in _table_row_indices(manifest)}
        assert on_disk == table_locations

    def test_physical_dir_format(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        for i in _table_row_indices(manifest):
            loc = manifest.column("location")[i].as_py()
            prefix, _, rest = loc.partition("_")
            assert _HASH_PREFIX_RE.match(prefix)
            assert rest.startswith("v1$")
            assert rest.endswith("$dataset")
            # And the directory actually exists.
            assert os.path.isdir(os.path.join(namespace_dir, loc))

    def test_partition_columns_stripped_from_leaf_dataset(self, namespace_dir, sample_df):
        """The Lance dataset for a partition holds only non-partition columns."""
        import lance

        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        tbl_idx = _table_row_indices(manifest)[0]
        loc = manifest.column("location")[tbl_idx].as_py()
        ds = lance.dataset(os.path.join(namespace_dir, loc))
        leaf_names = set(ds.schema.names)
        assert "year" not in leaf_names
        assert "country" not in leaf_names
        assert "value" in leaf_names
        assert "name" in leaf_names


# ===========================================================================
# Read side
# ===========================================================================


class TestRead:
    def test_read_all_rows(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        result = daft.read_lance(namespace_dir, namespace_partitioning=True).to_pydict()
        assert sorted(result["value"]) == [10, 20, 30, 40, 50, 60]
        assert set(result["year"]) == {2024, 2025}
        assert set(result["country"]) == {"US", "UK"}

    def test_schema_includes_partition_columns(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        names = {f.name for f in df.schema()}
        assert names == {"year", "country", "value", "name"}

    def test_partition_filter_single_col(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.where(df["year"] == 2024).to_pydict()
        assert all(y == 2024 for y in result["year"])
        assert sorted(result["value"]) == [10, 20, 50]

    def test_partition_filter_string_col(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.where(df["country"] == "US").to_pydict()
        assert all(c == "US" for c in result["country"])
        assert sorted(result["value"]) == [10, 30, 50, 60]

    def test_combined_partition_filter(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.where((df["year"] == 2025) & (df["country"] == "UK")).to_pydict()
        assert result["year"] == [2025]
        assert result["country"] == ["UK"]
        assert result["value"] == [40]

    def test_data_filter(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.where(df["value"] > 25).to_pydict()
        assert all(v > 25 for v in result["value"])

    def test_combined_partition_and_data_filter(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.where((df["year"] == 2024) & (df["value"] > 15)).to_pydict()
        assert all(y == 2024 for y in result["year"])
        assert all(v > 15 for v in result["value"])

    def test_column_projection_data_only(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.select("value", "name").to_pydict()
        assert set(result.keys()) == {"value", "name"}
        assert len(result["value"]) == 6

    def test_column_projection_partition_only(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.select("year").to_pydict()
        assert set(result.keys()) == {"year"}
        assert sorted(result["year"]) == [2024, 2024, 2024, 2025, 2025, 2025]

    def test_column_projection_mixed(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.select("country", "value").to_pydict()
        assert set(result.keys()) == {"country", "value"}
        # Pair up to verify partition column is correctly aligned to data
        pairs = sorted(zip(result["country"], result["value"]))
        assert pairs == [
            ("UK", 20),
            ("UK", 40),
            ("US", 10),
            ("US", 30),
            ("US", 50),
            ("US", 60),
        ]

    def test_pruning_skips_unmatched_partitions(self, namespace_dir, sample_df, monkeypatch):
        """Verify partition pruning only scans the matching partition dataset.

        When a partition filter rules out 3 of 4 partitions, only the matching
        table should be *scanned*. The planner may open additional datasets for
        schema sniffing (e.g., to detect sibling-leaf stitching), but actual
        data reads (via ``.scanner(...)``) should hit only the matched leaf.
        """
        import lance

        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")

        scanned_uris: list[str] = []
        real_dataset = lance.dataset

        def tracing_dataset(uri, *args, **kwargs):
            ds = real_dataset(uri, *args, **kwargs)
            if "__manifest" in str(uri):
                return ds
            real_scanner = ds.scanner

            def tracing_scanner(*sa, **sk):
                scanned_uris.append(str(uri))
                return real_scanner(*sa, **sk)

            ds.scanner = tracing_scanner
            return ds

        monkeypatch.setattr(lance, "dataset", tracing_dataset)
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        df.where((df["year"] == 2025) & (df["country"] == "UK")).to_pydict()

        assert len(set(scanned_uris)) == 1, scanned_uris


# ===========================================================================
# Roundtrip
# ===========================================================================


class TestFragmentIdAndRowAddr:
    """Read-side support needed for the fast-path partitioned merge.

    `include_fragment_id` exposes per-leaf fragment ids, and
    `default_scan_options={'with_row_address': True}` exposes `_rowaddr`.
    """

    def test_include_fragment_id_adds_column(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True, include_fragment_id=True)
        names = {f.name for f in df.schema()}
        assert "fragment_id" in names

    def test_include_fragment_id_values_match_leaf(self, namespace_dir):
        # Write a small partitioned dataset, then read with fragment_id and verify
        # every emitted fragment_id corresponds to an actual fragment in the
        # matching leaf table.
        import lance

        df_in = daft.from_pydict(
            {
                "robot_id": [1, 1, 2, 2, 2],
                "ts": [10, 11, 20, 21, 22],
                "v": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        )
        df_in.write_lance(namespace_dir, partition_cols=["robot_id"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True, include_fragment_id=True).to_pydict()

        # Build a {robot_id: set(fragment_ids)} from the manifest's leaves.
        _, manifest = _open_manifest(namespace_dir)
        leaf_ids: dict[int, set[int]] = {}
        for i in _table_row_indices(manifest):
            robot_id = manifest.column("partition_field_robot_id")[i].as_py()
            loc = manifest.column("location")[i].as_py()
            ds = lance.dataset(os.path.join(namespace_dir, loc))
            leaf_ids[robot_id] = {f.fragment_id for f in ds.get_fragments()}

        for r, fid in zip(df["robot_id"], df["fragment_id"]):
            assert fid in leaf_ids[r], (r, fid, leaf_ids[r])

    def test_with_row_address_exposes_rowaddr(self, namespace_dir):
        df_in = daft.from_pydict({"k": ["a", "a", "b"], "v": [1, 2, 3]})
        df_in.write_lance(namespace_dir, partition_cols=["k"], mode="create")

        df = daft.read_lance(
            namespace_dir,
            namespace_partitioning=True,
            include_fragment_id=True,
            default_scan_options={"with_row_address": True},
        ).to_pydict()
        assert "_rowaddr" in df
        # row addresses are unique per leaf table.
        for k_val in set(df["k"]):
            addrs = [a for k, a in zip(df["k"], df["_rowaddr"]) if k == k_val]
            assert len(addrs) == len(set(addrs))


class TestPartitionedMerge:
    """End-to-end enrichment via the partitioned fast-path merge.

    Reads with fragment_id + _rowaddr, computes a new column, writes back
    via partitioned fast-path merge. Verifies the new column is visible
    after re-read with values aligned per row, the base data is unchanged,
    and the manifest's leaf read_versions are bumped.
    """

    @pytest.fixture
    def base_df(self):
        return daft.from_pydict(
            {
                "robot_id": [1, 1, 1, 2, 2],
                "ts": [10, 11, 12, 20, 21],
                "v": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        )

    def _read_with_meta(self, uri):
        return daft.read_lance(
            uri,
            namespace_partitioning=True,
            include_fragment_id=True,
            default_scan_options={"with_row_address": True},
        )

    def test_merge_requires_fragment_id(self, namespace_dir, base_df):
        base_df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="create")
        # Build an input lacking fragment_id (and _rowaddr).
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        df = df.with_column("enriched", df["v"] * 10)
        with pytest.raises(ValueError, match="fragment_id"):
            df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="merge")

    def test_merge_requires_rowaddr(self, namespace_dir, base_df):
        base_df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True, include_fragment_id=True)
        df = df.with_column("enriched", df["v"] * 10)
        with pytest.raises(ValueError, match="_rowaddr"):
            df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="merge")

    def test_merge_requires_new_columns(self, namespace_dir, base_df):
        base_df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="create")
        df = self._read_with_meta(namespace_dir)
        # No new columns added.
        with pytest.raises(ValueError, match="new column"):
            df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="merge")

    def test_merge_unknown_partition_raises(self, namespace_dir, base_df):
        base_df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="create")
        df = self._read_with_meta(namespace_dir)
        # Manually mutate the robot_id to an unknown partition value.
        df = df.with_column("robot_id", df["robot_id"] + 99)
        df = df.with_column("enriched", df["v"] * 10)
        with pytest.raises(ValueError, match="partition"):
            df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="merge")

    def test_merge_adds_new_column(self, namespace_dir, base_df):
        base_df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="create")
        df = self._read_with_meta(namespace_dir)
        df = df.with_column("enriched", df["v"] * 10)

        df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="merge")

        result = daft.read_lance(namespace_dir, namespace_partitioning=True).to_pydict()
        # Base columns preserved.
        assert sorted(zip(result["robot_id"], result["ts"], result["v"])) == [
            (1, 10, 1.0),
            (1, 11, 2.0),
            (1, 12, 3.0),
            (2, 20, 4.0),
            (2, 21, 5.0),
        ]
        # Enriched column matches v * 10 with rows aligned.
        rows = sorted(zip(result["robot_id"], result["ts"], result["v"], result["enriched"]))
        for _, _, v, e in rows:
            assert e == pytest.approx(v * 10)

    def test_merge_bumps_leaf_read_versions(self, namespace_dir, base_df):
        base_df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="create")
        _, manifest_before = _open_manifest(namespace_dir)
        before = {
            manifest_before.column("object_id")[i].as_py(): manifest_before.column("read_version")[i].as_py()
            for i in _table_row_indices(manifest_before)
        }

        df = self._read_with_meta(namespace_dir)
        df = df.with_column("enriched", df["v"] * 10)
        df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="merge")

        _, manifest_after = _open_manifest(namespace_dir)
        after = {
            manifest_after.column("object_id")[i].as_py(): manifest_after.column("read_version")[i].as_py()
            for i in _table_row_indices(manifest_after)
        }
        for oid, v0 in before.items():
            assert after[oid] is not None
            assert after[oid] > v0, (oid, v0, after[oid])

    def test_merge_updates_namespace_schema_metadata(self, namespace_dir, base_df):
        base_df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="create")
        df = self._read_with_meta(namespace_dir)
        df = df.with_column("enriched", df["v"] * 10)
        df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="merge")

        ds, _ = _open_manifest(namespace_dir)
        schema_obj = json.loads(_decode_meta(ds.schema.metadata, "schema"))
        names = {f["name"] for f in schema_obj["fields"]}
        assert "enriched" in names
        # Existing fields keep their lance:field_id; new field gets a fresh one.
        ids = {f["name"]: int(f["metadata"]["lance:field_id"]) for f in schema_obj["fields"]}
        assert ids["enriched"] not in {ids["robot_id"], ids["ts"], ids["v"]}

    def test_merge_pins_new_column_field_ids(self, namespace_dir, base_df):
        """Merged column's ``lance:field_id`` matches namespace, consistently across leaves.

        After a merge, the new column's ``lance:field_id`` Arrow metadata
        must match the namespace's declared id on every touched leaf — and
        the same (column name, field id) pair must hold across all leaves so
        spec-strict readers resolving by id see a single, consistent column.
        """
        import lance

        base_df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="create")
        df = self._read_with_meta(namespace_dir)
        df = df.with_column("enriched", df["v"] * 10)
        df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="merge")

        ds, manifest = _open_manifest(namespace_dir)
        ns_ids = {
            f["name"]: f["metadata"]["lance:field_id"]
            for f in json.loads(_decode_meta(ds.schema.metadata, "schema"))["fields"]
        }
        expected_enriched_id = ns_ids["enriched"]

        seen: list[str] = []
        for i in _table_row_indices(manifest):
            location = manifest.column("location")[i].as_py()
            leaf = lance.dataset(os.path.join(namespace_dir, location))
            enriched_field = leaf.schema.field("enriched")
            md = dict(enriched_field.metadata or {})
            raw = md.get(b"lance:field_id") or md.get("lance:field_id")
            assert raw is not None, f"leaf {location} missing lance:field_id on 'enriched'"
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode()
            assert raw == expected_enriched_id, (
                f"leaf {location}: enriched lance:field_id={raw} but namespace says {expected_enriched_id}"
            )
            seen.append(raw)
        # All leaves agree on the same id for 'enriched'.
        assert len(set(seen)) == 1, seen

    def test_merge_does_not_rewrite_base_data_files(self, namespace_dir, base_df):
        """Fast-path merge writes a NEW .lance file alongside existing ones.

        The original data files must remain untouched.
        """
        base_df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="create")
        _, manifest = _open_manifest(namespace_dir)
        # Snapshot the data files of each leaf table.
        files_before: dict[str, set[str]] = {}
        for i in _table_row_indices(manifest):
            loc = manifest.column("location")[i].as_py()
            data_dir = os.path.join(namespace_dir, loc, "data")
            files_before[loc] = set(os.listdir(data_dir))

        df = self._read_with_meta(namespace_dir)
        df = df.with_column("enriched", df["v"] * 10)
        df.write_lance(namespace_dir, partition_cols=["robot_id"], mode="merge")

        for loc, before in files_before.items():
            data_dir = os.path.join(namespace_dir, loc, "data")
            after = set(os.listdir(data_dir))
            # The original files must still be present.
            assert before <= after, (loc, before - after)
            # And the merge added at least one new file.
            assert len(after - before) >= 1, (loc, after, before)


class TestRoundtrip:
    def test_simple_roundtrip(self, namespace_dir):
        original = {"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [10.0, 20.0, 30.0]}
        df = daft.from_pydict(original)
        df.write_lance(namespace_dir, partition_cols=["b"], mode="create")
        result = daft.read_lance(namespace_dir, namespace_partitioning=True).sort("a").to_pydict()
        assert result["a"] == [1, 2, 3]
        assert result["b"] == ["x", "y", "z"]
        assert result["c"] == [10.0, 20.0, 30.0]

    def test_roundtrip_with_nulls_in_data(self, namespace_dir):
        df = daft.from_pydict({"key": ["a", "a", "b", "b"], "val": [1, None, 3, None]})
        df.write_lance(namespace_dir, partition_cols=["key"], mode="create")
        result = daft.read_lance(namespace_dir, namespace_partitioning=True).to_pydict()
        assert sorted(result["key"]) == ["a", "a", "b", "b"]
        non_null = sorted(v for v in result["val"] if v is not None)
        assert non_null == [1, 3]
        assert result["val"].count(None) == 2

    def test_many_partitions(self, namespace_dir):
        n = 50
        df = daft.from_pydict({"part": list(range(n)), "data": list(range(n))})
        stats = df.write_lance(namespace_dir, partition_cols=["part"], mode="create").to_pydict()
        assert stats["num_partitions"] == [n]

        result = daft.read_lance(namespace_dir, namespace_partitioning=True).sort("part").to_pydict()
        assert result["part"] == list(range(n))
        assert result["data"] == list(range(n))


# ===========================================================================
# Helpers in daft.io.lance.lance_namespace
# ===========================================================================


class TestHelpers:
    def test_random_namespace_id_shape(self):
        from daft.io.lance.lance_namespace import random_namespace_id

        ids = [random_namespace_id() for _ in range(50)]
        for i in ids:
            assert _NS_ID_RE.match(i), i
        assert len(set(ids)) == len(ids), "IDs must be unique with overwhelming probability"

    def test_random_dir_hash_shape(self):
        from daft.io.lance.lance_namespace import random_dir_hash

        h = random_dir_hash()
        assert _HASH_PREFIX_RE.match(h), h

    def test_make_object_id_namespace(self):
        from daft.io.lance.lance_namespace import make_object_id

        assert make_object_id("v1", [], is_table=False) == "v1"
        assert make_object_id("v1", ["abc"], is_table=False) == "v1$abc"
        assert make_object_id("v2", ["abc", "def"], is_table=False) == "v2$abc$def"

    def test_make_object_id_table(self):
        from daft.io.lance.lance_namespace import make_object_id

        assert make_object_id("v1", ["abc"], is_table=True) == "v1$abc$dataset"
        assert make_object_id("v1", ["abc", "def"], is_table=True) == "v1$abc$def$dataset"

    def test_parse_object_id(self):
        from daft.io.lance.lance_namespace import parse_object_id

        assert parse_object_id("v1") == ("v1", [], False)
        assert parse_object_id("v1$abc") == ("v1", ["abc"], False)
        assert parse_object_id("v1$abc$def") == ("v1", ["abc", "def"], False)
        assert parse_object_id("v1$abc$dataset") == ("v1", ["abc"], True)
        assert parse_object_id("v1$abc$def$dataset") == ("v1", ["abc", "def"], True)

    def test_partition_spec_json_round_trip(self):
        from daft.io.lance.lance_namespace import (
            PartitionFieldSpec,
            make_partition_spec_json,
            parse_partition_spec_json,
        )

        specs = [
            PartitionFieldSpec("year", "year", 0, "identity", pa.int64()),
            PartitionFieldSpec("country", "country", 1, "identity", pa.utf8()),
        ]
        payload = make_partition_spec_json(1, specs)
        parsed = parse_partition_spec_json(payload)
        assert parsed["id"] == 1
        assert [f["field_id"] for f in parsed["fields"]] == ["year", "country"]
        assert all(f["transform"] == {"type": "identity"} for f in parsed["fields"])
        assert parsed["fields"][0]["result_type"] == {"type": "int64"}
        assert parsed["fields"][1]["result_type"] == {"type": "utf8"}

    def test_namespace_schema_json_round_trip(self):
        from daft.io.lance.lance_namespace import (
            make_namespace_schema_json,
            parse_namespace_schema_json,
        )

        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("year", pa.int32(), nullable=True),
                pa.field("country", pa.utf8(), nullable=True),
            ]
        )
        payload = make_namespace_schema_json(schema, {"id": 0, "year": 1, "country": 2})
        parsed = parse_namespace_schema_json(payload)
        assert parsed.names == ["id", "year", "country"]
        assert parsed.field("id").type == pa.int64()
        assert parsed.field("id").nullable is False
        assert parsed.field("year").type == pa.int32()
        assert parsed.field("country").type == pa.utf8()
        # field IDs preserved on metadata
        for name, expected in (("id", "0"), ("year", "1"), ("country", "2")):
            md = parsed.field(name).metadata
            assert md is not None
            assert md[b"lance:field_id"] == expected.encode()

    def test_arrow_type_round_trip(self):
        from daft.io.lance.lance_namespace import (
            arrow_type_to_json_arrow,
            json_arrow_to_arrow_type,
        )

        for dtype in [
            pa.bool_(),
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.uint32(),
            pa.float32(),
            pa.float64(),
            pa.utf8(),
            pa.binary(),
            pa.date32(),
        ]:
            assert json_arrow_to_arrow_type(arrow_type_to_json_arrow(dtype)) == dtype

    def test_pin_field_ids(self):
        from daft.io.lance.lance_namespace import pin_field_ids

        # Field A already has unrelated metadata; field B has none; partition
        # col is in the schema but not in the field_ids map (pass-through).
        schema = pa.schema(
            [
                pa.field("a", pa.int64(), metadata={"daft:nullability": "non-null"}),
                pa.field("b", pa.utf8()),
                pa.field("partition_col", pa.utf8()),
            ],
            metadata={"namespace": "test"},
        )
        pinned = pin_field_ids(schema, {"a": 0, "b": 2})

        # Schema-level metadata preserved.
        assert pinned.metadata == schema.metadata

        # field 'a' gets the new lance:field_id AND keeps its existing metadata.
        a_md = pinned.field("a").metadata or {}
        assert a_md.get(b"lance:field_id") == b"0"
        assert a_md.get(b"daft:nullability") == b"non-null"

        # field 'b' gets a fresh metadata dict with just lance:field_id.
        b_md = pinned.field("b").metadata or {}
        assert b_md.get(b"lance:field_id") == b"2"

        # field not in the map is passed through unchanged.
        assert pinned.field("partition_col").metadata == schema.field("partition_col").metadata


# ===========================================================================
# Cross-implementation spec compliance: read a hand-built manifest matching
# the spec's Appendix B (Physical Layout) and Appendix C (Manifest Table)
# byte-for-byte. If Daft can read this, our reader honors the literal spec.
# ===========================================================================


def _build_spec_literal_namespace(root: str) -> dict[str, dict[str, list]]:
    """Construct a namespace on disk that mirrors Appendix B / C of the spec.

    Uses a single partition spec (v1, identity transform on ``event_date``)
    so the example exercises the parts our reader supports. The object_id
    namespace segment is the literal 16-char value from the spec example.
    Physical directories are placed at hash-prefixed paths matching what the
    manifest records under ``location``.

    Returns the expected data per partition, so tests can verify reads.
    """
    import lance

    # Per Appendix A: a single identity partition by event_date with field_id=1 in the schema.
    partition_spec_v1 = {
        "id": 1,
        "fields": [
            {
                "field_id": "event_date",
                "source_ids": [1],
                "transform": {"type": "identity"},
                "result_type": {"type": "date32"},
            }
        ],
    }
    schema_json = {
        "fields": [
            {
                "name": "id",
                "nullable": False,
                "type": {"type": "int64"},
                "metadata": {"lance:field_id": "0"},
            },
            {
                "name": "event_date",
                "nullable": True,
                "type": {"type": "date32"},
                "metadata": {"lance:field_id": "1"},
            },
            {
                "name": "country",
                "nullable": True,
                "type": {"type": "utf8"},
                "metadata": {"lance:field_id": "2"},
            },
        ]
    }

    # Two leaf tables exactly like the v1 rows in Appendix C.
    leaf_specs = [
        {
            "ns_id": "k7m2n9p4q8r5s3t6",
            "hash": "b4a3c2d1",
            "event_date_str": "2025-12-10",
            "rows": {"id": [1, 2], "country": ["US", "UK"]},
        },
        {
            "ns_id": "w1x2y3z4a5b6c7d8",
            "hash": "55667788",
            "event_date_str": "2025-12-11",
            "rows": {"id": [3, 4], "country": ["US", "JP"]},
        },
    ]

    # Write each leaf Lance dataset (without partition columns, since they're inherited).
    expected: dict[str, dict[str, list]] = {}
    leaf_pa_schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("country", pa.utf8())])
    for spec in leaf_specs:
        ns_id = spec["ns_id"]
        h = spec["hash"]
        object_id = f"v1${ns_id}$dataset"
        location = f"{h}_{object_id}"
        leaf_table = pa.table(spec["rows"], schema=leaf_pa_schema)
        lance.write_dataset(leaf_table, os.path.join(root, location))
        expected[location] = {
            "object_id": object_id,
            "event_date_str": spec["event_date_str"],
            **spec["rows"],
        }

    # Manifest rows: 1 root ns + 2 intermediate ns + 2 tables = 5 rows total.
    manifest_rows: list[dict] = [
        {
            "object_id": "v1",
            "object_type": "namespace",
            "location": None,
            "metadata": "{}",
            "base_objects": None,
            "read_version": None,
            "read_branch": None,
            "read_tag": None,
            "partition_field_event_date": None,
        },
    ]
    for spec in leaf_specs:
        manifest_rows.append(
            {
                "object_id": f"v1${spec['ns_id']}",
                "object_type": "namespace",
                "location": None,
                "metadata": "{}",
                "base_objects": None,
                "read_version": None,
                "read_branch": None,
                "read_tag": None,
                "partition_field_event_date": spec["event_date_str"],
            }
        )
        manifest_rows.append(
            {
                "object_id": f"v1${spec['ns_id']}$dataset",
                "object_type": "table",
                "location": f"{spec['hash']}_v1${spec['ns_id']}$dataset",
                "metadata": "{}",
                "base_objects": None,
                "read_version": 1,  # Lance creates v1 of the leaf dataset on write_dataset
                "read_branch": None,
                "read_tag": None,
                "partition_field_event_date": spec["event_date_str"],
            }
        )

    manifest_schema = pa.schema(
        [
            pa.field("object_id", pa.utf8(), nullable=False),
            pa.field("object_type", pa.utf8(), nullable=False),
            pa.field("location", pa.utf8()),
            pa.field("metadata", pa.utf8()),
            pa.field("base_objects", pa.list_(pa.utf8())),
            pa.field("read_version", pa.uint64()),
            pa.field("read_branch", pa.utf8()),
            pa.field("read_tag", pa.utf8()),
            pa.field("partition_field_event_date", pa.date32()),
        ],
        metadata={
            "partition_spec_v1": json.dumps(partition_spec_v1),
            "schema": json.dumps(schema_json),
        },
    )

    columns = {field.name: [r[field.name] for r in manifest_rows] for field in manifest_schema}
    # Convert event_date strings to date32 for that column.
    import datetime as _dt

    columns["partition_field_event_date"] = [
        _dt.date.fromisoformat(v) if isinstance(v, str) else v for v in columns["partition_field_event_date"]
    ]
    arrays = [pa.array(columns[f.name], type=f.type) for f in manifest_schema]
    manifest_table = pa.Table.from_arrays(arrays, schema=manifest_schema)

    lance.write_dataset(manifest_table, os.path.join(root, "__manifest"))
    return expected


class TestSpecLiteralFixture:
    """Cross-implementation evidence for the spec's literal manifest shape.

    We build a manifest from the spec's literal Appendix B/C structure (not
    via our writer) and verify our reader handles it correctly. This proves
    the reader follows the spec, not just our own writer's quirks.
    """

    @pytest.fixture
    def spec_namespace(self, tmp_path):
        root = str(tmp_path / "spec_ns")
        os.makedirs(root, exist_ok=True)
        expected = _build_spec_literal_namespace(root)
        return root, expected

    def test_read_returns_all_rows(self, spec_namespace):
        root, expected = spec_namespace
        df = daft.read_lance(root, namespace_partitioning=True).to_pydict()
        total_rows = sum(len(v["id"]) for v in expected.values())
        assert len(df["id"]) == total_rows
        # Every (id, country, event_date) combination from the fixture is present.
        actual = sorted(zip(df["id"], df["country"], [str(d) for d in df["event_date"]]))
        expected_tuples: list[tuple] = []
        for v in expected.values():
            for i, c in zip(v["id"], v["country"]):
                expected_tuples.append((i, c, v["event_date_str"]))
        assert actual == sorted(expected_tuples)

    def test_schema_includes_all_namespace_fields(self, spec_namespace):
        root, _ = spec_namespace
        df = daft.read_lance(root, namespace_partitioning=True)
        names = {f.name for f in df.schema()}
        assert names == {"id", "country", "event_date"}

    def test_partition_filter_on_spec_fixture(self, spec_namespace):
        root, expected = spec_namespace
        df = daft.read_lance(root, namespace_partitioning=True)
        import datetime as _dt

        target = _dt.date(2025, 12, 10)
        result = df.where(df["event_date"] == target).to_pydict()
        # Should only return rows from the (2025-12-10) partition.
        match_specs = [v for v in expected.values() if v["event_date_str"] == "2025-12-10"]
        assert len(match_specs) == 1
        assert sorted(result["id"]) == sorted(match_specs[0]["id"])

    def test_only_leaf_tables_have_dirs(self, spec_namespace):
        root, expected = spec_namespace
        dirs_on_disk = {d for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))}
        dirs_on_disk.discard("__manifest")
        expected_dirs = set(expected.keys())
        assert dirs_on_disk == expected_dirs

    def test_heterogeneous_leaf_schemas_fixture(self):
        """Read a checked-in fixture exercising legacy JSON shape + sibling-leaf stitching.

        The fixture at ``tests/assets/lance/partitioned-namespace/heterogeneous-leaf-schemas``
        encodes a real-world scenario from the partitioning bench: topic-keyed
        partitions where each (robot, topic) leaf stores one signal column
        (speed-topic leaves have ``[ts, speed]``; caption-topic leaves have
        ``[ts, caption]``). The leaves under each robot are written in
        lockstep — same row count, positional alignment — so the reader
        stitches them column-wise into wider rows.

        Also exercises the *legacy* partition-spec JSON shape (``"transform":
        "identity"`` as a bare string, ``"result_type": "large_string"``
        rather than ``{"type": ...}``, and ``"source_ids": []``).
        """
        fixture = "tests/assets/lance/partitioned-namespace/heterogeneous-leaf-schemas"
        df = daft.read_lance(fixture, namespace_partitioning=True)
        schema_names = {f.name for f in df.schema()}
        assert schema_names == {"ts", "speed", "caption", "robot_id", "topic"}

        # 6 rows after stitching: 2 robots × 3 rows per stitched leaf-group.
        assert df.count_rows() == 6

        # Each row carries both `speed` and `caption` populated positionally.
        full = df.to_pydict()
        # robot_a rows: speed values from speed leaf, caption values from caption leaf.
        # robot_b rows: same pattern.
        by_robot: dict[str, list[tuple]] = {}
        for r, ts_v, sp, cap in zip(full["robot_id"], full["ts"], full["speed"], full["caption"]):
            by_robot.setdefault(r, []).append((ts_v, sp, cap))

        assert sorted(by_robot["robot_a"], key=lambda x: x[0]) == [
            (100, 12.5, "left turn"),
            (200, 13.1, "straight"),
            (300, 11.8, None),
        ]
        assert sorted(by_robot["robot_b"], key=lambda x: x[0]) == [
            (100, 25.0, None),
            (200, 24.8, "highway"),
            (300, 26.1, "exit"),
        ]

        # Partition filter on the parent partition (robot_id) prunes the
        # stitched group; partition col that varies WITHIN a stitch group
        # (topic) is NULL in stitched rows.
        robot_a = df.where(df["robot_id"] == "robot_a").to_pydict()
        assert len(robot_a["ts"]) == 3
        assert sorted(zip(robot_a["ts"], robot_a["speed"], robot_a["caption"])) == [
            (100, 12.5, "left turn"),
            (200, 13.1, "straight"),
            (300, 11.8, None),
        ]

        # Projection on individual columns works without scanning useless leaves.
        speed_only = df.select("robot_id", "speed").to_pydict()
        assert sorted(speed_only["speed"]) == sorted([12.5, 13.1, 11.8, 25.0, 24.8, 26.1])
        caption_only = df.select("robot_id", "caption").to_pydict()
        caption_non_null = sorted(v for v in caption_only["caption"] if v is not None)
        assert caption_non_null == sorted(["left turn", "straight", "highway", "exit"])

    def test_disjoint_leaf_schemas_only_scans_matching_leaves(self, tmp_path):
        """Selecting a column skips leaves without that column.

        Different leaves may have different schemas (after partial merge or
        topic-keyed partitioning). Selecting a column should only scan the
        leaves that actually have it; leaves without it must be skipped.
        """
        import datetime as _dt

        import lance

        root = str(tmp_path / "ns")
        os.makedirs(root, exist_ok=True)

        # Namespace schema (logical union): topic, ts, scalar_0.
        schema_json = {
            "fields": [
                {"name": "topic", "nullable": False, "type": {"type": "utf8"}, "metadata": {"lance:field_id": "0"}},
                {"name": "ts", "nullable": True, "type": {"type": "int64"}, "metadata": {"lance:field_id": "1"}},
                {
                    "name": "scalar_0",
                    "nullable": True,
                    "type": {"type": "float64"},
                    "metadata": {"lance:field_id": "2"},
                },
            ]
        }
        partition_spec_v1 = {
            "id": 1,
            "fields": [
                {
                    "field_id": "topic",
                    "source_ids": [0],
                    "transform": {"type": "identity"},
                    "result_type": {"type": "utf8"},
                }
            ],
        }

        # Leaf A (topic="A"): only [ts]. Leaf B (topic="B"): [ts, scalar_0].
        leaf_a_schema = pa.schema([pa.field("ts", pa.int64())])
        leaf_a_table = pa.table({"ts": [10, 11, 12]}, schema=leaf_a_schema)
        lance.write_dataset(leaf_a_table, os.path.join(root, "aaaaaaaa_v1$nsa00000000000000$dataset"))

        leaf_b_schema = pa.schema([pa.field("ts", pa.int64()), pa.field("scalar_0", pa.float64())])
        leaf_b_table = pa.table(
            {"ts": [20, 21], "scalar_0": [3.14, 2.71]},
            schema=leaf_b_schema,
        )
        lance.write_dataset(leaf_b_table, os.path.join(root, "bbbbbbbb_v1$nsb00000000000000$dataset"))

        manifest_schema = pa.schema(
            [
                pa.field("object_id", pa.utf8(), nullable=False),
                pa.field("object_type", pa.utf8(), nullable=False),
                pa.field("location", pa.utf8()),
                pa.field("metadata", pa.utf8()),
                pa.field("base_objects", pa.list_(pa.utf8())),
                pa.field("read_version", pa.uint64()),
                pa.field("read_branch", pa.utf8()),
                pa.field("read_tag", pa.utf8()),
                pa.field("partition_field_topic", pa.utf8()),
            ],
            metadata={
                "partition_spec_v1": json.dumps(partition_spec_v1),
                "schema": json.dumps(schema_json),
            },
        )
        rows = {
            "object_id": [
                "v1",
                "v1$nsa00000000000000",
                "v1$nsa00000000000000$dataset",
                "v1$nsb00000000000000",
                "v1$nsb00000000000000$dataset",
            ],
            "object_type": ["namespace", "namespace", "table", "namespace", "table"],
            "location": [
                None,
                None,
                "aaaaaaaa_v1$nsa00000000000000$dataset",
                None,
                "bbbbbbbb_v1$nsb00000000000000$dataset",
            ],
            "metadata": ["{}"] * 5,
            "base_objects": [None] * 5,
            "read_version": [None, None, 1, None, 1],
            "read_branch": [None] * 5,
            "read_tag": [None] * 5,
            "partition_field_topic": [None, "A", "A", "B", "B"],
        }
        arrays = [pa.array(rows[f.name], type=f.type) for f in manifest_schema]
        manifest_table = pa.Table.from_arrays(arrays, schema=manifest_schema)
        lance.write_dataset(manifest_table, os.path.join(root, "__manifest"))

        _ = _dt  # silence

        # Selecting scalar_0 should NOT error; should return only B's 2 rows.
        result = daft.read_lance(root, namespace_partitioning=True).select("scalar_0").to_pydict()
        non_null = sorted(v for v in result["scalar_0"] if v is not None)
        assert non_null == [pytest.approx(2.71), pytest.approx(3.14)]
        # And we should only get rows from B (no NULLs from skipping A).
        assert len(result["scalar_0"]) == 2

    def test_source_ids_resolved_by_lance_field_id_not_position(self, tmp_path):
        """`source_ids` references `lance:field_id`, not Arrow positional index.

        Build a manifest with non-positional field_ids (e.g., 10, 42, 99) and
        verify the reader resolves the partition source field correctly.
        """
        import datetime as _dt

        import lance

        root = str(tmp_path / "ns")
        os.makedirs(root, exist_ok=True)

        schema_json = {
            "fields": [
                {"name": "id", "nullable": False, "type": {"type": "int64"}, "metadata": {"lance:field_id": "10"}},
                {"name": "country", "nullable": True, "type": {"type": "utf8"}, "metadata": {"lance:field_id": "42"}},
                {
                    "name": "event_date",
                    "nullable": True,
                    "type": {"type": "date32"},
                    "metadata": {"lance:field_id": "99"},
                },
            ]
        }
        # Partition spec references source_id 99 -> event_date.
        partition_spec_v1 = {
            "id": 1,
            "fields": [
                {
                    "field_id": "event_date",
                    "source_ids": [99],
                    "transform": {"type": "identity"},
                    "result_type": {"type": "date32"},
                }
            ],
        }

        leaf_pa_schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("country", pa.utf8())])
        leaf_table = pa.table({"id": [1, 2], "country": ["US", "JP"]}, schema=leaf_pa_schema)
        location = "deadbeef_v1$ns0000000000000000$dataset"
        lance.write_dataset(leaf_table, os.path.join(root, location))

        manifest_schema = pa.schema(
            [
                pa.field("object_id", pa.utf8(), nullable=False),
                pa.field("object_type", pa.utf8(), nullable=False),
                pa.field("location", pa.utf8()),
                pa.field("metadata", pa.utf8()),
                pa.field("base_objects", pa.list_(pa.utf8())),
                pa.field("read_version", pa.uint64()),
                pa.field("read_branch", pa.utf8()),
                pa.field("read_tag", pa.utf8()),
                pa.field("partition_field_event_date", pa.date32()),
            ],
            metadata={
                "partition_spec_v1": json.dumps(partition_spec_v1),
                "schema": json.dumps(schema_json),
            },
        )
        rows = {
            "object_id": ["v1", "v1$ns0000000000000000", "v1$ns0000000000000000$dataset"],
            "object_type": ["namespace", "namespace", "table"],
            "location": [None, None, location],
            "metadata": ["{}", "{}", "{}"],
            "base_objects": [None, None, None],
            "read_version": [None, None, 1],
            "read_branch": [None, None, None],
            "read_tag": [None, None, None],
            "partition_field_event_date": [None, _dt.date(2025, 1, 1), _dt.date(2025, 1, 1)],
        }
        arrays = [pa.array(rows[f.name], type=f.type) for f in manifest_schema]
        manifest_table = pa.Table.from_arrays(arrays, schema=manifest_schema)
        lance.write_dataset(manifest_table, os.path.join(root, "__manifest"))

        result = daft.read_lance(root, namespace_partitioning=True).to_pydict()
        assert sorted(result["id"]) == [1, 2]
        assert set(result["country"]) == {"US", "JP"}
        assert all(d == _dt.date(2025, 1, 1) for d in result["event_date"])


# ===========================================================================
# JSON-schema-driven validation: the partition_spec_v<N> JSON and the
# `schema` JSON we emit must conform to the spec's JsonArrowSchema /
# JsonArrowField / JsonArrowDataType model shapes and the partition_spec
# model.
# ===========================================================================


# JSON Schema definitions transcribed from the spec model docs at
# https://github.com/lance-format/lance-namespace/tree/main/docs/src/client/operations/models
# - JsonArrowDataType.md, JsonArrowField.md, JsonArrowSchema.md
# - Partition Spec / Partition Field schemas from partitioning-spec.md
#
# Everything is inlined under one schema with $defs so we don't have to fight
# the jsonschema 4.x / referencing library's registry API.
_SPEC_SCHEMAS = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$defs": {
        "JsonArrowDataType": {
            "type": "object",
            "properties": {
                "type": {"type": "string"},
                "length": {"type": "integer"},
                "fields": {"type": "array", "items": {"$ref": "#/$defs/JsonArrowField"}},
            },
            "required": ["type"],
            "additionalProperties": False,
        },
        "JsonArrowField": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "nullable": {"type": "boolean"},
                "type": {"$ref": "#/$defs/JsonArrowDataType"},
                "metadata": {"type": "object", "additionalProperties": {"type": "string"}},
            },
            "required": ["name", "nullable", "type"],
            "additionalProperties": False,
        },
        "JsonArrowSchema": {
            "type": "object",
            "properties": {
                "fields": {"type": "array", "items": {"$ref": "#/$defs/JsonArrowField"}},
                "metadata": {"type": "object", "additionalProperties": {"type": "string"}},
            },
            "required": ["fields"],
            "additionalProperties": False,
        },
        "PartitionTransform": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": [
                        "identity",
                        "year",
                        "month",
                        "day",
                        "hour",
                        "bucket",
                        "multi_bucket",
                        "truncate",
                    ],
                },
                "num_buckets": {"type": "integer"},
                "width": {"type": "integer"},
            },
            "required": ["type"],
            "additionalProperties": False,
        },
        "PartitionField": {
            "type": "object",
            "properties": {
                "field_id": {"type": "string"},
                "source_ids": {"type": "array", "items": {"type": "integer"}},
                "transform": {"$ref": "#/$defs/PartitionTransform"},
                "expression": {"type": "string"},
                "result_type": {"$ref": "#/$defs/JsonArrowDataType"},
            },
            "required": ["field_id", "source_ids", "result_type"],
            "additionalProperties": False,
        },
        "PartitionSpec": {
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "fields": {"type": "array", "items": {"$ref": "#/$defs/PartitionField"}},
            },
            "required": ["id", "fields"],
            "additionalProperties": False,
        },
    },
}


def _make_validator(def_name: str):
    """Validator pinned to one of the named $defs in _SPEC_SCHEMAS."""
    from jsonschema.validators import Draft202012Validator

    schema = {**_SPEC_SCHEMAS, "$ref": f"#/$defs/{def_name}"}
    return Draft202012Validator(schema)


class TestSpecJsonSchemas:
    """JSON Schema validation of manifest metadata JSON.

    Validate the JSON we write into the manifest's schema metadata against
    JSON Schemas transcribed from the spec's model docs.
    """

    def test_partition_spec_json_conforms(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        spec_json = json.loads(_decode_meta(ds.schema.metadata, "partition_spec_v1"))

        validator = _make_validator("PartitionSpec")
        errors = sorted(validator.iter_errors(spec_json), key=lambda e: e.path)
        assert not errors, "partition_spec_v1 schema violations: " + "; ".join(
            f"{list(e.absolute_path)}: {e.message}" for e in errors
        )

        # Spec rule beyond the JSON shape: exactly one of transform/expression per field.
        for f in spec_json["fields"]:
            has_transform = "transform" in f
            has_expression = "expression" in f
            assert has_transform != has_expression, f

    def test_namespace_schema_json_conforms(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        schema_obj = json.loads(_decode_meta(ds.schema.metadata, "schema"))

        validator = _make_validator("JsonArrowSchema")
        errors = sorted(validator.iter_errors(schema_obj), key=lambda e: e.path)
        assert not errors, "schema JSON violations: " + "; ".join(
            f"{list(e.absolute_path)}: {e.message}" for e in errors
        )

        # Spec rule beyond the JSON shape: every field carries a lance:field_id metadata.
        for f in schema_obj["fields"]:
            assert "metadata" in f, f
            assert "lance:field_id" in f["metadata"], f
            assert isinstance(f["metadata"]["lance:field_id"], str), f

    def test_schema_field_ids_are_unique(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        schema_obj = json.loads(_decode_meta(ds.schema.metadata, "schema"))
        ids = [f["metadata"]["lance:field_id"] for f in schema_obj["fields"]]
        assert len(ids) == len(set(ids))

    def test_partition_source_ids_resolve_to_schema_field_ids(self, namespace_dir, sample_df):
        """Spec invariant: ``source_ids`` reference ``lance:field_id``s in the namespace schema."""
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        spec_json = json.loads(_decode_meta(ds.schema.metadata, "partition_spec_v1"))
        schema_obj = json.loads(_decode_meta(ds.schema.metadata, "schema"))

        schema_field_ids = {int(f["metadata"]["lance:field_id"]) for f in schema_obj["fields"]}
        for f in spec_json["fields"]:
            for sid in f["source_ids"]:
                assert sid in schema_field_ids, (sid, schema_field_ids)


# ===========================================================================
# Strongest available cross-impl validation: round-trip our JSON through the
# canonical Pydantic models generated from the lance-namespace OpenAPI spec
# (https://github.com/lance-format/lance-namespace, on PyPI as
# `lance-namespace-urllib3-client`). Pinned as a dev dependency in
# pyproject.toml so these run as real assertions everywhere.
# ===========================================================================

from lance_namespace_urllib3_client.models import (
    JsonArrowSchema as _OfficialJsonArrowSchema,
)
from lance_namespace_urllib3_client.models import (
    PartitionSpec as _OfficialPartitionSpec,
)


class TestOfficialClientModels:
    """Validation against the canonical generated Pydantic models.

    Validate our written JSON against the lance-namespace OpenAPI spec's
    canonical generated Pydantic models. This is the strictest cross-impl
    check available without a Rust subprocess.
    """

    def test_partition_spec_validates_against_official_model(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        raw = _decode_meta(ds.schema.metadata, "partition_spec_v1")
        # Round-trip through the official PartitionSpec Pydantic model.
        spec_obj = _OfficialPartitionSpec.from_json(raw)
        assert spec_obj is not None
        assert spec_obj.id == 1
        assert len(spec_obj.fields) == 2

        # Each PartitionField should validate (field_id, source_ids, transform OR
        # expression, result_type). Our writer emits transform.
        for f in spec_obj.fields:
            assert f.field_id in ("year", "country")
            assert len(f.source_ids) >= 1
            assert f.transform is not None
            assert f.transform.type == "identity"
            assert f.expression is None
            # result_type validates as JsonArrowDataType.
            assert f.result_type.type is not None

    def test_namespace_schema_validates_against_official_model(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        raw = _decode_meta(ds.schema.metadata, "schema")
        schema_obj = _OfficialJsonArrowSchema.from_json(raw)
        assert schema_obj is not None
        names = {f.name for f in schema_obj.fields}
        assert names == {"year", "value", "name", "country"}
        # Every field carries the lance:field_id metadata.
        for f in schema_obj.fields:
            assert f.metadata is not None
            assert "lance:field_id" in f.metadata
            # JsonArrowField.type validates as JsonArrowDataType
            assert f.type.type is not None

    def test_partition_field_round_trip_matches_official(self, namespace_dir, sample_df):
        """Pydantic dump-then-reload of the spec's PartitionSpec should match what we wrote."""
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        ds, _ = _open_manifest(namespace_dir)
        raw = _decode_meta(ds.schema.metadata, "partition_spec_v1")
        original = json.loads(raw)
        roundtripped = json.loads(_OfficialPartitionSpec.from_json(raw).to_json())
        # Pydantic's by_alias dump drops `None` keys; comparison should match
        # for our writes (we never emit `expression` or unused transform params).
        assert roundtripped == original
