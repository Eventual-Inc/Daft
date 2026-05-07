from __future__ import annotations

import json
import os
import tempfile

import pytest

import daft


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


class TestPartitionedWrite:
    def test_basic_write(self, namespace_dir, sample_df):
        result = sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        result_dict = result.to_pydict()
        assert result_dict["num_partitions"] == [4]
        assert result_dict["num_fragments"] == [4]

    def test_manifest_created(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        import lance

        manifest_ds = lance.dataset(os.path.join(namespace_dir, "__manifest"))
        manifest_table = manifest_ds.to_table()
        assert manifest_table.num_rows == 4
        assert "_dataset_path" in manifest_table.column_names
        assert "partition_field_year" in manifest_table.column_names
        assert "partition_field_country" in manifest_table.column_names

    def test_manifest_metadata(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        import lance

        manifest_ds = lance.dataset(os.path.join(namespace_dir, "__manifest"))
        metadata = manifest_ds.schema.metadata
        assert b"partition_spec_v1" in metadata
        spec = json.loads(metadata[b"partition_spec_v1"])
        assert spec["id"] == 1
        assert len(spec["fields"]) == 2

    def test_partition_dirs_are_random_base36(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        import lance

        manifest_ds = lance.dataset(os.path.join(namespace_dir, "__manifest"))
        manifest_table = manifest_ds.to_table()
        for i in range(manifest_table.num_rows):
            path = manifest_table.column("_dataset_path")[i].as_py()
            parts = path.split("$")
            assert len(parts) == 3
            assert parts[0] == "v1"
            assert parts[2] == "dataset"
            dir_name = parts[1]
            assert len(dir_name) == 16
            assert all(c in "abcdefghijklmnopqrstuvwxyz0123456789" for c in dir_name)

    def test_partition_columns_stripped_from_data(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        import lance

        manifest_ds = lance.dataset(os.path.join(namespace_dir, "__manifest"))
        manifest_table = manifest_ds.to_table()
        path = manifest_table.column("_dataset_path")[0].as_py()
        partition_ds = lance.dataset(os.path.join(namespace_dir, path))
        schema_names = set(partition_ds.schema.names)
        assert "year" not in schema_names
        assert "country" not in schema_names
        assert "value" in schema_names
        assert "name" in schema_names

    def test_single_partition_col(self, namespace_dir):
        df = daft.from_pydict({"region": ["east", "west", "east"], "val": [1, 2, 3]})
        result = df.write_lance(namespace_dir, partition_cols=["region"], mode="create")
        assert result.to_pydict()["num_partitions"] == [2]

    def test_create_mode_fails_if_exists(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")
        with pytest.raises(ValueError, match="already exists"):
            sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="create")

    def test_overwrite_mode(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df2 = daft.from_pydict({"year": [2026], "country": ["JP"], "value": [100], "name": ["kenji"]})
        df2.write_lance(namespace_dir, partition_cols=["year", "country"], mode="overwrite")
        read_back = daft.read_lance(namespace_dir, namespace_partitioning=True).collect()
        assert read_back.to_pydict()["year"] == [2026]

    def test_append_to_existing_partition_no_duplicates(self, namespace_dir):
        df1 = daft.from_pydict({"key": ["a", "b"], "val": [1, 2]})
        df1.write_lance(namespace_dir, partition_cols=["key"], mode="create")

        df2 = daft.from_pydict({"key": ["a", "c"], "val": [3, 4]})
        df2.write_lance(namespace_dir, partition_cols=["key"], mode="append")

        import lance

        manifest = lance.dataset(os.path.join(namespace_dir, "__manifest")).to_table()
        paths = manifest.column("_dataset_path").to_pylist()
        assert len(paths) == len(set(paths)), f"Duplicate manifest rows: {paths}"
        assert manifest.num_rows == 3

        read_back = daft.read_lance(namespace_dir, namespace_partitioning=True).collect()
        result = read_back.to_pydict()
        assert sorted(result["val"]) == [1, 2, 3, 4]

    def test_partition_cols_with_merge_mode_raises(self, namespace_dir, sample_df):
        with pytest.raises(ValueError, match="partition_cols is not supported with merge mode"):
            sample_df.write_lance(namespace_dir, partition_cols=["year"], mode="merge")


class TestPartitionedRead:
    def test_read_all_data(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        read_df = daft.read_lance(namespace_dir, namespace_partitioning=True).collect()
        read_dict = read_df.to_pydict()
        assert sorted(read_dict["value"]) == [10, 20, 30, 40, 50, 60]
        assert set(read_dict["year"]) == {2024, 2025}
        assert set(read_dict["country"]) == {"US", "UK"}

    def test_partition_filter_single_col(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.where(df["year"] == 2024).collect()
        result_dict = result.to_pydict()
        assert all(y == 2024 for y in result_dict["year"])
        assert sorted(result_dict["value"]) == [10, 20, 50]

    def test_partition_filter_string_col(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.where(df["country"] == "US").collect()
        result_dict = result.to_pydict()
        assert all(c == "US" for c in result_dict["country"])
        assert sorted(result_dict["value"]) == [10, 30, 50, 60]

    def test_partition_filter_combined(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.where((df["year"] == 2025) & (df["country"] == "UK")).collect()
        result_dict = result.to_pydict()
        assert result_dict["year"] == [2025]
        assert result_dict["country"] == ["UK"]
        assert result_dict["value"] == [40]

    def test_data_filter(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.where(df["value"] > 25).collect()
        assert all(v > 25 for v in result.to_pydict()["value"])

    def test_combined_partition_and_data_filter(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.where((df["year"] == 2024) & (df["value"] > 15)).collect()
        result_dict = result.to_pydict()
        assert all(y == 2024 for y in result_dict["year"])
        assert all(v > 15 for v in result_dict["value"])

    def test_column_projection(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.select("value", "country").collect()
        result_dict = result.to_pydict()
        assert set(result_dict.keys()) == {"value", "country"}
        assert len(result_dict["value"]) == 6

    def test_column_projection_partition_col_only(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        result = df.select("year").collect()
        result_dict = result.to_pydict()
        assert set(result_dict.keys()) == {"year"}
        assert sorted(result_dict["year"]) == [2024, 2024, 2024, 2025, 2025, 2025]

    def test_schema_includes_partition_columns(self, namespace_dir, sample_df):
        sample_df.write_lance(namespace_dir, partition_cols=["year", "country"], mode="create")
        df = daft.read_lance(namespace_dir, namespace_partitioning=True)
        schema_names = [f.name for f in df.schema()]
        assert "year" in schema_names
        assert "country" in schema_names
        assert "value" in schema_names
        assert "name" in schema_names


class TestRoundTrip:
    def test_roundtrip_preserves_data(self, namespace_dir):
        original = {"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [10.0, 20.0, 30.0]}
        df = daft.from_pydict(original)
        df.write_lance(namespace_dir, partition_cols=["b"], mode="create")
        read_back = daft.read_lance(namespace_dir, namespace_partitioning=True).sort(daft.col("a")).collect()
        result = read_back.to_pydict()
        assert result["a"] == [1, 2, 3]
        assert result["b"] == ["x", "y", "z"]
        assert result["c"] == [10.0, 20.0, 30.0]

    def test_roundtrip_with_nulls(self, namespace_dir):
        df = daft.from_pydict(
            {
                "key": ["a", "a", "b", "b"],
                "val": [1, None, 3, None],
            }
        )
        df.write_lance(namespace_dir, partition_cols=["key"], mode="create")
        read_back = daft.read_lance(namespace_dir, namespace_partitioning=True).collect()
        result = read_back.to_pydict()
        assert sorted(result["key"]) == ["a", "a", "b", "b"]
        assert set(v for v in result["val"] if v is not None) == {1, 3}
        assert result["val"].count(None) == 2

    def test_roundtrip_many_partitions(self, namespace_dir):
        n = 50
        df = daft.from_pydict({"part": list(range(n)), "data": list(range(n))})
        result = df.write_lance(namespace_dir, partition_cols=["part"], mode="create")
        assert result.to_pydict()["num_partitions"] == [n]
        read_back = daft.read_lance(namespace_dir, namespace_partitioning=True).collect()
        assert len(read_back.to_pydict()["part"]) == n
