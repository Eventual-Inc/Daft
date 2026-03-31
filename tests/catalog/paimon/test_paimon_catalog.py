"""Tests for PaimonCatalog and PaimonTable via the daft.catalog abstraction."""

from __future__ import annotations

import pyarrow as pa
import pytest

import daft
from daft.catalog import Catalog, Identifier, NotFoundError, Table
from daft.catalog.__paimon import PaimonCatalog, PaimonTable

pypaimon = pytest.importorskip("pypaimon")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def inner_catalog(tmp_path):
    """A bare pypaimon FileSystemCatalog with a 'test_db' database."""
    catalog = pypaimon.CatalogFactory.create({"warehouse": str(tmp_path)})
    catalog.create_database("test_db", ignore_if_exists=True)
    return catalog, tmp_path


@pytest.fixture
def inner_catalog_with_table(inner_catalog):
    """A pypaimon catalog pre-populated with an append-only partitioned table."""
    catalog, tmp_path = inner_catalog
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("dt", pa.string()),
            ]
        ),
        partition_keys=["dt"],
        options={"bucket": "1", "file.format": "parquet"},
    )
    catalog.create_table("test_db.test_table", schema, ignore_if_exists=True)

    # Pre-populate with data
    table = catalog.get_table("test_db.test_table")
    data = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "name": pa.array(["alice", "bob", "carol"]),
            "dt": pa.array(["2024-01-01", "2024-01-01", "2024-01-02"]),
        }
    )
    write_builder = table.new_batch_write_builder()
    table_write = write_builder.new_write()
    table_commit = write_builder.new_commit()
    try:
        table_write.write_arrow(data)
        commit_messages = table_write.prepare_commit()
        table_commit.commit(commit_messages)
    finally:
        table_write.close()
        table_commit.close()

    return catalog, tmp_path


@pytest.fixture
def paimon_catalog(inner_catalog_with_table):
    """Daft PaimonCatalog wrapping the pre-populated inner catalog."""
    catalog, tmp_path = inner_catalog_with_table
    return Catalog.from_paimon(catalog, name="test_paimon"), catalog, tmp_path


# ---------------------------------------------------------------------------
# PaimonCatalog — basic properties
# ---------------------------------------------------------------------------


def test_catalog_name(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    assert daft_catalog.name == "test_paimon"


def test_catalog_default_name(inner_catalog):
    inner, _ = inner_catalog
    daft_catalog = Catalog.from_paimon(inner)
    assert daft_catalog.name == "paimon"


def test_catalog_from_paimon_invalid_type():
    with pytest.raises(ValueError, match="Unsupported paimon catalog type"):
        Catalog.from_paimon("not_a_catalog")


# ---------------------------------------------------------------------------
# PaimonCatalog — namespace operations
# ---------------------------------------------------------------------------


def test_catalog_has_namespace(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    assert daft_catalog.has_namespace("test_db")
    assert not daft_catalog.has_namespace("nonexistent_db")


def test_catalog_list_namespaces(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    namespaces = daft_catalog.list_namespaces()
    assert Identifier("test_db") in namespaces


def test_catalog_create_namespace(tmp_path):
    inner = pypaimon.CatalogFactory.create({"warehouse": str(tmp_path)})
    daft_catalog = Catalog.from_paimon(inner)
    daft_catalog.create_namespace("new_db")
    assert daft_catalog.has_namespace("new_db")


def test_catalog_create_namespace_if_not_exists(tmp_path):
    inner = pypaimon.CatalogFactory.create({"warehouse": str(tmp_path)})
    daft_catalog = Catalog.from_paimon(inner)
    daft_catalog.create_namespace_if_not_exists("myns")
    daft_catalog.create_namespace_if_not_exists("myns")  # should not raise
    assert daft_catalog.has_namespace("myns")


def test_catalog_drop_namespace_not_supported(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    with pytest.raises(NotImplementedError):
        daft_catalog.drop_namespace("test_db")


# ---------------------------------------------------------------------------
# PaimonCatalog — table operations
# ---------------------------------------------------------------------------


def test_catalog_has_table(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    assert daft_catalog.has_table("test_db.test_table")
    assert not daft_catalog.has_table("test_db.nonexistent_table")
    assert not daft_catalog.has_table("nonexistent_db.test_table")


def test_catalog_list_tables(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    tables = daft_catalog.list_tables()
    assert Identifier("test_db", "test_table") in tables


def test_catalog_list_tables_with_pattern(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    tables = daft_catalog.list_tables(pattern="test_db")
    assert Identifier("test_db", "test_table") in tables
    tables_no_match = daft_catalog.list_tables(pattern="other_db")
    assert len(tables_no_match) == 0


def test_catalog_get_table(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    table = daft_catalog.get_table("test_db.test_table")
    assert table.name == "test_table"
    assert isinstance(table, Table)


def test_catalog_get_table_not_found(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    with pytest.raises(NotFoundError):
        daft_catalog.get_table("test_db.nonexistent_table")


def test_catalog_drop_table_not_supported(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    with pytest.raises(NotImplementedError):
        daft_catalog.drop_table("test_db.test_table")


def test_catalog_create_table(tmp_path):
    inner = pypaimon.CatalogFactory.create({"warehouse": str(tmp_path)})
    inner.create_database("mydb", ignore_if_exists=True)
    daft_catalog = Catalog.from_paimon(inner)

    schema = daft.from_pydict({"id": [1, 2], "name": ["a", "b"]}).schema()
    table = daft_catalog.create_table("mydb.new_table", schema)
    assert table.name == "new_table"
    assert daft_catalog.has_table("mydb.new_table")


def test_catalog_create_table_with_partitions(tmp_path):
    from daft.io.partitioning import PartitionField

    inner = pypaimon.CatalogFactory.create({"warehouse": str(tmp_path)})
    inner.create_database("mydb", ignore_if_exists=True)
    daft_catalog = Catalog.from_paimon(inner)

    df = daft.from_pydict({"id": [1], "name": ["a"], "dt": ["2024-01-01"]})
    schema = df.schema()
    dt_field = schema["dt"]
    partition_fields = [PartitionField.create(dt_field)]
    table = daft_catalog.create_table("mydb.part_table", schema, partition_fields=partition_fields)
    assert table.name == "part_table"
    assert table.partition_keys == ["dt"]


# ---------------------------------------------------------------------------
# PaimonCatalog — read / write through catalog API
# ---------------------------------------------------------------------------


def test_catalog_read_table(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    df = daft_catalog.read_table("test_db.test_table")
    result = df.sort("id").to_pydict()
    assert result["id"] == [1, 2, 3]
    assert result["name"] == ["alice", "bob", "carol"]


def test_catalog_write_table_append(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    new_data = daft.from_pydict(
        {
            "id": [4, 5],
            "name": ["dave", "eve"],
            "dt": ["2024-01-03", "2024-01-03"],
        }
    )
    daft_catalog.write_table("test_db.test_table", new_data, mode="append")
    result = daft_catalog.read_table("test_db.test_table").sort("id").to_pydict()
    assert result["id"] == [1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# Table.from_paimon — direct table wrapping
# ---------------------------------------------------------------------------


def test_table_from_paimon(inner_catalog_with_table):
    inner, _ = inner_catalog_with_table
    inner_table = inner.get_table("test_db.test_table")
    table = Table.from_paimon(inner_table)
    assert isinstance(table, Table)
    assert table.name == "test_table"


def test_table_from_paimon_invalid_type():
    with pytest.raises(ValueError, match="Unsupported paimon table type"):
        Table.from_paimon("not_a_table")


# ---------------------------------------------------------------------------
# PaimonTable — read / write
# ---------------------------------------------------------------------------


def test_table_read(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    table = daft_catalog.get_table("test_db.test_table")
    df = table.read()
    result = df.sort("id").to_pydict()
    assert result["id"] == [1, 2, 3]


def test_table_select(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    table = daft_catalog.get_table("test_db.test_table")
    df = table.select("id", "name").sort("id")
    assert "dt" not in df.schema().column_names()
    assert df.to_pydict()["id"] == [1, 2, 3]


def test_table_append(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    table = daft_catalog.get_table("test_db.test_table")
    new_rows = daft.from_pydict({"id": [99], "name": ["zara"], "dt": ["2024-03-01"]})
    table.append(new_rows)
    ids = sorted(table.read().to_pydict()["id"])
    assert 99 in ids


def test_table_overwrite(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    table = daft_catalog.get_table("test_db.test_table")
    replacement = daft.from_pydict({"id": [100, 200], "name": ["p", "q"], "dt": ["2024-05-01", "2024-05-02"]})
    table.overwrite(replacement)
    result = sorted(table.read().to_pydict()["id"])
    assert result == [100, 200]


def test_table_write_dispatch(paimon_catalog):
    daft_catalog, _, _ = paimon_catalog
    table = daft_catalog.get_table("test_db.test_table")
    extra = daft.from_pydict({"id": [50], "name": ["extra"], "dt": ["2024-06-01"]})
    table.write(extra, mode="append")
    ids = table.read().to_pydict()["id"]
    assert 50 in ids


# ---------------------------------------------------------------------------
# PaimonTable — properties
# ---------------------------------------------------------------------------


class TestPaimonTableProperties:
    """Tests for PaimonTable properties."""

    @pytest.fixture
    def pk_catalog(self, tmp_path):
        """Create a catalog with primary key table for testing properties."""
        inner = pypaimon.CatalogFactory.create({"warehouse": str(tmp_path)})
        inner.create_database("test_db", ignore_if_exists=True)

        # Create primary key table
        schema = pypaimon.Schema.from_pyarrow_schema(
            pa.schema([
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.int64()),
            ]),
            primary_keys=["id"],
            options={"bucket": "2"},
        )
        inner.create_table("test_db.pk_table", schema, ignore_if_exists=True)

        # Create partitioned primary key table
        schema2 = pypaimon.Schema.from_pyarrow_schema(
            pa.schema([
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("dt", pa.string()),
            ]),
            partition_keys=["dt"],
            primary_keys=["id"],
            options={"bucket": "1"},
        )
        inner.create_table("test_db.partitioned_pk", schema2, ignore_if_exists=True)

        return Catalog.from_paimon(inner)

    def test_append_only_table_properties(self, paimon_catalog):
        daft_catalog, _, _ = paimon_catalog
        table = daft_catalog.get_table("test_db.test_table")

        # Verify append-only table properties
        assert table.is_primary_key_table is False
        assert table.primary_keys == []
        assert table.partition_keys == ["dt"]

    def test_primary_key_table_properties(self, pk_catalog):
        table = pk_catalog.get_table("test_db.pk_table")

        # Verify primary key table properties
        assert table.is_primary_key_table is True
        assert table.primary_keys == ["id"]
        assert table.partition_keys == []
        assert table.bucket_count == 2
        assert table.table_options.get("bucket") == "2"

    def test_partitioned_primary_key_table_properties(self, pk_catalog):
        table = pk_catalog.get_table("test_db.partitioned_pk")

        # Verify partitioned primary key table properties
        assert table.is_primary_key_table is True
        assert table.primary_keys == ["id"]
        assert table.partition_keys == ["dt"]
        assert table.bucket_count == 1

    def test_table_schema(self, paimon_catalog):
        daft_catalog, _, _ = paimon_catalog
        table = daft_catalog.get_table("test_db.test_table")
        schema = table.schema()

        assert "id" in schema.column_names()
        assert "name" in schema.column_names()
        assert "dt" in schema.column_names()


# ---------------------------------------------------------------------------
# Session integration
# ---------------------------------------------------------------------------


def test_session_sql_select(paimon_catalog):
    from daft.session import Session

    daft_catalog, _, _ = paimon_catalog
    sess = Session()
    sess.attach(daft_catalog, "test_paimon")
    sess.set_namespace("test_db")

    df = sess.sql("SELECT id, name FROM test_table ORDER BY id")
    result = df.to_pydict()
    assert result["id"] == [1, 2, 3]
    assert result["name"] == ["alice", "bob", "carol"]


def test_session_read_table(paimon_catalog):
    from daft.session import Session

    daft_catalog, _, _ = paimon_catalog
    sess = Session()
    sess.attach(daft_catalog, "test_paimon")

    df = sess.read_table("test_paimon.test_db.test_table")
    assert df.count_rows() == 3

