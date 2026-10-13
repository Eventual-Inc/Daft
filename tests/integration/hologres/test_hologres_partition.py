from __future__ import annotations

import pytest

import daft
from daft.catalog import Identifier
from daft.catalog.__hologres import hologres_connection
from daft.datatype import DataType
from tests.integration.hologres.conftest import unique_table


@pytest.fixture()
def partitioned_table(hologres_uri: str, cleanup_tables):
    """A logical partition table PARTITION BY LIST (ds) with PK (ds, id)."""
    schema = daft.Schema._from_field_name_and_types(
        [
            ("ds", DataType.string()),
            ("id", DataType.int64()),
            ("name", DataType.string()),
        ]
    )
    ident = Identifier("public", unique_table())
    cleanup_tables(str(ident))
    daft.Catalog.from_hologres(hologres_uri).create_table(
        ident,
        schema,
        properties={"partition_by": "ds", "primary_key": "id"},
    )
    yield f"public.{ident[1]}"


def _partition_df(days: list[str], per_day: int = 3) -> daft.DataFrame:
    ids = list(range(1, days.__len__() * per_day + 1))
    ds_col = []
    name_col = []
    for d in days:
        for i in range(1, per_day + 1):
            ds_col.append(d)
            name_col.append(f"{d}_row_{i}")
    return daft.from_pydict({"ds": ds_col, "id": ids, "name": name_col})


@pytest.mark.integration()
def test_partition_table_roundtrip(hologres_uri: str, partitioned_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(partitioned_table)

    # metadata: logical partition column detected
    holo = table._load_hologres_schema()
    assert holo.partition_column == "ds"
    assert holo.is_partition_parent

    table.append(_partition_df(["2024-01-01", "2024-01-02"], 3))

    # default read auto-selects the partition column for parallel reads
    out = table.read().sort("id").to_pydict()
    assert out["id"] == list(range(1, 7))
    assert sorted(out["ds"]) == ["2024-01-01"] * 3 + ["2024-01-02"] * 3


@pytest.mark.integration()
def test_partition_table_overwrite(hologres_uri: str, partitioned_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(partitioned_table)
    table.append(_partition_df(["2024-01-01", "2024-01-02"], 3))

    table.overwrite(_partition_df(["2024-02-01"], 4))

    out = table.read().sort("id").to_pydict()
    assert len(out["id"]) == 4
    assert set(out["ds"]) == {"2024-02-01"}


@pytest.mark.integration()
def test_partition_table_explicit_partition_col_read(hologres_uri: str, partitioned_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(partitioned_table)
    table.append(_partition_df(["2024-01-01", "2024-01-02", "2024-01-03"], 3))

    df = daft.read_hologres(partitioned_table, hologres_uri, partition_col="ds", num_partitions=2)
    out = df.sort("id").to_pydict()
    assert out["id"] == list(range(1, 10))
    assert len(set(out["ds"])) == 3


@pytest.mark.integration()
def test_partition_table_listed_once(hologres_uri: str, partitioned_table) -> None:
    catalog = daft.Catalog.from_hologres(hologres_uri)
    table = catalog.get_table(partitioned_table)
    table.append(_partition_df(["2024-01-01"], 2))

    tables = catalog.list_tables()
    names = [str(t) for t in tables]
    assert names.count(partitioned_table) == 1


@pytest.mark.integration()
def test_partition_table_schema_includes_partition_key_in_pk(hologres_uri: str, partitioned_table) -> None:
    """Hologres requires the partition column inside the primary key."""
    with hologres_connection(hologres_uri) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT a.attname FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid "
            "AND a.attnum = ANY(i.indkey) WHERE i.indrelid = %s::regclass AND i.indisprimary",
            (partitioned_table,),
        )
        pk = {row[0] for row in cur.fetchall()}
    assert {"ds", "id"} <= pk
