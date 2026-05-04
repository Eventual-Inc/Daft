from __future__ import annotations

import pytest

import daft

EXPECTED_PARTITIONED_TXN_IDS = sorted(
    [
        "TXN-001",
        "TXN-003",
        "TXN-007",
        "TXN-008",
        "TXN-011",
        "TXN-012",
        "TXN-013",
        "TXN-014",
        "TXN-015",
        "TXN-016",
        "TXN-017",
        "TXN-018",
    ]
)

EXPECTED_NONPART_TXN_IDS = sorted(
    [
        "TXN-001",
        "TXN-003",
        "TXN-004",
        "TXN-006",
        "TXN-007",
        "TXN-008",
        "TXN-009",
        "TXN-010",
        "TXN-011",
        "TXN-012",
        "TXN-013",
        "TXN-014",
        "TXN-015",
        "TXN-016",
    ]
)

EXPECTED_TIMEBASED_NONHIVESTYLE_TXN_IDS = sorted(
    [
        "TXN-001",
        "TXN-001",
        "TXN-003",
        "TXN-004",
        "TXN-005",
    ]
)

EXPECTED_TIMEBASED_EPOCH_TXN_IDS = sorted(
    [
        "TXN-001",
        "TXN-001",
        "TXN-002",
        "TXN-003",
        "TXN-005",
        "TXN-006",
    ]
)

EXPECTED_TIMEBASED_UNIX_TXN_IDS = sorted(
    [
        "TXN-001",
        "TXN-001",
        "TXN-002",
        "TXN-003",
        "TXN-005",
        "TXN-006",
    ]
)


@pytest.mark.integration()
def test_read_schema(v9_table):
    df = daft.read_hudi(v9_table)
    col_names = df.schema().column_names()
    assert "txn_id" in col_names
    assert "amount" in col_names
    assert "currency" in col_names


@pytest.mark.integration()
def test_read_partitioned_record_count(v9_partitioned_table):
    df = daft.read_hudi(v9_partitioned_table)
    result = df.select("txn_id").sort("txn_id").to_pydict()
    assert result["txn_id"] == EXPECTED_PARTITIONED_TXN_IDS


@pytest.mark.integration()
def test_read_nonpartitioned_record_count(v9_nonpartitioned_table):
    df = daft.read_hudi(v9_nonpartitioned_table)
    result = df.select("txn_id").sort("txn_id").to_pydict()
    assert result["txn_id"] == EXPECTED_NONPART_TXN_IDS


@pytest.mark.integration()
@pytest.mark.parametrize(
    "table_name,expected_ids",
    [
        ("v9_timebasedkeygen_nonhivestyle", EXPECTED_TIMEBASED_NONHIVESTYLE_TXN_IDS),
        ("v9_timebasedkeygen_epochmillis", EXPECTED_TIMEBASED_EPOCH_TXN_IDS),
        ("v9_timebasedkeygen_unixtimestamp", EXPECTED_TIMEBASED_UNIX_TXN_IDS),
    ],
)
def test_read_timebasedkeygen_record_count(table_name, expected_ids, tmp_path):
    from tests.integration.hudi.conftest import _extract_table

    table_path = _extract_table(table_name, tmp_path)
    df = daft.read_hudi(table_path)
    result = df.select("txn_id").sort("txn_id").to_pydict()
    assert result["txn_id"] == expected_ids


@pytest.mark.integration()
def test_column_projection(v9_table):
    df = daft.read_hudi(v9_table)
    projected = df.select("txn_id", "amount", "currency").sort("txn_id").to_pydict()
    assert len(projected["txn_id"]) > 0
    assert len(projected["amount"]) == len(projected["txn_id"])
    assert len(projected["currency"]) == len(projected["txn_id"])


@pytest.mark.integration()
def test_mutated_records(v9_txns_simple_meta):
    df = daft.read_hudi(v9_txns_simple_meta)
    txn001 = df.where(daft.col("txn_id") == "TXN-001").to_pydict()
    assert txn001["txn_type"] == ["reversal"]

    txn007 = df.where(daft.col("txn_id") == "TXN-007").to_pydict()
    assert float(txn007["fee_amount"][0]) == pytest.approx(75.00)


@pytest.mark.integration()
def test_deleted_records_absent(v9_txns_simple_meta):
    df = daft.read_hudi(v9_txns_simple_meta)
    txn_ids = df.select("txn_id").to_pydict()["txn_id"]
    assert "TXN-002" not in txn_ids
    assert "TXN-005" not in txn_ids


@pytest.mark.integration()
def test_partition_values(v9_txns_simple_meta):
    df = daft.read_hudi(v9_txns_simple_meta)
    regions = sorted(set(df.select("region").to_pydict()["region"]))
    assert regions == ["apac", "eu", "us"]


@pytest.mark.integration()
def test_partition_record_distribution(v9_txns_simple_meta):
    df = daft.read_hudi(v9_txns_simple_meta)
    by_region = {}
    for txn_id, region in zip(
        df.select("txn_id", "region").to_pydict()["txn_id"],
        df.select("txn_id", "region").to_pydict()["region"],
    ):
        by_region.setdefault(region, []).append(txn_id)
    assert len(by_region["us"]) == 5
    assert len(by_region["eu"]) == 3
    assert len(by_region["apac"]) == 4


@pytest.mark.integration()
def test_decimal_precision(v9_table):
    df = daft.read_hudi(v9_table)
    amounts = df.select("txn_id", "amount").where(daft.col("txn_id") == "TXN-001").to_pydict()
    assert len(amounts["amount"]) >= 1
    assert all(a is not None for a in amounts["amount"])


@pytest.mark.integration()
def test_read_with_limit(v9_table):
    df = daft.read_hudi(v9_table)
    result = df.limit(3).collect()
    assert len(result) <= 3
