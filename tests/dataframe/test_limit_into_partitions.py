from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import daft
from tests.conftest import get_tests_daft_runner_name

pytestmark = pytest.mark.skipif(
    get_tests_daft_runner_name() != "ray",
    reason="IntoPartitions requires Ray runner to be in use",
)


@pytest.fixture
def partitioned_limit_source(tmp_path):
    # Multiple scan tasks keep IntoPartitions in the physical plan.
    for part in range(4):
        ids = list(range(part * 5, (part + 1) * 5))
        pq.write_table(
            pa.table({"id": ids, "value": [None if value % 3 == 0 else "duplicate" for value in ids]}),
            tmp_path / f"part-{part}.parquet",
        )
    return daft.read_parquet(str(tmp_path / "*.parquet"))


@pytest.mark.timeout(60, method="thread")
@pytest.mark.parametrize("num_partitions", [2, 4, 8])
@pytest.mark.parametrize("limit", [0, 1, 10, 20, 100])
@pytest.mark.parametrize("limit_first", [True, False])
def test_into_partitions_with_limit(partitioned_limit_source, num_partitions, limit, limit_first):
    source = partitioned_limit_source
    if limit_first:
        result = source.limit(limit).into_partitions(num_partitions)
    else:
        # PushDownLimit can put Limit below IntoPartitions in this order too.
        result = source.into_partitions(num_partitions).limit(limit)

    assert result.schema() == source.schema()
    rows = result.to_pylist()
    ids = [row["id"] for row in rows]
    assert len(rows) == min(limit, 20)
    assert len(set(ids)) == len(ids)
    assert set(ids) <= set(range(20))
    for row in rows:
        assert row["value"] == (None if row["id"] % 3 == 0 else "duplicate")
    if limit >= 20:
        assert set(ids) == set(range(20))
    assert len(list(result.iter_partitions())) == num_partitions


@pytest.mark.timeout(60, method="thread")
@pytest.mark.parametrize("num_partitions", [2, 4, 8])
@pytest.mark.parametrize("offset,expected_rows", [(3, 7), (18, 2), (25, 0)])
def test_into_partitions_with_limit_offset(partitioned_limit_source, num_partitions, offset, expected_rows):
    result = partitioned_limit_source.offset(offset).limit(7).into_partitions(num_partitions)
    rows = result.to_pylist()
    assert len(rows) == expected_rows
    assert len({row["id"] for row in rows}) == expected_rows


@pytest.mark.timeout(60, method="thread")
def test_into_partitions_with_limit_after_empty_filter(partitioned_limit_source):
    result = partitioned_limit_source.where(daft.col("id") < 0).limit(10).into_partitions(2)
    assert result.to_pydict() == {"id": [], "value": []}
    assert result.schema() == partitioned_limit_source.schema()


@pytest.mark.timeout(60, method="thread")
@pytest.mark.parametrize("num_partitions", [4, 8])
def test_into_partitions_with_limit_flight(partitioned_limit_source, tmp_path, num_partitions):
    with daft.execution_config_ctx(shuffle_algorithm="flight_shuffle", flight_shuffle_dirs=[str(tmp_path / "shuffle")]):
        result = partitioned_limit_source.limit(1).into_partitions(num_partitions)
        assert len(result.to_pylist()) == 1
        assert result.schema() == partitioned_limit_source.schema()
        assert len(list(result.iter_partitions())) == num_partitions
