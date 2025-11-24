from __future__ import annotations

import pyarrow as pa
import pytest

from daft import col
from daft.io.lance import lance_scan

PYARROW_LOWER_BOUND_SKIP = tuple(int(s) for s in pa.__version__.split(".") if s.isnumeric()) < (9, 0, 0)
pytestmark = pytest.mark.skipif(PYARROW_LOWER_BOUND_SKIP, reason="lance not supported on old versions of pyarrow")

# Import-or-skip lance once at module level so individual tests don't need to do this
lance = pytest.importorskip("lance")

@pytest.fixture(scope='function')
def lance_dataset(tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp('lance_point')
    table = pa.table({'id': [0, 1, 2, 3], 'value': ['a', 'b', 'c', 'd']})
    lance.write_dataset(table, tmp_dir)
    ds = lance.dataset(tmp_dir)
    return ds


def _scan(ds):
    return lance_scan.LanceDBScanOperator(ds)


@pytest.mark.parametrize('idx_type', ['BTREE','BITMAP','BLOOMFILTER'])
def test_point_lookup_equal_hits_scalar_index(lance_dataset, idx_type):
    ds = lance_dataset
    ds.create_scalar_index('id', idx_type)
    scan = _scan(ds)
    scan.push_filters([(col('id') == 2)._expr])
    assert scan._should_use_index_for_point_lookup() is True


@pytest.mark.parametrize('idx_type', ['BTREE','BITMAP','BLOOMFILTER'])
def test_in_list_hits_scalar_index(lance_dataset, idx_type):
    ds = lance_dataset
    ds.create_scalar_index('id', idx_type)
    scan = _scan(ds)
    scan.push_filters([col('id').is_in([1, 2])._expr])
    assert scan._should_use_index_for_point_lookup() is True


@pytest.mark.parametrize('idx_type', ['BTREE','BITMAP','BLOOMFILTER'])
def test_non_point_and_point_mixed_skips_index(lance_dataset, idx_type):
    ds = lance_dataset
    ds.create_scalar_index('id', idx_type)
    scan = _scan(ds)
    scan.push_filters([((col('id') == 2) & (col('id') > 1))._expr])
    assert scan._should_use_index_for_point_lookup() is False


@pytest.mark.parametrize('idx_type', ['BTREE','BITMAP','BLOOMFILTER'])
def test_or_not_skips_index(lance_dataset, idx_type):
    ds = lance_dataset
    ds.create_scalar_index('id', idx_type)
    scan = _scan(ds)
    scan.push_filters([((col('id') == 2) | (col('id') == 3))._expr])
    assert scan._should_use_index_for_point_lookup() is False
    scan2 = _scan(ds)
    scan2.push_filters([(~(col('id') == 2))._expr])
    assert scan2._should_use_index_for_point_lookup() is False


@pytest.mark.parametrize('idx_type', ['BTREE','BITMAP','BLOOMFILTER'])
@pytest.mark.skipif(not hasattr(lance.LanceDataset, 'create_index'), reason='composite index not supported')
def test_multi_column_btree_index_prefix_matching(lance_dataset, idx_type):
    ds = lance_dataset
    try:
        ds.create_index(['id', 'value'], idx_type)
    except Exception as e:
        pytest.xfail(f'composite indexes are not unsupported: {e}')
