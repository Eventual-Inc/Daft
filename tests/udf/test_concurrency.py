import pytest
import daft
from daft import DataType

# Legacy UDF tests
def test_legacy_udf_concurrency_mapping():
    @daft.udf(return_dtype=DataType.int64(), concurrency=5)
    def my_udf(x):
        return x

    assert my_udf.min_concurrency is None
    assert my_udf.max_concurrency is None
    assert my_udf.concurrency == 5

def test_legacy_udf_min_max_concurrency():
    @daft.udf(return_dtype=DataType.int64(), min_concurrency=2, max_concurrency=10)
    def my_udf(x):
        return x

    assert my_udf.min_concurrency == 2
    assert my_udf.max_concurrency == 10
    assert my_udf.concurrency is None

def test_legacy_udf_min_only_concurrency():
    @daft.udf(return_dtype=DataType.int64(), min_concurrency=3)
    def my_udf(x):
        return x

    # If only min is specified, max should default to min (based on logic in legacy.py __post_init__)
    assert my_udf.min_concurrency == 3
    assert my_udf.max_concurrency == 3
    assert my_udf.concurrency is None

def test_legacy_udf_concurrency_conflict():
    with pytest.raises(ValueError, match="Cannot specify both `concurrency` and `min_concurrency`/`max_concurrency`"):
        @daft.udf(return_dtype=DataType.int64(), concurrency=5, min_concurrency=2)
        def my_udf(x):
            return x

def test_legacy_udf_invalid_min_max():
    with pytest.raises(ValueError, match="min_concurrency .* cannot be greater than max_concurrency"):
        @daft.udf(return_dtype=DataType.int64(), min_concurrency=10, max_concurrency=5)
        def my_udf(x):
            return x

# Class UDF tests (daft.cls)
def test_cls_concurrency_params():
    @daft.cls(min_concurrency=2, max_concurrency=5)
    class MyCls:
        def __init__(self):
            pass
        def forward(self, x) -> int:
            return x
            
    obj = MyCls()
    # Check the bound method, which should be a Func object with the params
    func = obj.forward
    assert func.min_concurrency == 2
    assert func.max_concurrency == 5

def test_cls_min_only_concurrency():
    @daft.cls(min_concurrency=3)
    class MyCls:
        def __init__(self):
            pass
        def forward(self, x) -> int:
            return x
            
    obj = MyCls()
    func = obj.forward
    assert func.min_concurrency == 3
    # In daft.cls (v2), max_concurrency is automatically set to min_concurrency if unset
    assert func.max_concurrency == 3

def test_cls_max_only_concurrency():
    @daft.cls(max_concurrency=4)
    class MyCls:
        def __init__(self):
            pass
        def forward(self, x) -> int:
            return x
            
    obj = MyCls()
    func = obj.forward
    assert func.min_concurrency is None
    assert func.max_concurrency == 4

def test_cls_concurrency_invalid():
    with pytest.raises(ValueError, match="min_concurrency .* cannot be greater than max_concurrency"):
        @daft.cls(min_concurrency=10, max_concurrency=5)
        class MyCls:
            def __init__(self):
                pass
            def __call__(self, x):
                return x
