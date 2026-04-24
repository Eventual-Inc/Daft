from __future__ import annotations

import pytest

import daft
from daft import DataType, Series, col


@daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
class SumUDAF:
    def agg(self, values: Series) -> float:
        return sum(values.to_pylist())

    def combine(self, states: Series) -> float:
        return sum(states.to_pylist())

    def finalize(self, state: float) -> float:
        return state


@daft.udaf(
    return_dtype=DataType.float64(),
    state={"sum": DataType.float64(), "count": DataType.int64()},
)
class MeanUDAF:
    def agg(self, values: Series) -> dict:
        vals = values.to_pylist()
        return {"sum": float(sum(vals)), "count": len(vals)}

    def combine(self, states: dict[str, Series]) -> dict:
        sums = states["sum"].to_pylist()
        counts = states["count"].to_pylist()
        return {"sum": float(sum(sums)), "count": int(sum(counts))}

    def finalize(self, state: dict) -> float:
        return state["sum"] / state["count"]


@daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
class BoundedSumUDAF:
    def __init__(self, max_val: float):
        self.max_val = max_val

    def agg(self, values: Series) -> float:
        return float(sum(min(v, self.max_val) for v in values.to_pylist()))

    def combine(self, states: Series) -> float:
        return sum(states.to_pylist())

    def finalize(self, state: float) -> float:
        return state


class TestUDAFSingleState:
    def test_groupby_sum(self):
        df = daft.from_pydict({"cat": ["a", "a", "b", "b"], "val": [1.0, 2.0, 3.0, 4.0]})
        my_sum = SumUDAF()
        result = df.groupby("cat").agg(my_sum(col("val")).alias("total")).sort("cat").collect()
        assert result.to_pydict() == {"cat": ["a", "b"], "total": [3.0, 7.0]}

    def test_global_agg(self):
        df = daft.from_pydict({"val": [1.0, 2.0, 3.0, 4.0]})
        my_sum = SumUDAF()
        result = df.agg(my_sum(col("val")).alias("total")).collect()
        assert result.to_pydict() == {"total": [10.0]}

    def test_partitioned(self):
        df = daft.from_pydict({"cat": ["a", "a", "b", "b"], "val": [1.0, 2.0, 3.0, 4.0]})
        df = df.into_partitions(4)
        my_sum = SumUDAF()
        result = df.groupby("cat").agg(my_sum(col("val")).alias("total")).sort("cat").collect()
        assert result.to_pydict() == {"cat": ["a", "b"], "total": [3.0, 7.0]}


class TestUDAFMultiState:
    def test_groupby_mean(self):
        df = daft.from_pydict({"cat": ["a", "a", "b", "b"], "val": [2.0, 4.0, 6.0, 8.0]})
        my_mean = MeanUDAF()
        result = df.groupby("cat").agg(my_mean(col("val")).alias("avg")).sort("cat").collect()
        assert result.to_pydict() == {"cat": ["a", "b"], "avg": [3.0, 7.0]}

    def test_global_agg(self):
        df = daft.from_pydict({"val": [2.0, 4.0, 6.0, 8.0]})
        my_mean = MeanUDAF()
        result = df.agg(my_mean(col("val")).alias("avg")).collect()
        assert result.to_pydict() == {"avg": [5.0]}

    def test_partitioned(self):
        df = daft.from_pydict({"cat": ["a", "a", "b", "b"], "val": [2.0, 4.0, 6.0, 8.0]})
        df = df.into_partitions(4)
        my_mean = MeanUDAF()
        result = df.groupby("cat").agg(my_mean(col("val")).alias("avg")).sort("cat").collect()
        assert result.to_pydict() == {"cat": ["a", "b"], "avg": [3.0, 7.0]}


class TestUDAFParameterized:
    def test_bounded_sum(self):
        df = daft.from_pydict({"cat": ["a", "a", "b", "b"], "val": [1.0, 100.0, 3.0, 4.0]})
        bounded_sum = BoundedSumUDAF(max_val=10.0)
        result = df.groupby("cat").agg(bounded_sum(col("val")).alias("total")).sort("cat").collect()
        assert result.to_pydict() == {"cat": ["a", "b"], "total": [11.0, 7.0]}


class TestUDAFMultiple:
    def test_multiple_udafs_in_same_agg(self):
        df = daft.from_pydict({"cat": ["a", "a", "b", "b"], "val": [2.0, 4.0, 6.0, 8.0]})
        my_sum = SumUDAF()
        my_mean = MeanUDAF()
        result = (
            df.groupby("cat")
            .agg(my_sum(col("val")).alias("total"), my_mean(col("val")).alias("avg"))
            .sort("cat")
            .collect()
        )
        assert result.to_pydict() == {"cat": ["a", "b"], "total": [6.0, 14.0], "avg": [3.0, 7.0]}

    def test_same_udaf_different_columns(self):
        df = daft.from_pydict({"cat": ["a", "a", "b", "b"], "x": [1.0, 2.0, 3.0, 4.0], "y": [10.0, 20.0, 30.0, 40.0]})
        sum1 = SumUDAF()
        sum2 = SumUDAF()
        result = (
            df.groupby("cat")
            .agg(sum1(col("x")).alias("sum_x"), sum2(col("y")).alias("sum_y"))
            .sort("cat")
            .collect()
        )
        assert result.to_pydict() == {"cat": ["a", "b"], "sum_x": [3.0, 7.0], "sum_y": [30.0, 70.0]}


@daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
class WeightedSumUDAF:
    def agg(self, values: Series, weights: Series) -> float:
        v = values.to_pylist()
        w = weights.to_pylist()
        return float(sum(a * b for a, b in zip(v, w)))

    def combine(self, states: Series) -> float:
        return sum(states.to_pylist())

    def finalize(self, state: float) -> float:
        return state


@daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
class NullSafeSumUDAF:
    def agg(self, values: Series) -> float:
        return float(sum(v for v in values.to_pylist() if v is not None))

    def combine(self, states: Series) -> float:
        return sum(states.to_pylist())

    def finalize(self, state: float) -> float:
        return state


class TestUDAFNulls:
    def test_global_agg_with_nulls(self):
        df = daft.from_pydict({"val": [1.0, None, 3.0]})
        my_sum = NullSafeSumUDAF()
        result = df.agg(my_sum(col("val")).alias("total")).collect()
        assert result.to_pydict() == {"total": [4.0]}

    def test_groupby_with_null_values(self):
        df = daft.from_pydict({"cat": ["a", "a", "b", "b"], "val": [1.0, None, None, 4.0]})
        my_sum = NullSafeSumUDAF()
        result = df.groupby("cat").agg(my_sum(col("val")).alias("total")).sort("cat").collect()
        assert result.to_pydict() == {"cat": ["a", "b"], "total": [1.0, 4.0]}


class TestUDAFEdgeCases:
    def test_single_row(self):
        df = daft.from_pydict({"val": [5.0]})
        my_sum = SumUDAF()
        result = df.agg(my_sum(col("val")).alias("total")).collect()
        assert result.to_pydict() == {"total": [5.0]}

    def test_many_partitions(self):
        df = daft.from_pydict({"cat": ["a", "a", "b", "b", "a", "b"], "val": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]})
        df = df.into_partitions(6)
        my_sum = SumUDAF()
        result = df.groupby("cat").agg(my_sum(col("val")).alias("total")).sort("cat").collect()
        assert result.to_pydict() == {"cat": ["a", "b"], "total": [8.0, 13.0]}


class TestUDAFMultiInput:
    def test_weighted_sum_global(self):
        df = daft.from_pydict({"val": [1.0, 2.0, 3.0], "weight": [0.5, 1.0, 2.0]})
        ws = WeightedSumUDAF()
        result = df.agg(ws(col("val"), col("weight")).alias("total")).collect()
        assert result.to_pydict() == {"total": [8.5]}

    def test_weighted_sum_groupby(self):
        df = daft.from_pydict({"cat": ["a", "a", "b", "b"], "val": [1.0, 2.0, 3.0, 4.0], "weight": [1.0, 2.0, 1.0, 0.5]})
        ws = WeightedSumUDAF()
        result = df.groupby("cat").agg(ws(col("val"), col("weight")).alias("total")).sort("cat").collect()
        assert result.to_pydict() == {"cat": ["a", "b"], "total": [5.0, 5.0]}


class TestUDAFErrors:
    def test_missing_combine(self):
        with pytest.raises(ValueError, match="must define a `combine` method"):

            @daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
            class BadUDAF:
                def agg(self, values: Series) -> float:
                    return 0.0

                def finalize(self, state: float) -> float:
                    return state

    def test_missing_agg(self):
        with pytest.raises(ValueError, match="must define a `agg` method"):

            @daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
            class BadUDAF:
                def combine(self, states: Series) -> float:
                    return 0.0

                def finalize(self, state: float) -> float:
                    return state

    def test_missing_finalize(self):
        with pytest.raises(ValueError, match="must define a `finalize` method"):

            @daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
            class BadUDAF:
                def agg(self, values: Series) -> float:
                    return 0.0

                def combine(self, states: Series) -> float:
                    return 0.0

    def test_no_expression_args(self):
        my_sum = SumUDAF()
        with pytest.raises(ValueError, match="at least one Expression argument"):
            my_sum(42)
