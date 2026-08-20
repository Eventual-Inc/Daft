# Aggregate UDFs with `@daft.udaf`

When Daft's built-in aggregation functions (sum, mean, count, etc.) aren't sufficient, `@daft.udaf` lets you define custom aggregations in Python. UDAFs work with `groupby().agg()` and global `agg()`, and support Daft's three-stage aggregation pipeline for efficient distributed execution.

## Quick Example

```python
import daft
from daft import DataType, Series

@daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
class MySum:
    def aggregate(self, values: Series) -> float:
        return sum(values.to_pylist())

    def combine(self, states: Series) -> float:
        return sum(states.to_pylist())

    def finalize(self, state: float) -> float:
        return state

my_sum = MySum()
df = daft.from_pydict({"cat": ["a", "a", "b", "b"], "val": [1.0, 2.0, 3.0, 4.0]})
df.groupby("cat").agg(my_sum(daft.col("val")).alias("total")).show()
```

```
╭──────┬─────────╮
│ cat  ┆ total   │
│ ---  ┆ ---     │
│ Utf8 ┆ Float64 │
╞══════╪═════════╡
│ a    ┆ 3.0     │
├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ b    ┆ 7.0     │
╰──────┴─────────╯
```

## How It Works

A UDAF class defines a three-stage aggregation pipeline:

```text
Aggregation:   aggregate(inputs)  -> partial state
Combination:   combine(states)    -> merged state   (associative & commutative)
Finalization:  finalize(state)    -> final output
```

1. **`aggregate(*inputs: Series) -> value | dict`** — Aggregation stage. Receives input columns as `Series` objects, returns a partial state value.
2. **`combine(states: Series | dict[str, Series]) -> value | dict`** — Combination stage. Merges multiple partial states into one. Must be commutative and associative.
3. **`finalize(state: value | dict) -> value`** — Finalization stage. Converts the final merged state into the output value.

Intermediate state is typed: the `state` parameter declares one data type per state component. The framework carries state between stages using these types, which lets Arrow and the query planner reason about intermediate results. Daft's planner automatically decomposes UDAFs into aggregation and finalization stages so partial aggregation happens close to the data.

## Single-State UDAF

For simple accumulators, pass a single `DataType` as `state`:

```python
@daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
class MySum:
    def aggregate(self, values: Series) -> float:
        return sum(values.to_pylist())

    def combine(self, states: Series) -> float:
        return sum(states.to_pylist())

    def finalize(self, state: float) -> float:
        return state
```

- `combine` receives a `Series` of partial state values
- `finalize` receives a single state value

## Multi-State UDAF

For aggregations that need to track multiple fields (e.g., both a sum and a count for computing a mean), pass a dict of `{name: DataType}` as `state`:

```python
@daft.udaf(
    return_dtype=DataType.float64(),
    state={"sum": DataType.float64(), "count": DataType.int64()},
)
class MyMean:
    def aggregate(self, values: Series) -> dict:
        vals = values.to_pylist()
        return {"sum": float(sum(vals)), "count": len(vals)}

    def combine(self, states: dict[str, Series]) -> dict:
        return {
            "sum": float(sum(states["sum"].to_pylist())),
            "count": int(sum(states["count"].to_pylist())),
        }

    def finalize(self, state: dict) -> float:
        return state["sum"] / state["count"]
```

- `aggregate` returns a dict with one key per state field
- `combine` receives a dict mapping field names to `Series` of partial values
- `finalize` receives a dict mapping field names to single values

## Parameterized UDAF

UDAFs can accept constructor arguments via `__init__`:

```python
@daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
class BoundedSum:
    def __init__(self, max_val: float):
        self.max_val = max_val

    def aggregate(self, values: Series) -> float:
        return float(sum(min(v, self.max_val) for v in values.to_pylist()))

    def combine(self, states: Series) -> float:
        return sum(states.to_pylist())

    def finalize(self, state: float) -> float:
        return state

bounded_sum = BoundedSum(max_val=10.0)
df.groupby("cat").agg(bounded_sum(daft.col("val")).alias("total"))
```

## Multi-Input UDAF

UDAFs can consume multiple input columns:

```python
@daft.udaf(return_dtype=DataType.float64(), state=DataType.float64())
class WeightedSum:
    def aggregate(self, values: Series, weights: Series) -> float:
        v = values.to_pylist()
        w = weights.to_pylist()
        return float(sum(a * b for a, b in zip(v, w)))

    def combine(self, states: Series) -> float:
        return sum(states.to_pylist())

    def finalize(self, state: float) -> float:
        return state

ws = WeightedSum()
df.groupby("cat").agg(ws(daft.col("val"), daft.col("weight")).alias("weighted_total"))
```

## Global Aggregation

UDAFs work without `groupby` for whole-table aggregation:

```python
my_sum = MySum()
df = daft.from_pydict({"val": [1.0, 2.0, 3.0, 4.0]})
df.agg(my_sum(daft.col("val")).alias("total")).show()
```

```
╭─────────╮
│ total   │
│ ---     │
│ Float64 │
╞═════════╡
│ 10.0    │
╰─────────╯
```

## Multiple UDAFs in One Aggregation

You can use multiple UDAFs (or the same UDAF on different columns) in a single `.agg()` call:

```python
my_sum = MySum()
my_mean = MyMean()

df.groupby("cat").agg(
    my_sum(daft.col("val")).alias("total"),
    my_mean(daft.col("val")).alias("avg"),
).show()
```
