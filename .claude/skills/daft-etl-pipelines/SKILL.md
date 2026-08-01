---
name: "daft-etl-pipelines"
description: "Write Python ETL pipelines with the Daft API. Invoke when building data pipelines, transforming DataFrames, or looking up any Daft function, expression, or I/O signature."
---

# Daft ETL Pipelines

Write correct Daft pipelines without guessing at the API. The `references/`
directory contains every public Daft symbol; this file teaches the transform
stage and routes you to that reference.

## 1. Lookup protocol — never guess a symbol

Before using any Daft function, expression method, or reader/writer you are not
certain of, grep the index:

```bash
grep -i '^<name>' .claude/skills/daft-etl-pipelines/references/INDEX.md
grep -i 'regex\|replace' .claude/skills/daft-etl-pipelines/references/INDEX.md
```

Each index row is `name | namespace | signature | summary | file#anchor`. Open
the pointed-to reference file only when you need the full docstring.

**If a symbol is not in `INDEX.md`, it does not exist in this version of Daft.
Do not invent it.**

## 2. Two call styles

Most operations exist both as an `Expression` method and as a free function in
`daft.functions`:

```python
from daft import col
from daft.functions import lower

col("name").lower()      # Expression method
lower(col("name"))       # daft.functions free function — identical result
```

Column 2 of each `INDEX.md` row tells you which namespace a symbol lives in
(`Expression`, `daft.functions`, `daft`, `daft.io`, `DataFrame`). Prefer the
Expression method inside a chain; reach for the free function when composing
without a base column.

## 3. Daft has NO accessor namespaces

`Expression` exposes 247 **flat** methods. There is no `.str`, `.dt`, or `.list`
accessor. Habits from Polars/PySpark raise `AttributeError`:

| Habit from | Wrong | Right |
|---|---|---|
| Polars/PySpark | `col("s").str.contains("x")` | `col("s").contains("x")` |
| Polars | `col("t").dt.year()` | `col("t").year()` |
| Polars | `col("l").list.sort()` | `col("l").list_sort()` |
| Polars | `col("l").list.len()` | `col("l").list_count()` |

List operations are prefixed `list_*`; datetime and string operations are plain
method names. When unsure, grep the index.

## 4. Laziness and the execution boundary

A `DataFrame` is lazy. Nothing runs until a terminal method:

`collect`, `show`, `to_pandas`, `to_arrow`, `to_pydict`, `to_pylist`,
`iter_rows`, `iter_partitions`, `to_arrow_iter`, `count_rows`, every `write_*`,
and the `to_torch_*` / `to_ray_dataset` / `to_dask_dataframe` bridges.

- Use `df.explain()` to inspect the plan without running it.
- Do NOT call `collect()` mid-pipeline just to check shape — chain the whole
  pipeline, collect once at the end.

## 5. Transform stage patterns

```python
import daft
from daft import col
from daft.functions import sum as sum_, mean

df = daft.read_parquet("s3://bucket/events/**/*.parquet")

# Project / add columns
df = df.select("user_id", "ts", "amount")
df = df.with_column("amount_usd", col("amount") * 1.1)
df = df.with_columns({"day": col("ts").date(), "hour": col("ts").hour()})

# Filter (where is an alias for filter)
df = df.where(col("amount") > 0)

# Join
orders.join(users, on="user_id", how="inner")
orders.join(users, left_on="uid", right_on="user_id", how="left")

# Group + aggregate
df.groupby("user_id").agg(
    sum_(col("amount")).alias("total"),
    mean(col("amount")).alias("avg"),
)

# Window
from daft import Window
w = Window().partition_by("user_id").order_by("ts")
df.with_column("running_total", sum_(col("amount")).over(w))

# Reshape
df.explode("items")                 # one row per list element
df.unpivot(["id"], ["q1", "q2"], variable_name="quarter", value_name="rev")
df.pivot("id", "quarter", "rev", agg_expr="sum")
```

**Alias map — these are the same operation, not different choices:**

| Canonical | Aliases |
|---|---|
| `where` | `filter` |
| `distinct` | `unique`, `drop_duplicates` |
| `unpivot` | `melt` |
| `mean` | `avg` |

## 6. Beyond the transform stage — use the sibling skills

- **UDFs** (`@daft.func`, `@daft.cls`, `@daft.func.batch`, `return_dtype`,
  GPU/async/batch tuning) → `daft-udf-tuning`.
- **Runner choice, partitioning, memory, distributed execution** →
  `daft-distributed-scaling`.
- **Concepts, guides, searching the docs** → `daft-docs-navigation`.
- **Reader/writer signatures and credential configs** →
  `references/toplevel.md` (readers/writers) and `references/io.md`
  (`S3Config`, `GCSConfig`, `AzureConfig`, …).

## Maintenance

The `references/*.md` files are generated. After changing Daft's API (e.g. a
rebase on upstream), regenerate and check:

```bash
python3 .claude/skills/daft-etl-pipelines/scripts/gen_reference.py
python3 .claude/skills/daft-etl-pipelines/scripts/gen_reference.py --check   # exit 0 = in sync
python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/
```

`.gitignore` excludes `.claude/**/*.md`, so committing `SKILL.md` or any
regenerated reference requires `git add -f`.
