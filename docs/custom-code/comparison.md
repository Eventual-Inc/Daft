# Migrating to daft.func & daft.cls from daft.udf

**What’s changing?**

Daft’s new UDF system centers on two decorators:

* **`@daft.func`** — stateless row‑wise UDFs (with async + generator variants and a batch variant).
* **`@daft.cls`** (+ optional **`@daft.method`**) — stateful UDFs where you initialize once (e.g., load a model) and reuse across rows.
* New UDFs support eager execution with scalars, type‑hint‑driven return type inference, generator UDFs, and batch UDFs.

**Should I switch now?**

* For most production workloads, **yes**.
* You get cleaner ergonomics, async/generators, and better typing.

**Two caveats:**

* **CPU/memory resource knobs** from legacy are **not** yet exposed in the next‑gen API (GPUs, `max_concurrency`, and `use_process` are). If you rely on explicit CPU memory limits, you may want to keep those calls on legacy a bit longer or re-tune with `batch_size` and scheduling.
* A few behaviors are still evolving (type enforcement on inputs, async‑batch).

The newer `@daft.func` and `@daft.cls` decorators provide a cleaner interface for most use cases. The legacy `@daft.udf` decorator still has a few advanced features:

| Feature | @daft.func / @daft.cls | @daft.udf |
|---------|------------------------|-----------|
| Function UDFs | ✅ Yes (@daft.func) | ✅ Yes |
| Class UDFs | ✅ Yes (@daft.cls) | ✅ Yes |
| Type inference from hints | ✅ Yes | ❌ No |
| Eager evaluation mode | ✅ Yes | ❌ No |
| Async functions | ✅ Yes | ❌ No |
| Generator functions | ✅ Yes | ❌ No |
| Concurrency control | ✅ Yes (@daft.cls) | ✅ Yes (class UDFs) |
| Resource requests (GPUs) | ✅ Yes (@daft.cls) | ✅ Yes |
| Batch processing | ✅ Yes (@daft.func.batch) | ✅ Yes |

---

## At‑a‑glance: core differences

**What you gain with next‑gen UDFs**

* **Type inference from Python hints**:
    * no more always‑required `return_dtype`
    * **eager mode** with scalar args
    * Both `@daft.func` and `@daft.method` come with **async**, **generator**, and **batch** variants
* **Stateful class UDFs** via `@daft.cls`
    * lazy per‑worker init for models/clients
    * with **GPU requests**, **`max_concurrency`**, and **`use_process`** for isolation.
* **Methods work by default**: in a `@daft.cls`, **any method (including `__call__`) can be used as a Daft expression**; add `@daft.method` only when you need to override return types or unnest.

**What legacy `@daft.udf` had that is different**

* A single decorator that handled both functions and classes, and exposed **resource knobs**: `num_cpus`, `num_gpus`, **`memory_bytes`**, `batch_size`, `concurrency`, `use_process`.
* Class UDF controls like `with_init_args`, `with_concurrency`, `override_options`.

---

## Decision guide

**Use `@daft.func`** when:

* You don’t need persistent state between rows.
* You want a simple row‑wise function (including **async** or **generator**) or a **batch** function (`@daft.func.batch`).

**Use `@daft.cls`** when:

* You need **expensive init** amortized across rows (models, clients, caches).
* You need **GPU scheduling**, **process isolation**, or to bound the number of **concurrent instances** across the cluster (`max_concurrency`).
* You still get row‑wise, async, generator, and **batch methods** via `@daft.method` / `@daft.method.batch`.

---

## Migration FAQ (from Slack + docs)

### Q1) Do we recommend switching to `@daft.cls`/`@daft.method` / `@daft.func` now?

**Yes** for most cases.

New APIs are cleaner, support async/generators, and give you batch UDFs and eager local execution for debugging. If you rely on **CPU/memory** resource overrides from legacy (`num_cpus`, `memory_bytes`), note those are **not exposed yet** on next‑gen (design is being revisited). GPUs, `max_concurrency`, and `use_process` are supported on `@daft.cls`.

### Q2) Does `@daft.cls` support **batch**?

**Yes.**

Use `@daft.method.batch(return_dtype=..., batch_size=...)` for class methods. For stateless, use `@daft.func.batch`. You can also mix **Series + scalar** args; `batch_size` is tunable.

### Q3) What’s the **migration cost** from legacy?

Mostly **rename + signature**:

Stateless:

* **Before (legacy)**: `@daft.udf(return_dtype=...) def f(x): ...`
* **After (next‑gen)**: `@daft.func  def f(x: int) -> str: ...` *(return type can be inferred; keep `return_dtype` if complex)*.

Stateful:

* **Before**: `@daft.udf(...) class MyUdf: def __init__(...); def __call__(...)`
* **After**: `@daft.cls class MyUdf: def __init__(...); def __call__(...)` *(no `@daft.method` needed for `__call__`; optionally decorate other methods to override return types or unnest)*.

Resource knobs:

* **Legacy** `num_cpus`, `memory_bytes`, `concurrency` → **Next‑gen** `gpus`, `max_concurrency`, `use_process`; no CPU/memory knob yet.
* `with_init_args(...)` → just **instantiate your class** with those args.

### Q4) How do I **set concurrency and batch size** now?

* **Class‑level concurrency:** set `max_concurrency` on `@daft.cls`.
* **Batch size:** set `batch_size` on `@daft.func.batch` or `@daft.method.batch`. Docs include tuning tips (avoid OOM, match model batch).

### Q5) What about **async**?

* **Row‑wise** `@daft.func` and `@daft.cls` methods support async now.
* **Async batch** is also supported as of [0.6.8](https://github.com/Eventual-Inc/Daft/releases/tag/v0.6.8)

### Q6) Do I still need `@daft.method` on class methods?

**No.**

In a `@daft.cls`, **all methods (including `__call__`) are usable as UDFs**. Add `@daft.method(...)` when you need to **set `return_dtype`** or **`unnest=True`**. [See Using `@daft.method`](cls.md/#using-daftmethod)

### Q7) How do I return **multiple columns**?

Return a **struct** (dict) and either select it as a single struct column or **set `unnest=True`** to expand fields to columns. Works with both `@daft.func` and `@daft.method`.

### Q8) Can I feed a **whole row** (many columns) to a UDF?

You can feed as many column inputs as you need, but they will each be passed as individual arguments.
There isn’t a `mapPartitions`-style API yet. Today you can pass the whole row as a **struct** using `daft.struct(*df.column_names)` and then `.unnest()` the result. This pattern was recommended in the [Mapping UDFs to Partitions](https://github.com/Eventual-Inc/Daft/discussions/5150) discussion.

### Q9) Do I still need to supply `return_dtype`?

Often **no**—**next‑gen infers** it from Python type hints (and supports struct + unnest). For complex schemas or when you want explicit control, still pass `return_dtype`.

### Q10) Any **observability** or runner caveats?

* Some users report **log suppression** at `concurrency > 1`.
* Setting concurrency on **stateful UDFs on Ray** had failures in certain versions. Track these issues if you hit them.

---

## Migration cookbook (side‑by‑side)

### Stateless function → `@daft.func`

**Legacy**

```python
import daft

@daft.udf(return_dtype=daft.DataType.int64())
def inc(x):
    return x + 1

df = df.select(inc(df["x"]))
```

**Next‑gen**

```python
import daft
from typing import Iterable

@daft.func()
def inc(x: int) -> int:
    return x + 1

# Also supports generators and async
@daft.func()
def explode_chars(s: str) -> Iterable[str]:
    for ch in s:
        yield ch

df = df.select(inc(df["x"]), explode_chars(df["name"]))
```

New features used here: **type inference**, **generator** UDFs.

---

### Stateful class → `@daft.cls`

**Legacy**

```python
@daft.udf(return_dtype=daft.DataType.string(), concurrency=2)
class Model:
    def __init__(self, path):
        self.m = load_model(path)
    def __call__(self, text):
        return self.m(text)
```

**Next‑gen**

```python
import daft
from daft import DataType

@daft.cls(gpus=1, max_concurrency=2, use_process=True)
class Model:
    def __init__(self, path: str):
        self.m = load_model(path)

    # usable without decoration; add @daft.method() to override return dtype or unnest
    def __call__(self, text: str) -> str:
        return self.m(text)

model = Model("resnet50.pt")
df = df.select(model(df["text"]))
```

Controls now live on `@daft.cls` (`gpus`, `max_concurrency`, `use_process`).

---

### Batch processing

**Stateless batch**

```python
from daft import DataType, Series

@daft.func.batch(return_dtype=DataType.int64(), batch_size=1024)
def add(a: Series, b: Series) -> Series:
    import pyarrow.compute as pc
    return pc.add(a.to_arrow(), b.to_arrow())
```

**Class method batch**

```python
@daft.cls
class Embedder:
    def __init__(self, model_name: str):
        self.model = load(model_name)

    @daft.method.batch(return_dtype=DataType.list(DataType.float32()), batch_size=256)
    def embed(self, text: Series) -> Series:
        return self.model.embed(text.to_arrow().to_pylist())
```

Both patterns support **Series + scalar** args and include **batch size tuning guidance** in the docs.

---

### Multi‑column outputs (struct + unnest)

```python
from daft import DataType

@daft.func(return_dtype=DataType.struct({"first": DataType.string(), "age": DataType.int64()}), unnest=True)
def parse(line: str) -> dict:
    name, age = line.split(",")
    return {"first": name, "age": int(age)}

df = df.select(parse(df["raw"]))
```

---

### “Whole row in / whole row out” today

```python
# Pass entire row as a single struct argument
row_expr = daft.struct(*df.column_names)

@daft.func(return_dtype=DataType.struct({"out": DataType.int64()}), unnest=True)
def compute(row: dict) -> dict:
    return {"out": row["a"] + row["b"]}

df = df.select(compute(row_expr))
```

Pattern discussed in [Mapping UDFs to Partitions](https://github.com/Eventual-Inc/Daft/discussions/5150) discussion.

---

## Resource model: then vs now

* **Legacy `@daft.udf`** let you specify **`num_cpus`**, **`num_gpus`**, **`memory_bytes`**, `batch_size`, `concurrency`, `use_process` at decoration time (or later via `override_options/with_concurrency`).
* **Next‑gen** exposes **`gpus`**, **`max_concurrency`**, **`use_process`** on `@daft.cls`. **CPU & memory** knobs are purposely not exposed yet while the team revisits design (see [roadmap discussion](https://github.com/Eventual-Inc/Daft/discussions/4820)). Use **batch sizing** and **process isolation** to tune for now.

---

## Known limitations & active items (watchlist)

* **Type checking of input signatures**: next‑gen plans/claims type checking from hints, but there’s an **[open issue](https://github.com/Eventual-Inc/Daft/issues/5462)** where `@daft.func` may not yet enforce the input signature strictly. Keep explicit types + tests for now.
* **Async + process isolation**: a recent bug (“`use_process` no longer works with async functions”) was **fixed**; ensure you’re on a recent version.
* **Async batch**: Available as of release [0.6.8](https://github.com/Eventual-Inc/Daft/pull/5459)

---

## Migration checklist

1. **Inventory**: list your legacy UDFs and label them **stateless** vs **stateful**.
2. **Replace decorators**:

   * Stateless → `@daft.func` (or `@daft.func.batch`).
   * Stateful → `@daft.cls` (+ `@daft.method`/`.batch` as needed).
3. **Return types**: rely on **type hints** where possible; otherwise set `return_dtype`. Use **struct + `unnest=True`** for multi‑column outputs. Track [this issue for using unnest=True inside the UDF args.](https://github.com/Eventual-Inc/Daft/issues/5448)
4. **Resource knobs**: map legacy to new:

   * `concurrency` → `max_concurrency` on `@daft.cls`.
   * `num_gpus` → `gpus` on `@daft.cls`.
   * `num_cpus/memory_bytes` → **not exposed** yet; tune with `batch_size` + isolation and watch the roadmap.
5. **Batch**: convert vectorized UDFs to `@daft.func.batch` or `@daft.method.batch`, start with conservative `batch_size`, increase until stable throughput w/o OOM.
7. **Async**: switch blocking network/model calls to **async** functions/methods when feasible;

---

## Quick reference

**`@daft.cls`**

```python
@daft.cls(gpus=1, max_concurrency=4, use_process=True)   # resource + concurrency controls
class MyUDF:
    def __init__(self, arg: str): ...
    def __call__(self, x: int) -> int: ...               # works as-is
    @daft.method(return_dtype=DataType.struct({...}), unnest=True)
    def classify(self, text: str): ...
    @daft.method.batch(return_dtype=DataType.int64(), batch_size=512)
    def score(self, xs: Series) -> Series: ...
```

(Methods are usable without `@daft.method` unless you need to override return dtype/unnest.)

**`@daft.func`**

```python
@daft.func                      # row-wise, type-hint inferred
def f(x: int) -> str: ...

@daft.func.batch(return_dtype=DataType.int64(), batch_size=1024)
def g(xs: Series) -> Series: ...

@daft.func                      # generator
def tokens(text: str) -> Iterable[str]:
    ...
```

**Legacy knobs you might be replacing**

```python
# Legacy
@daft.udf(return_dtype=..., num_cpus=2, num_gpus=1, memory_bytes=..., concurrency=4, use_process=True)
def f(...): ...
# New equivalents (subset)
@daft.cls(gpus=1, max_concurrency=4, use_process=True)
class C: ...
```

---

## See also

* **[New UDFs guide]()** — feature overview, row‑wise/async/generator/batch, struct + unnest, and the comparison table with legacy.
* **[Legacy UDFs guide]()** — resource request examples and class UDF patterns.
* **[Python API](../api/udf.md)** — authoritative signatures for legacy `udf(...)` (shows `num_cpus/num_gpus/memory_bytes/...`) and for `@daft.cls` / `@daft.method` / `@daft.func`.
* **[New UDFs roadmap](https://github.com/Eventual-Inc/Daft/discussions/4820)** — status updates (async support added in 0.5.18; CPU/memory not yet exposed).
* **[Open issues to watch](https://github.com/Eventual-Inc/Daft/issues?q=is%3Aissue%20state%3Aopen%20%20UDF)** — type‑signature enforcement, Ray concurrency behavior, logging at concurrency > 1.
* **[Recent PRs](https://github.com/Eventual-Inc/Daft/pulls?q=is%3Apr+is%3Aclosed+UDF)** — async batch support work in flight.
