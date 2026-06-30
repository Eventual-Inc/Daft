# Learnings: Daft 0.7.x

Architectural notes and Daft patterns accumulated while building Archetype. Read before proposing changes to processor or UDF code. For normative contracts, see `docs/guide/specification.md`.

---

## Daft DataFrames are lazy, columnar, and vectorized

The DataFrame is already columnar. Operations on columns are inherently batched/vectorized. Don't overthink it.

```python
# Just use expressions when possible
df = df.with_column("score", col("reward") * 0.5 + col("bonus"))
df = df.where(col("score") > 0.5)
df = df.groupby("env_id").agg(col("reward").mean())
```

UDFs are the **escape hatch** for when you can't express logic as DataFrame operations (e.g., calling an LLM, loading a custom model).

---

## Daft 0.7.x UDF Patterns

### 1. `@daft.func` — Stateless, Row-by-Row

For simple element-wise transforms.

```python
@daft.func(return_dtype=DataType.float64())
def double(x: float) -> float:
    return x * 2.0

df = df.with_column("doubled", double(col("value")))
```

### 2. `@daft.func.batch` — Stateless, Series→Series

For batch operations where you need the full Series.

```python
from daft import Series

@daft.func.batch(return_dtype=DataType.string())
def process_batch(values: Series) -> Series:
    results = [transform(v) for v in values.to_pylist()]
    return Series.from_pylist(results)
```

### 3. `@daft.cls()` — Stateful Class

For expensive initialization that should happen **once per worker** (models, connections).

```python
@daft.cls()
class ModelInference:
    def __init__(self):
        # Runs ONCE per worker
        self.model = load_expensive_model()

    # Default: row-by-row (like @daft.func)
    def predict(self, x: str) -> str:
        return self.model(x)

    # Explicit batch (like @daft.func.batch)
    @daft.method.batch
    def predict_batch(self, xs: Series) -> Series:
        return Series.from_pylist(self.model.batch_predict(xs.to_pylist()))

inference = ModelInference()
df = df.with_column("pred", inference.predict(col("input")))
```

**Key insight**: All public methods on `@daft.cls()` are automatically `daft.method` (row-by-row). Use `@daft.method.batch` explicitly when you want Series→Series.

---

## When to Use Batch

**Only use `@daft.func.batch` or `@daft.method.batch` if:**

1. The underlying operation actually supports batching
2. You need access to the full Series (not just individual values)

| Library | Supports Batch? | Pattern |
|---------|-----------------|---------|
| vLLM | ✓ Yes | `@daft.method.batch` — batch all prompts |
| Transformers | ✓ Yes | `@daft.method.batch` — batch inputs |
| OpenAI API | ✗ No (rate limited) | Default row-by-row |
| Ollama | ✗ No | Default row-by-row |
| PyTorch inference | ✓ Yes | `@daft.method.batch` |
| Simple transforms | — | Just use DataFrame expressions |

If the model doesn't batch, using `@daft.method.batch` and looping internally is just extra complexity for no gain.

---

## Expression namespaces were deprecated v0.7.x

**Old way (deprecated):**

```python
col("result").struct.get("field")  # ✗ No longer works
```

**New way:**

```python
col("result")["field"]  # ✓ Use indexing
```

---

## UDF column args resolve to the column's FINAL value (read-then-overwrite footgun)

Daft folds a `@daft.func`'s column arguments to that column's **final definition in
the plan**, not its value at the `with_column` call site. So a processor that reads a
column with a UDF and then overwrites that same column later in the chain feeds the UDF
the **post-overwrite** value — even across separate `with_column`/`with_columns` calls,
and even through an intermediate snapshot column.

```python
# ✗ BROKEN: append reads seq_next, then seq_next is bumped -> append sees the BUMPED value
df = df.with_column("plan", append_plan(col("plan"), col("seq_next"), ...))   # reads seq_next=1, not 0!
df = df.with_column("seq_next", col("seq_next") + 1)
# A plain projection snapshot does NOT save you once a UDF consumes it:
df = df.with_column("snap", col("seq_next"))   # snap also folds to the bumped value
df = df.with_column("out",  some_udf(col("snap")))
```

This silently corrupts any "read the pre-mutation value" logic — e.g. a state-hash taken
*before* applying an effect, or a sequence counter — and the bug is invisible until you
assert on exact values.

**Fix:** consolidate every read-then-overwrite into **one** struct-returning UDF that
reads each input once and returns all results as struct fields, then split the struct
back into columns. Because the output columns derive *from* the UDF, Daft cannot fold the
UDF's own inputs to those outputs (that would be a cycle), so it reads the genuine
pre-mutation values.

```python
# ✓ CORRECT: one UDF reads originals, returns a struct; split it back out
df = df.with_column("eff", apply_effect(col("atoms_json"), col("plan_json"), col("seq_next"), ...))
for f in ("atoms_json", "plan_json", "seq_next", "pre_state_sig"):
    df = df.with_column(f, col("eff")[f])
df = df.exclude("eff")
```

Plain projections (no UDF, e.g. a column swap via `with_columns({"a": col("b"), "b": col("a")})`)
*do* read input values atomically — the folding hazard is specific to UDF arguments.
Discovered building `src/archetype/htn/` (see `EffectProcessor` / `udfs.apply_effect`).

---

## Common Mistakes I Made

1. **Used `@daft.udf`** — Deprecated in 0.7.0, removed in 0.8.0. Use `@daft.func.batch` instead. :white_check_mark: *All UDFs migrated Jan 2026*

2. **Used `.struct.get()`** — Use `[]` indexing instead

3. **Thought `@daft.cls()` required `batch`** — No, default methods are row-by-row

4. **Over-engineered UDFs** — Many transforms are just DataFrame expressions

5. **Used `batch` without actual batching** — If you loop inside a batch UDF, you're not batching

---

## File Handling: `daft.File`

For weights-as-data pattern:

```python
@daft.cls()
class Trainer:
    @daft.method.batch
    def train(self, data: Series, weights_file: Series) -> Series:
        # weights_file contains daft.File objects
        for wf in weights_file.to_pylist():
            with wf.to_tempfile() as tmp:
                state = torch.load(tmp.name)
        # ...
```

---

## The Data-Centric Principle (Mar 2026)

Archetype is **data-centric**. The DataFrame is the source of truth. Processors are pure functions `DataFrame → DataFrame`. So long as the data looks right at the end of a tick, nothing else matters — not how the LLM was called, not whether it was async or sync, not how long it took.

This means:

1. **Never break the lazy DAG unless you must.** `.collect().to_pylist()` pulls data out of Daft's execution engine. You lose lazy evaluation, automatic parallelism, and plan optimization. The default instinct — collect everything, loop in Python, push back — is wrong for this codebase.

2. **Use `@daft.func` (row-wise) by default.** If your "batch" UDF is just a for-loop over `Series.to_pylist()`, it should be `@daft.func`. Daft supports async `@daft.func` natively.

3. **Use `@daft.cls()` for non-serializable state.** API clients, model weights, DB connections — anything that can't be pickled goes in `@daft.cls().__init__()`. Methods are row-wise. Daft recreates the class per worker.

4. **Only `.collect()` for cross-row context.** Message routing (sender → receiver) requires global visibility. Name lookups across entities require global visibility. These are justified collects. Document them inline.

5. **Don't import actor patterns.** No `asyncio.gather` over collected rows. No building dicts from pylist loops and feeding them back through batch UDFs. If you find yourself doing this, you're fighting the execution model.

```python
# :x: WRONG: Imperative actor pattern in a data-centric system
rows = df.select("entity_id", "agent__name", "inbox__messages").collect().to_pylist()
results = await asyncio.gather(*[call_llm(row) for row in rows])
response_by_id = {r["id"]: r["text"] for r in results}

@daft.func.batch(return_dtype=...)
def write_back(entity_ids: Series) -> list:
    return [response_by_id.get(eid, "") for eid in entity_ids.to_pylist()]

# :white_check_mark: RIGHT: Row-wise, Daft manages execution
@daft.func
async def think_and_respond(name: str, role: str, inbox: list[str]) -> list[str]:
    response = await client.messages.create(...)  # Daft handles concurrency
    return [json.dumps({"receiver_id": target, "content": response.content[0].text})]

df = df.with_column("outbox__messages", think_and_respond(col("agent__name"), ...))
```

**The serialization constraint:** `@daft.func` closures must be picklable. API clients, mocks, and anything with network state are NOT picklable. Use `@daft.cls()` for these — the client lives in `__init__`, reconstructed per worker, never serialized.

```python
# :white_check_mark: Production pattern: @daft.cls() for non-serializable clients
@daft.cls()
class ClaudeAgent:
    def __init__(self):
        import anthropic
        self.client = anthropic.AsyncAnthropic()

    async def respond(self, name: str, role: str, inbox: list[str]) -> list[str]:
        response = await self.client.messages.create(model="claude-sonnet-4-6", ...)
        return [json.dumps({...})]

agent = ClaudeAgent()
df = df.with_column("outbox__messages", agent.respond(col("agent__name"), ...))
```

---

## Lazy-Audit UDF-Boundary Exemption (Jun 2026)

``scripts/check_lazy_audit.py`` gates every ``.collect()`` and
``.to_pylist()`` call in ``src/`` against ``lazy_audit.toml``.  There is
**one sanctioned exception** that does not require an allowlist entry:

> ``Series.to_pylist()`` called on a *parameter* of a function decorated
> with ``@daft.method.batch`` or ``@daft.func.batch``.

When Daft invokes such a function the batch is already materialised by the
executor — the function receives concrete ``Series`` objects.  Converting
those parameters to Python lists is the expected interface at the C-library
or RPC boundary, not premature materialisation.  The checker detects this
pattern via AST analysis and reports these sites as
*"udf-boundary (sanctioned)"*.

```python
# :white_check_mark: SANCTIONED — no lazy_audit.toml entry needed
@daft.cls()
class Stepper:
    @daft.method.batch(return_dtype=_STATE_STRUCT)
    def step(self, cart_pos: Series, pole_angle: Series) -> Series:
        cp = cart_pos.to_pylist()   # ← sanctioned: param of @daft.method.batch
        pa = pole_angle.to_pylist() # ← sanctioned: param of @daft.method.batch
        ...

# :x: GATED — requires a lazy_audit.toml entry with a specific technical reason
def query_rows(df):
    return df.to_pylist()  # ← DataFrame-level; still audited
```

Rules:

- ``Series.to_pylist()`` on a **batch-UDF parameter** → exempt, no entry.
- ``DataFrame.to_pylist()`` anywhere → requires entry.
- ``DataFrame.collect()`` anywhere → requires entry.
- ``Series.to_pylist()`` **outside** a batch-UDF → requires entry.
- ``collect()`` inside a batch-UDF on a DataFrame (not a parameter) → requires entry.

See ``lazy_audit.toml`` for the authoritative policy header and
``tests/scripts/test_check_lazy_audit.py`` for positive/negative coverage.

---

## Daft Lazy Evaluation Gotcha (Jan 2026)

Daft is lazily evaluated. Intermediate `.select(...).collect()` calls break the DAG and may cause upstream operations (like `prompt()`) to execute on a **separate plan** that discards downstream work.

```python
# :x: WRONG: This breaks the DAG
df = df.with_column("response", prompt(col("input"), ...))
debug = df.select("response").limit(1).collect()  # Materializes SEPARATE plan!
df = df.with_column("next", col("response") + "...")  # response may be empty!

# :white_check_mark: RIGHT: Keep all columns in plan until final collect
df = df.with_column("response", prompt(col("input"), ...))
df = df.with_column("next", col("response") + "...")
result = df.collect()  # Single materialization
```

**Key insight:** When debugging Daft pipelines, use `df.explain()` to inspect the DAG rather than intermediate collects.

---

## Row-wise `@daft.func` vs `@daft.func.batch` (Jan 2026)

For simple row transforms, prefer `@daft.func` over `@daft.func.batch`:

```python
# :white_check_mark: Clean: Row-wise with automatic type inference
@daft.func
def update_history(history_json: str, agent: str, response: str) -> str:
    history = json.loads(history_json) if history_json else []
    history.append({"agent": agent, "statement": response})
    return json.dumps(history)

# :x: Unnecessary: Batch when you're just looping anyway
@daft.func.batch(return_dtype=DataType.string())
def update_history_batch(history: Series, agent: Series, response: Series) -> Series:
    results = []
    for h, a, r in zip(history.to_pylist(), agent.to_pylist(), response.to_pylist()):
        hist = json.loads(h) if h else []
        hist.append({"agent": a, "statement": r})
        results.append(json.dumps(hist))
    return Series.from_pylist(results)
```

**Rule of thumb:** Use `.batch` only when the underlying operation actually benefits from batching (vectorized NumPy, batch inference, etc.).





---

## JSON-Encoding Complex Types (Jan 2026)

Daft can't store `list[dict]` directly. JSON-encode to `str`:

**Also applies to:** nested dicts, custom objects, anything non-primitive.

---

## Summary

1. **DataFrames are batched by nature** — use expressions first
2. **`@daft.func`** for simple row-wise transforms (auto type inference)
3. **`@daft.func.batch`** only when operation actually benefits from batching
4. **`@daft.cls()`** for stateful (models), methods are row-by-row by default
5. **`@daft.method.batch`** only when the model actually supports batching
6. **`col("x")["field"]`** for struct access
7. **JSON-encode** complex types (`list[dict]`, nested objects) for Arrow compatibility
8. **Resources** for type-safe DI in processors
9. **Hooks** for observability without processor coupling
10. **Messaging pipeline** — Outbox/Inbox components + MessageDeliveryProcessor (not broker)
11. **Tick-gating** for expensive operations (LLM calls, inner worlds)
12. **Keep columns in DAG** — avoid intermediate `.collect()` breaking lazy evaluation



## Daft 0.7.x: `with_column` Not `with_columns` (Apr 2026)

`DataFrame.with_columns(expr1, expr2)` raises `TypeError: too many positional arguments` in Daft 0.7.x. The method accepts **one expression at a time**. Chain calls instead:

```python
# :x: WRONG — multiple positional args
df = df.with_columns(
    (col("position__x") + col("velocity__vx")).alias("position__x"),
    (col("position__y") + col("velocity__vy")).alias("position__y"),
)

# :white_check_mark: RIGHT — chain single expressions
df = df.with_column("position__x", col("position__x") + col("velocity__vx"))
df = df.with_column("position__y", col("position__y") + col("velocity__vy"))
```
