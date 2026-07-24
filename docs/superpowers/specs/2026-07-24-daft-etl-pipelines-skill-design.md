# Design: `daft-etl-pipelines` Skill

**Date:** 2026-07-24
**Status:** Approved
**Branch:** `feat/daft-etl-pipelines-skill`

## Purpose

A Claude Code skill that lets an AI agent write correct Daft ETL pipelines in Python
without guessing at the API. It combines an exhaustive, generated reference covering
every public Daft symbol with hand-written guidance on the transform stage, where
judgment rather than lookup is required.

The consumer is an AI agent writing automated pipelines, not a human browsing docs.
Every format decision below follows from that: grep-first indexing, exact signatures,
one anchor per symbol, and explicit anti-hallucination rules.

The skill is committed to this repository so anyone who clones the fork and opens it
in Claude Code gets it with no setup step.

## Motivating problem

Daft's API has two properties that reliably break agents carrying habits from other
DataFrame libraries.

**No accessor namespaces.** `Expression` exposes 247 flat public methods and has no
`@property` accessors at all. An agent that writes `col("s").str.contains("x")`,
`col("t").dt.year()`, or `col("l").list.sort()` — all correct Polars — raises
`AttributeError`. Daft wants `col("s").contains("x")`, `col("t").year()`, and
`col("l").list_sort()`.

**Two overlapping call styles.** Most operations exist both as an `Expression` method
and as a free function in `daft.functions`. Nothing in the code tells an agent which
namespace a given symbol lives in.

Both failure modes are lookup failures, which is what an exhaustive index fixes.

## API surface (measured 2026-07-24)

| Source | Symbols |
|---|---|
| `daft/__init__.py` `__all__` | 133 |
| `daft/functions/__init__.py` `__all__` | 356 |
| `daft/io/__init__.py` `__all__` | 38 |
| Union of the three `__all__` lists | 503 |
| `DataFrame` public methods | 98 (97 with docstrings) |
| `Expression` public methods | 247 (242 with docstrings) |
| **Total unique entries** | **848** |

The three lists are not disjoint: `daft/io/__all__` shares 23 names with
`daft/__all__` (every `read_*` entry point is exported from both), and `daft/__all__`
shares one with `daft.functions`. Deduplication policy is in
[Reference file assignment](#reference-file-assignment).

Too large for a single always-loaded `SKILL.md`, hence a router plus on-demand
references.

## Architecture

Namespace-mirrored reference files with a grep-first index. The generator is a dumb
mirror of the source tree, so regeneration never requires human curation.

```
.claude/skills/daft-etl-pipelines/
  SKILL.md                       hand-written, ~250 lines, always loaded
  scripts/gen_reference.py       AST generator
  scripts/test_gen_reference.py  structural tests
  references/                    generated, all committed
    INDEX.md                     848 lines, one per symbol
    toplevel.md                  133  daft.* entry points
    dataframe.md                  98  DataFrame methods
    expressions.md               247  Expression methods
    io.md                         14  configs and source/sink protocols
    functions-str.md              59
    functions-datetime.md         60
    functions-numeric.md          48
    functions-spatial.md          39  spatial + spatial_index
    functions-list.md             20
    functions-agg.md              21
    functions-window.md            8
    functions-misc.md             22
    functions-media.md            34  image, video, audio, url, file, hdf5
    functions-ai.md                5  embed_text, embed_image,
                                      classify_text, classify_image, prompt
    functions-etc.md              40  binary, bitwise, columnar, struct,
                                      distance, similarity, partition,
                                      llm, process
```

The 25 modules and one subpackage under `daft/functions/` collapse into 11 reference
files whose counts sum to exactly 356. Namespaces with 20 or more symbols get their
own file; the long tail groups by theme. Every generated file lands between roughly
2KB and 66KB, so an agent that opens one never loads the whole corpus.

### Reference file assignment

Each symbol appears in exactly one reference file, and `INDEX.md` points to that one
location. Two rules resolve the overlaps:

- The 23 names exported from both `daft` and `daft.io` — the `read_*` entry points —
  live in `toplevel.md`, because that is how pipeline code calls them
  (`daft.read_parquet`, not `daft.io.read_parquet`). `io.md` therefore holds only the
  14 io-exclusive public symbols: the credential configs (`S3Config`, `S3Credentials`,
  `GCSConfig`, `AzureConfig`, `HTTPConfig`, `CosConfig`, `TosConfig`, `GooseFSConfig`,
  `GravitinoConfig`, `HuggingFaceConfig`, `UnityConfig`) and the extension protocols
  (`DataSource`, `DataSourceTask`, `DataSink`). The underscore-prefixed `_range` in
  `daft/io/__all__` is excluded as private.
- The one name shared between `daft` and `daft.functions` lives in the
  `functions-*.md` file for its namespace.

`DataFrame` and `Expression` appear in `toplevel.md` as class entries; their methods
are separate entries in `dataframe.md` and `expressions.md`.

### `INDEX.md` format

The entry point, and usually the only file an agent needs. Pipe-delimited, one line
per symbol, signature inline:

```
regexp_replace | daft.functions | (expr, pattern, replacement) -> Expression | replace all regex matches | functions-str.md#regexp_replace
list_sort      | Expression     | (desc=False, nulls_first=None) -> Expression | sort list elements      | expressions.md#list_sort
read_deltalake | daft           | (table, version=None, io_config=None, ...) -> DataFrame | read a Delta table | toplevel.md#read_deltalake
```

Column 2 resolves the two-call-styles problem: it names which namespace owns the
symbol. An agent opens a reference file only when it needs the docstring body.

## The generator

`scripts/gen_reference.py`. Pure stdlib `ast`. Never imports `daft`.

Runtime introspection was rejected: `daft` cannot be imported without building the
Rust extension via `make build`, and the broken `.venv` in this checkout has no
`bin/`. Static parsing was validated at 356/356 resolution of
`daft.functions.__all__`, so nothing measurable is lost, and the generator runs on a
fresh clone in under a second. That validation required recursing into
`daft/functions/ai/`; the generator walks the package recursively, not just its
top-level modules.

### Resolution pass

Public API is defined by the three `__all__` lists plus the public method bodies of
the `DataFrame` and `Expression` class nodes. Because `__all__` entries are
re-exports, the generator first walks every `from X import Y` in the package to build
a `name -> defining module` map, then locates the `def` or `class` node in that
module.

### Extraction

For each symbol:

- Signature: `ast.unparse` over the argument list and return annotation.
- `INDEX.md` description: first sentence of the docstring.
- Reference body: docstring summary plus `Args:`, `Returns:`, `Raises:`, and `Note:`
  sections.

### Docstring policy

`Examples:` sections are dropped. This halves the corpus and keeps `dataframe.md` at
roughly 66KB instead of 119KB:

| Content | DataFrame | Expression | daft.functions | Total |
|---|---|---|---|---|
| Full docstrings | 119KB | 40KB | 231KB | ~391KB |
| Examples dropped | 66KB | 39KB | 104KB | ~209KB |

Daft's examples are doctest blocks with printed table output — bulk that an agent
reading a typed signature rarely needs. Worked examples live in `SKILL.md`, where
they are chosen for ETL relevance rather than inherited wholesale.

### Determinism

Symbols sorted alphabetically within each file. No timestamps and no version strings
in file bodies. Regenerating after an unrelated change produces byte-identical
output, so `git diff` shows only genuine API movement.

### Drift check

`python3 scripts/gen_reference.py --check` regenerates to a temp directory and diffs
against the committed files, exiting non-zero on mismatch and listing added, removed,
and changed symbols. Run after any rebase on upstream Daft. Not wired into CI, since
this is a fork-local asset; it is documented as a manual step in `SKILL.md`.

### Failure modes

All non-fatal, all visible:

- A name in `__all__` that resolves to nothing is emitted into an `## Unresolved`
  section of `INDEX.md` rather than silently dropped. Currently zero; a re-export
  moving into Rust would introduce some.
- A file that fails to parse produces a stderr warning, and the skip is recorded in
  the run summary.
- A symbol with no docstring produces a signature-only entry tagged `_(no docstring)_`,
  so gaps are visible rather than invisible.

## `SKILL.md`

Frontmatter matching the three existing Daft skills:

```yaml
---
name: "daft-etl-pipelines"
description: "Write Python ETL pipelines with the Daft API. Invoke when
  building data pipelines, transforming DataFrames, or looking up any
  Daft function, expression, or I/O signature."
---
```

Six sections, roughly 250 lines.

**1. Lookup protocol.** First, because it is the routing rule. Never guess a Daft
symbol; grep `references/INDEX.md` first. If a symbol is not in `INDEX.md` it does not
exist in this version of Daft and must not be invented. This is the anti-hallucination
guard and the reason the exhaustive index earns its size.

**2. The two call styles.** Most operations exist as both an `Expression` method and a
`daft.functions` free function. States which to prefer, and that column 2 of
`INDEX.md` resolves the namespace question.

**3. No accessor namespaces.** The headline gotcha, as a correction table:

| Habit from | Wrong | Right |
|---|---|---|
| Polars/PySpark | `col("s").str.contains("x")` | `col("s").contains("x")` |
| Polars | `col("t").dt.year()` | `col("t").year()` |
| Polars | `col("l").list.sort()` | `col("l").list_sort()` |

**4. Laziness and the execution boundary.** Nothing runs until a terminal method,
enumerated explicitly: `collect`, `show`, `to_pandas`, `to_arrow`, `to_pydict`,
`to_pylist`, `iter_rows`, `iter_partitions`, `to_arrow_iter`, `count_rows`, every
`write_*`, and the `to_torch_*` / `to_ray_dataset` / `to_dask_dataframe` bridges. Use
`.explain()` to inspect a plan without triggering it. Do not call `collect()`
mid-pipeline merely to check shape.

**5. Transform stage patterns.** `select`, `with_column`, `with_columns`, `where`,
`join`, `groupby` + `agg`, `Window`, `explode`, `unpivot`, `pivot` — each with a short
runnable snippet using real signatures. Includes the alias map, since 98 `DataFrame`
methods overstate the real surface: `filter` = `where`, `distinct` = `unique` =
`drop_duplicates`, `unpivot` = `melt`, `mean` = `avg`. An agent should know these are
one thing, not four choices.

**6. Cross-links.** UDF authoring goes to `daft-udf-tuning`; runner and partition
tuning to `daft-distributed-scaling`; conceptual docs to `daft-docs-navigation`; I/O
signatures to `references/io.md` and `references/toplevel.md`. `SKILL.md` writes no
I/O, UDF, or runner-tuning prose of its own.

### Scope boundary

`SKILL.md` hand-writes transform-stage guidance only. Extract and load are covered by
generated reference signatures (`io.md`, `toplevel.md`, and the `write_*` methods in
`dataframe.md`), since those are lookup problems rather than judgment problems.
Production concerns — incremental writes, partitioning, checkpointing, runner
selection — are out of scope for the prose and reached through cross-links.

## Verification

`scripts/test_gen_reference.py`, beside the generator rather than in `tests/`, so
`make test` and any upstream PR stay untouched. Run with
`python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/`.

1. **Coverage.** Every name in each of the three `__all__` lists appears in exactly one
   reference file, and the `DataFrame` and `Expression` method counts match their class
   bodies (98 and 247). Asserted as: 848 total entries, no name emitted twice, and the
   11 `functions-*.md` counts summing to 356. A dropped or duplicated symbol fails
   rather than vanishing.
2. **No unresolved.** The `## Unresolved` section of `INDEX.md` is empty.
3. **Anchor integrity.** Every `file.md#anchor` target in `INDEX.md` resolves to a
   heading that exists in that file. The most important test: a broken pointer sends
   an agent to a file that cannot answer its question.
4. **Determinism.** Generate twice into temp directories; assert byte-identical.
5. **No repr leakage.** No entry contains `<ast.` or `object at 0x`, the classic
   `ast.unparse` failure signature.

### Behavioral spot-check

Structural tests prove the reference is complete, not that the skill changes agent
behavior. A manual checklist of prompts, each naming the wrong output it guards
against:

| Prompt | Must not produce | Should produce |
|---|---|---|
| lowercase a string column | `.str.lower()` | `col("s").lower()` |
| extract year from a timestamp | `.dt.year()` | `col("t").year()` |
| sort a list column | `.list.sort()` | `col("l").list_sort()` |
| count rows | `collect()` then `len` | `count_rows()` |

## Commit mechanics

`.gitignore:76` is `.claude/**/*.md`, which ignores markdown only. So
`gen_reference.py` and `test_gen_reference.py` commit normally, while `SKILL.md` and
the sixteen `references/*.md` files require `git add -f`. The three existing Daft
skills were force-added the same way. This asymmetry is easy to trip over on the next
update, so it is documented in `SKILL.md`'s maintenance note.

## Decisions and alternatives rejected

| Decision | Alternative rejected | Reason |
|---|---|---|
| Namespace-mirrored references | Task-sharded by pipeline stage | Needs a hand-maintained symbol-to-stage map, 848 curation calls that rot on every API change |
| Namespace-mirrored references | Single flat reference | Any lookup pulls ~209KB into agent context |
| Static AST parsing | Runtime introspection | Requires `make build`; AST validated at 100% resolution |
| Committed generator | One-time static generation | Reference rots silently with no drift check |
| Committed in this repo | Standalone plugin repo | User wants collaborators on this fork to get it on clone |
| `Examples:` dropped | Full docstrings | Halves corpus; `dataframe.md` 66KB instead of 119KB |
| Transform prose only | Also I/O, UDF, production prose | Duplicates existing skills; I/O is lookup, not judgment |

## Next step

Implementation plan via the `writing-plans` skill.
