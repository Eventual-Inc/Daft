# daft-etl-pipelines Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a committed Claude Code skill that lets an AI agent write correct Daft ETL pipelines by combining an exhaustively generated API reference (848 public symbols) with hand-written transform-stage guidance.

**Architecture:** A pure-stdlib `ast` generator parses the Daft package source (`daft/**/*.py` plus the Rust extension's type stub `daft/daft/__init__.pyi`) and emits 16 namespace-mirrored markdown reference files plus a grep-first `INDEX.md`. A hand-written `SKILL.md` routes agents to the index and teaches the transform stage. Structural tests prove the reference is complete, deterministic, and internally consistent.

**Tech Stack:** Python 3 standard library only (`ast`, `pathlib`, `argparse`, `dataclasses`, `tempfile`, `difflib`), `pytest` for tests. The generator never imports `daft`, so it runs on a fresh clone without `make build`.

## Global Constraints

- **Skill location:** `.claude/skills/daft-etl-pipelines/` — copied every value verbatim below.
- **Generator imports:** stdlib only. Never `import daft`. Parse source with `ast`.
- **Parse sources:** every `*.py` under `daft/`, plus the stub `daft/daft/__init__.pyi` (holds Rust-native symbols like `S3Config`). Recurse into subpackages including `daft/functions/ai/`.
- **Determinism:** alphabetical sort within every file; no timestamps, no version strings in file bodies; two runs must be byte-identical.
- **Docstring policy:** keep summary + `Args:`/`Returns:`/`Raises:`/`Note:`/`Notes:` sections; drop `Examples:`/`Example:` sections and everything after them.
- **Public API definition:** the three `__all__` lists (`daft/__init__.py`=133; `daft/functions/__init__.py`=356; `daft/io/__init__.py`=38, of which `_range` is private → **37** kept) plus the public methods of the `DataFrame` (98) and `Expression` (247) classes. Each list keeps its own namespace, so a name in both `daft` and `daft.io` is two `Symbol` records: **871 total records** = 133 + 356 + 37 + 98 + 247. The renderer then collapses the 23 names shared between `daft` and `daft.io` into a single `toplevel.md` entry each, yielding **848 index rows**. Underscore-prefixed names are excluded as private.
- **Commit mechanics:** `.gitignore:76` is `.claude/**/*.md` (markdown only). `.py` files commit normally; `SKILL.md` and every `references/*.md` require `git add -f`.
- **Reference:** spec at `docs/superpowers/specs/2026-07-24-daft-etl-pipelines-skill-design.md`.

---

## File Structure

```
.claude/skills/daft-etl-pipelines/
  SKILL.md                        Task 6   hand-written router + transform guide
  scripts/
    gen_reference.py              Tasks 1-4  the AST generator
    test_gen_reference.py         Task 5   structural tests over generated output
  references/                     Task 5   generated, force-added
    INDEX.md                      848 lines, one per symbol
    toplevel.md · dataframe.md · expressions.md · io.md
    functions-str.md · functions-datetime.md · functions-numeric.md
    functions-spatial.md · functions-list.md · functions-agg.md
    functions-window.md · functions-misc.md · functions-media.md
    functions-ai.md · functions-etc.md
```

`gen_reference.py` internal components (built across Tasks 1-4, each independently tested):
- **Resolution** (`resolve_public_api`) — Task 1: turn the API definition into a list of `Symbol` records with located AST nodes.
- **Extraction** (`extract_signature`, `extract_docstring`) — Task 2: node → signature string and trimmed docstring.
- **Assignment + rendering** (`assign_file`, `render_reference`, `render_index`) — Task 3: symbol → target file; records → markdown.
- **CLI** (`main`, `--check`) — Task 4: write files, drift check, determinism.

---

## Validated facts (do not re-derive; these were measured against the checkout)

- `ast.unparse(node.args)` yields clean signatures; method args begin with `self` (strip it). Return annotations are `ast.unparse(node.returns)`, often quoted forward-refs like `'DataFrame'` (strip surrounding quotes).
- Docstrings are Google-style. `Examples:` / `Example:` is always a top-level section; cut the docstring at the first occurrence.
- 39 public names are defined in more than one file (`sql` → `io/_sql.py` + `sql/sql.py`; `lit`, `concat`, …). Resolve each `__all__` name by following the actual `ImportFrom` chain from the owning package `__init__`, NOT by global-name lookup, to avoid picking the wrong definition.
- **Re-exports are multi-hop.** `daft` re-exports `DataFrame` from `daft.dataframe`, which re-exports it from `daft.dataframe.dataframe`. Resolution must follow the chain recursively (bounded depth), not stop at the first `__init__.py`. A naive one-hop resolver gets only 76/133 for `daft`; the recursive resolver gets 133/133.
- **Not every `__all__` entry is a `def`/`class`.** The `daft` list (133) resolves to 112 defs/classes, 18 submodule exports (`io`, `functions`, `datasets`, `runners`, `context`, …), and 3 module-level objects. So `Symbol` carries a `kind` field: `"def"`, `"submodule"`, or `"object"`. Only `"def"` symbols get a `python` signature block; the other two render as a name + best-effort description.
- **Bare aliases are followed.** `range = _range` in `daft/__init__.py` resolves through the RHS name to `def _range` in `daft/io/_range.py`, inheriting its real signature and docstring — so `daft.range` is documented like the function it aliases.
- **Overload stubs.** Some functions (e.g. `_range`) have multiple `@overload` `def`s; only the final concrete one has a docstring. `_find` must return the matching def **that has a docstring**, falling back to the last one — never the first overload stub.
- 13 symbols are Rust-native (no `.py` node): `IOConfig`, `S3Config`, `S3Credentials`, `GCSConfig`, `AzureConfig`, `HTTPConfig`, `CosConfig`, `TosConfig`, `GooseFSConfig`, `GravitinoConfig`, `UnityConfig`, `HuggingFaceConfig`, `KeyFilteringSettings`. All 13 exist in `daft/daft/__init__.pyi` with full `__init__` signatures and docstrings.
- Class symbols (e.g. `S3Config`, `DataSource`) take their signature from the `__init__` method (minus `self`); if no `__init__`, signature is `()`.
- Per-namespace `functions-*` counts that must sum to 356: str 59, datetime 60, numeric 48, spatial 39 (spatial.py+spatial_index.py), list 20, agg 21, window 8, misc 22, media 34 (image+image_file_+video+audio+url+file_+hdf5), ai 5, etc 40 (binary+bitwise+columnar+struct+distance+similarity+partition+llm+process).

---

## Task 1: Skill scaffold + public-API resolution

**Files:**
- Create: `.claude/skills/daft-etl-pipelines/scripts/gen_reference.py`
- Create: `.claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py`

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
  - `@dataclass(frozen=True) class Symbol` with fields: `name: str`, `namespace: str` (one of `"daft"`, `"daft.functions"`, `"daft.io"`, `"DataFrame"`, `"Expression"`), `source_module: str` (path relative to repo root), `node` (the resolved `ast.AST` node, or `None` for a submodule), `kind: str` (`"def"`, `"submodule"`, or `"object"`), `is_class: bool`.
  - `def resolve_public_api(repo_root: Path) -> tuple[list[Symbol], list[str]]` — returns `(symbols, unresolved_names)`. `symbols` deduplicated by `(namespace, name)`; `unresolved_names` lists `__all__` names that located nothing (expected empty).
  - Module constants `REPO_ROOT`, `DAFT = REPO_ROOT / "daft"`, `STUB = DAFT / "daft" / "__init__.pyi"`.

- [ ] **Step 1: Write the failing test**

Create `.claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py`:

```python
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import gen_reference as gen  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[4]


def test_resolves_full_public_api():
    symbols, unresolved = gen.resolve_public_api(REPO_ROOT)
    by_ns = {}
    for s in symbols:
        by_ns.setdefault(s.namespace, set()).add(s.name)

    # No name in any __all__ failed to resolve (Rust-native ones come from the stub).
    assert unresolved == [], f"unresolved: {unresolved}"

    # Class method counts match the class bodies.
    assert len(by_ns["DataFrame"]) == 98
    assert len(by_ns["Expression"]) == 247

    # The three __all__ lists resolved fully (io=37: private _range excluded).
    assert len(by_ns["daft"]) == 133
    assert len(by_ns["daft.functions"]) == 356
    assert len(by_ns["daft.io"]) == 37

    # Total (namespace, name) records — each list keeps its own namespace, so
    # the 23 names shared by daft and daft.io are two records here (collapsed
    # only at render time). 133 + 356 + 37 + 98 + 247 = 871.
    assert len({(s.namespace, s.name) for s in symbols}) == 871

    # The ai subpackage resolved.
    assert "embed_text" in by_ns["daft.functions"]

    # A Rust-native config resolved from the stub.
    s3 = next(s for s in symbols if s.name == "S3Config" and s.namespace == "daft.io")
    assert s3.source_module.endswith("daft/daft/__init__.pyi")
    assert s3.is_class and s3.kind == "def"

    # Multi-hop re-export resolved (daft -> daft.dataframe -> dataframe.py).
    df_cls = next(s for s in symbols if s.name == "DataFrame" and s.namespace == "daft")
    assert df_cls.kind == "def"

    # Submodule export kept as kind="submodule".
    io_mod = next(s for s in symbols if s.name == "io" and s.namespace == "daft")
    assert io_mod.kind == "submodule"

    # Bare alias followed to the underlying def: `range` -> `_range`.
    import ast
    rng = next(s for s in symbols if s.name == "range" and s.namespace == "daft")
    assert rng.kind == "def"
    assert (ast.get_docstring(rng.node) or "").startswith("Creates a DataFrame")


def test_resolves_all_functions():
    symbols, _ = gen.resolve_public_api(REPO_ROOT)
    fns = {s.name for s in symbols if s.namespace == "daft.functions"}
    # Every daft.functions __all__ name resolved to a real def (no objects).
    kinds = {s.kind for s in symbols if s.namespace == "daft.functions"}
    assert kinds == {"def"}
    assert "regexp_replace" in fns
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py::test_resolves_full_public_api -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'gen_reference'`.

- [ ] **Step 3: Write minimal implementation**

Create `.claude/skills/daft-etl-pipelines/scripts/gen_reference.py`:

```python
"""Generate the daft-etl-pipelines API reference from Daft source via ast.

Never imports daft. Parses daft/**/*.py plus the Rust stub
daft/daft/__init__.pyi. Run from anywhere:

    python3 .claude/skills/daft-etl-pipelines/scripts/gen_reference.py [--check]
"""
from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
DAFT = REPO_ROOT / "daft"
STUB = DAFT / "daft" / "__init__.pyi"

DEF_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
MAX_HOPS = 8


@dataclass(frozen=True)
class Symbol:
    name: str
    namespace: str  # daft | daft.functions | daft.io | DataFrame | Expression
    source_module: str  # path relative to REPO_ROOT
    node: ast.AST | None  # None only for kind == "submodule"
    kind: str  # "def" | "submodule" | "object"
    is_class: bool


def _parse(path: Path) -> ast.Module | None:
    try:
        return ast.parse(path.read_text())
    except (SyntaxError, UnicodeDecodeError, OSError):
        return None


def _all_names(init_path: Path) -> list[str]:
    m = re.search(r"__all__\s*=\s*\[(.*?)\n\]", init_path.read_text(), re.S)
    return [n for n in re.findall(r'"([^"]+)"', m.group(1)) if not n.startswith("_")]


def _find_def(module: ast.Module, name: str):
    """Return the def/class node for `name`, preferring one with a docstring
    (handles @overload stubs where only the concrete def is documented)."""
    matches = [n for n in module.body if isinstance(n, DEF_NODES) and n.name == name]
    if matches:
        documented = [n for n in matches if ast.get_docstring(n)]
        return documented[-1] if documented else matches[-1]
    return None


def _find_assign(module: ast.Module, name: str):
    """Return (rhs, node) for a module-level `name = ...` / `name: T = ...`."""
    for n in module.body:
        if isinstance(n, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in n.targets
        ):
            return n.value, n
        if isinstance(n, ast.AnnAssign) and isinstance(n.target, ast.Name) \
                and n.target.id == name:
            return n.value, n
    return None, None


def _import_map(module_path: Path) -> dict[str, Path]:
    """Map each name imported in `module_path` to the file it comes from.

    Handles `from .mod import x`, `from daft.pkg.mod import x`, and
    `from . import submod` (module import; target is the submodule file)."""
    tree = _parse(module_path)
    pkg_dir = module_path.parent
    out: dict[str, Path] = {}
    if tree is None:
        return out
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.module is not None:
            if node.level > 0:
                base = pkg_dir
                for part in node.module.split("."):
                    base = base / part
            else:
                parts = node.module.split(".")
                if parts[0] != "daft":
                    continue
                base = REPO_ROOT.joinpath(*parts)
            for cand in (base.with_suffix(".py"), base / "__init__.py"):
                if cand.exists():
                    for alias in node.names:
                        out[alias.asname or alias.name] = cand
                    break
        elif node.level > 0:  # from . import submodule
            for alias in node.names:
                sub = pkg_dir / alias.name
                for cand in (sub.with_suffix(".py"), sub / "__init__.py"):
                    if cand.exists():
                        out[alias.asname or alias.name] = cand
                        break
    return out


def _submodule_file(module_path: Path, name: str) -> Path | None:
    pkg_dir = module_path.parent
    for cand in (pkg_dir / f"{name}.py", pkg_dir / name / "__init__.py"):
        if cand.exists():
            return cand
    return None


def _locate(name: str, module_path: Path, depth: int = 0):
    """Resolve `name` starting at `module_path`. Returns (kind, node, source_path).

    Order: a def/class here; a submodule here; a module-level assignment here
    (bare-Name RHS is followed to the underlying def); otherwise follow the
    import chain recursively. Returns (None, None, None) when nothing matches."""
    if depth > MAX_HOPS:
        return None, None, None
    module = _parse(module_path)
    if module is None:
        return None, None, None

    node = _find_def(module, name)
    if node is not None:
        return "def", node, module_path

    sub = _submodule_file(module_path, name)
    if sub is not None:
        return "submodule", None, sub

    rhs, assign_node = _find_assign(module, name)
    if assign_node is not None:
        if isinstance(rhs, ast.Name):  # bare alias: follow it in this module
            k, n, src = _locate(rhs.id, module_path, depth + 1)
            if n is not None:
                return k, n, src
        return "object", assign_node, module_path

    target = _import_map(module_path).get(name)
    if target is not None and target != module_path:
        return _locate(name, target, depth + 1)

    return None, None, None


def _resolve_list(init_path: Path, namespace: str, stub: dict[str, ast.AST]):
    symbols, unresolved = [], []
    for name in _all_names(init_path):
        kind, node, source = _locate(name, init_path)
        if kind is None and name in stub:  # Rust-native re-export
            kind, node, source = "def", stub[name], STUB
        if kind is None:
            unresolved.append(name)
            continue
        symbols.append(
            Symbol(
                name=name,
                namespace=namespace,
                source_module=str(source.relative_to(REPO_ROOT)),
                node=node,
                kind=kind,
                is_class=isinstance(node, ast.ClassDef),
            )
        )
    return symbols, unresolved


def _stub_symbols() -> dict[str, ast.AST]:
    tree = _parse(STUB)
    return {n.name: n for n in tree.body if isinstance(n, DEF_NODES)} if tree else {}


def _resolve_class_methods(rel_path: str, class_name: str):
    tree = _parse(REPO_ROOT / rel_path)
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return [
                Symbol(
                    name=b.name,
                    namespace=class_name,
                    source_module=rel_path,
                    node=b,
                    kind="def",
                    is_class=False,
                )
                for b in node.body
                if isinstance(b, (ast.FunctionDef, ast.AsyncFunctionDef))
                and not b.name.startswith("_")
            ]
    return []


def resolve_public_api(repo_root: Path) -> tuple[list[Symbol], list[str]]:
    stub = _stub_symbols()
    symbols: list[Symbol] = []
    unresolved: list[str] = []
    for init, ns in [
        (DAFT / "__init__.py", "daft"),
        (DAFT / "functions" / "__init__.py", "daft.functions"),
        (DAFT / "io" / "__init__.py", "daft.io"),
    ]:
        syms, unres = _resolve_list(init, ns, stub)
        symbols += syms
        unresolved += unres
    symbols += _resolve_class_methods("daft/dataframe/dataframe.py", "DataFrame")
    symbols += _resolve_class_methods("daft/expressions/expressions.py", "Expression")
    seen, deduped = set(), []
    for s in symbols:  # dedup by (namespace, name), keep first
        key = (s.namespace, s.name)
        if key not in seen:
            seen.add(key)
            deduped.append(s)
    return deduped, unresolved
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py::test_resolves_full_public_api -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add .claude/skills/daft-etl-pipelines/scripts/gen_reference.py \
        .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py
git commit -m "feat(skill): resolve Daft public API for reference generator"
```

---

## Task 2: Signature and docstring extraction

**Files:**
- Modify: `.claude/skills/daft-etl-pipelines/scripts/gen_reference.py`
- Modify: `.claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py`

**Interfaces:**
- Consumes: `Symbol` from Task 1.
- Produces:
  - `def extract_signature(sym: Symbol) -> str` — for `kind == "def"`: e.g. `"(other, on=None, ...) -> DataFrame"`, stripping leading `self`/`cls` and surrounding quotes on a forward-ref return annotation, classes using their `__init__` args (or `()`). For `kind in ("submodule", "object")`: returns `""` (no callable signature).
  - `def extract_docstring(sym: Symbol) -> str` — summary + kept sections, `Examples:` onward removed, trailing whitespace stripped. For a submodule (node is None) reads the target module's module-level docstring. Empty string when there is none.
  - `def first_sentence(doc: str) -> str` — first sentence of a docstring for the index line; `""` if empty.

- [ ] **Step 1: Write the failing test**

Append to `test_gen_reference.py`:

```python
def _sym(name, namespace):
    symbols, _ = gen.resolve_public_api(REPO_ROOT)
    return next(s for s in symbols if s.name == name and s.namespace == namespace)


def test_extract_signature_strips_self_and_quotes():
    sig = gen.extract_signature(_sym("join", "DataFrame"))
    assert sig.startswith("(other")
    assert "self" not in sig
    assert sig.rstrip().endswith("-> DataFrame")  # forward-ref quotes removed


def test_extract_signature_uses_init_for_classes():
    sig = gen.extract_signature(_sym("S3Config", "daft.io"))
    assert sig.startswith("(region_name")
    assert "self" not in sig


def test_extract_docstring_drops_examples():
    doc = gen.extract_docstring(_sym("regexp_replace", "daft.functions"))
    assert "Args:" in doc
    assert "Examples:" not in doc
    assert "Example:" not in doc


def test_first_sentence_is_one_line():
    fs = gen.first_sentence(gen.extract_docstring(_sym("regexp_replace", "daft.functions")))
    assert "\n" not in fs
    assert fs.endswith(".") or fs != ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py -k "signature or docstring or sentence" -v`
Expected: FAIL with `AttributeError: module 'gen_reference' has no attribute 'extract_signature'`.

- [ ] **Step 3: Write minimal implementation**

Append to `gen_reference.py`:

```python
_EXAMPLE_MARKERS = ("Examples:", "Example:")


def _def_node_for_signature(sym: Symbol):
    if not sym.is_class:
        return sym.node
    for b in sym.node.body:  # class: use __init__
        if isinstance(b, (ast.FunctionDef, ast.AsyncFunctionDef)) and b.name == "__init__":
            return b
    return None


def extract_signature(sym: Symbol) -> str:
    if sym.kind != "def":  # submodule / module-level object has no signature
        return ""
    node = _def_node_for_signature(sym)
    if node is None:
        return "()"
    args = ast.unparse(node.args)
    # Strip a leading self / cls parameter.
    args = re.sub(r"^\s*(self|cls)\s*(,\s*)?", "", args)
    ret = ""
    if getattr(node, "returns", None) is not None:
        ret_s = ast.unparse(node.returns).strip()
        if len(ret_s) >= 2 and ret_s[0] in "'\"" and ret_s[-1] == ret_s[0]:
            ret_s = ret_s[1:-1]
        ret = f" -> {ret_s}"
    return f"({args}){ret}"


def extract_docstring(sym: Symbol) -> str:
    if sym.kind == "submodule":  # node is None; use the module docstring
        mod = _parse(REPO_ROOT / sym.source_module)
        doc = (ast.get_docstring(mod) if mod else "") or ""
    elif isinstance(sym.node, DEF_NODES):  # def/class only
        doc = ast.get_docstring(sym.node) or ""
    else:  # kind == "object": an Assign node has no docstring
        doc = ""
    if not doc:
        return ""
    cut = len(doc)
    for marker in _EXAMPLE_MARKERS:
        # Match the marker as a section header (start of a line, optionally indented).
        m = re.search(rf"^\s*{re.escape(marker)}\s*$", doc, re.M)
        if m:
            cut = min(cut, m.start())
    return doc[:cut].rstrip()


def first_sentence(doc: str) -> str:
    if not doc:
        return ""
    flat = " ".join(doc.split())
    m = re.match(r"(.+?\.)(\s|$)", flat)
    return m.group(1) if m else flat
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py -k "signature or docstring or sentence" -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add .claude/skills/daft-etl-pipelines/scripts/gen_reference.py \
        .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py
git commit -m "feat(skill): extract signatures and trimmed docstrings"
```

---

## Task 3: File assignment and markdown rendering

**Files:**
- Modify: `.claude/skills/daft-etl-pipelines/scripts/gen_reference.py`
- Modify: `.claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py`

**Interfaces:**
- Consumes: `Symbol`, `extract_signature`, `extract_docstring`, `first_sentence`.
- Produces:
  - `def assign_file(sym: Symbol) -> str` — returns the reference filename (e.g. `"functions-str.md"`). Rules below.
  - `def anchor(name: str) -> str` — lowercase slug; here identical to `name` (Daft symbols are already lowercase/underscore or CamelCase → `name.lower()`).
  - `def render_reference(filename: str, syms: list[Symbol]) -> str` — the body of one reference file.
  - `def render_index(syms: list[Symbol], unresolved: list[str]) -> str` — the `INDEX.md` body.
  - Constant `FUNCTIONS_FILE_BY_MODULE: dict[str, str]` mapping a `daft/functions` source basename to its bucket.

Assignment rules (each symbol lands in exactly one file):
- `namespace == "DataFrame"` → `dataframe.md`; `"Expression"` → `expressions.md`.
- `namespace == "daft"` → `toplevel.md`. (This captures the 23 `read_*` names shared with `daft.io`, because they resolve under `daft` first.)
- `namespace == "daft.io"` AND the `(namespace="daft", name)` pair does not exist → `io.md`. If a `daft` symbol with the same name exists, skip (already in `toplevel.md`).
- `namespace == "daft.functions"` → bucket by `Path(source_module).name` via `FUNCTIONS_FILE_BY_MODULE`; `ai/__init__.py` maps to `functions-ai.md`.

- [ ] **Step 1: Write the failing test**

Append to `test_gen_reference.py`:

```python
def test_assign_file_buckets():
    assert gen.assign_file(_sym("join", "DataFrame")) == "dataframe.md"
    assert gen.assign_file(_sym("list_sort", "Expression")) == "expressions.md"
    assert gen.assign_file(_sym("regexp_replace", "daft.functions")) == "functions-str.md"
    assert gen.assign_file(_sym("embed_text", "daft.functions")) == "functions-ai.md"
    assert gen.assign_file(_sym("read_parquet", "daft")) == "toplevel.md"
    assert gen.assign_file(_sym("S3Config", "daft.io")) == "io.md"


def test_functions_buckets_sum_to_356():
    symbols, _ = gen.resolve_public_api(REPO_ROOT)
    counts = {}
    for s in symbols:
        if s.namespace == "daft.functions":
            counts[gen.assign_file(s)] = counts.get(gen.assign_file(s), 0) + 1
    assert sum(counts.values()) == 356
    assert counts["functions-str.md"] == 59
    assert counts["functions-ai.md"] == 5


def test_render_reference_has_anchor_headings():
    symbols, _ = gen.resolve_public_api(REPO_ROOT)
    strs = [s for s in symbols if gen.assign_file(s) == "functions-str.md"]
    body = gen.render_reference("functions-str.md", strs)
    assert "## regexp_replace" in body
    assert "```python" in body


def test_render_index_line_format():
    symbols, unresolved = gen.resolve_public_api(REPO_ROOT)
    idx = gen.render_index(symbols, unresolved)
    line = next(l for l in idx.splitlines() if l.startswith("regexp_replace |"))
    parts = [p.strip() for p in line.split("|")]
    assert parts[1] == "daft.functions"
    assert parts[-1] == "functions-str.md#regexp_replace"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py -k "assign or buckets or render" -v`
Expected: FAIL with `AttributeError: ... 'assign_file'`.

- [ ] **Step 3: Write minimal implementation**

Append to `gen_reference.py`. First, a module-level set of `daft` names for the io-dedup check is built lazily inside `render_all`; `assign_file` takes an optional `toplevel_names` set:

```python
FUNCTIONS_FILE_BY_MODULE = {
    "str.py": "functions-str.md",
    "datetime.py": "functions-datetime.md",
    "numeric.py": "functions-numeric.md",
    "spatial.py": "functions-spatial.md",
    "spatial_index.py": "functions-spatial.md",
    "list.py": "functions-list.md",
    "agg.py": "functions-agg.md",
    "window.py": "functions-window.md",
    "misc.py": "functions-misc.md",
    "image.py": "functions-media.md",
    "image_file_.py": "functions-media.md",
    "video.py": "functions-media.md",
    "audio.py": "functions-media.md",
    "url.py": "functions-media.md",
    "file_.py": "functions-media.md",
    "hdf5.py": "functions-media.md",
    "__init__.py": "functions-ai.md",  # daft/functions/ai/__init__.py
    "binary.py": "functions-etc.md",
    "bitwise.py": "functions-etc.md",
    "columnar.py": "functions-etc.md",
    "struct.py": "functions-etc.md",
    "distance.py": "functions-etc.md",
    "similarity.py": "functions-etc.md",
    "partition.py": "functions-etc.md",
    "llm.py": "functions-etc.md",
    "process.py": "functions-etc.md",
}


def assign_file(sym: Symbol, toplevel_names: set[str] | None = None) -> str:
    if sym.namespace == "DataFrame":
        return "dataframe.md"
    if sym.namespace == "Expression":
        return "expressions.md"
    if sym.namespace == "daft":
        return "toplevel.md"
    if sym.namespace == "daft.io":
        return "io.md"  # dedup against toplevel happens in render_all
    if sym.namespace == "daft.functions":
        return FUNCTIONS_FILE_BY_MODULE[Path(sym.source_module).name]
    raise ValueError(f"unknown namespace {sym.namespace}")


def anchor(name: str) -> str:
    return name.lower()


_KIND_NOTE = {"submodule": "_(submodule)_", "object": "_(exported object)_"}


def render_reference(filename: str, syms: list[Symbol]) -> str:
    title = filename.replace(".md", "").replace("-", " ")
    lines = [f"# {title}", ""]
    for s in sorted(syms, key=lambda s: s.name.lower()):
        lines.append(f"## {s.name}")
        lines.append("")
        sig = extract_signature(s)
        if sig:  # kind == "def"
            lines += ["```python", f"{s.name}{sig}", "```", ""]
        elif s.kind in _KIND_NOTE:
            lines += [_KIND_NOTE[s.kind], ""]
        doc = extract_docstring(s)
        lines.append(doc if doc else "_(no docstring)_")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def render_index(syms: list[Symbol], unresolved: list[str]) -> str:
    toplevel_names = {s.name for s in syms if s.namespace == "daft"}
    rows = []
    for s in syms:
        if s.namespace == "daft.io" and s.name in toplevel_names:
            continue  # deduped into toplevel.md
        fname = assign_file(s)
        sig = extract_signature(s) or f"({s.kind})"  # placeholder for non-def
        desc = first_sentence(extract_docstring(s)).replace("|", "/")
        rows.append(
            f"{s.name} | {s.namespace} | {sig} | {desc} | {fname}#{anchor(s.name)}"
        )
    rows.sort(key=lambda r: r.lower())
    out = ["# Daft API index", "",
           "`name | namespace | signature | summary | file#anchor`", ""]
    out += rows
    if unresolved:
        out += ["", "## Unresolved", ""] + [f"- {n}" for n in sorted(unresolved)]
    return "\n".join(out).rstrip() + "\n"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py -k "assign or buckets or render" -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add .claude/skills/daft-etl-pipelines/scripts/gen_reference.py \
        .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py
git commit -m "feat(skill): assign symbols to files and render markdown"
```

---

## Task 4: CLI, whole-corpus rendering, determinism, and --check

**Files:**
- Modify: `.claude/skills/daft-etl-pipelines/scripts/gen_reference.py`
- Modify: `.claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py`

**Interfaces:**
- Consumes: everything from Tasks 1-3.
- Produces:
  - `def render_all(repo_root: Path) -> dict[str, str]` — maps each output filename (`"INDEX.md"`, `"toplevel.md"`, …, all 16) to its full text. The io-vs-toplevel dedup is applied here so a shared name renders only in `toplevel.md`.
  - `def write_files(out_dir: Path, files: dict[str, str]) -> None`.
  - `def check(out_dir: Path, files: dict[str, str]) -> list[str]` — returns list of filenames that differ from disk (empty = in sync).
  - `def main(argv=None) -> int` — `--check` compares against `references/`, prints a summary, returns non-zero on drift; default writes files and prints per-file counts.

- [ ] **Step 1: Write the failing test**

Append to `test_gen_reference.py`:

```python
def test_render_all_produces_sixteen_files():
    files = gen.render_all(REPO_ROOT)
    expected = {
        "INDEX.md", "toplevel.md", "dataframe.md", "expressions.md", "io.md",
        "functions-str.md", "functions-datetime.md", "functions-numeric.md",
        "functions-spatial.md", "functions-list.md", "functions-agg.md",
        "functions-window.md", "functions-misc.md", "functions-media.md",
        "functions-ai.md", "functions-etc.md",
    }
    assert set(files) == expected


def test_render_all_is_deterministic():
    assert gen.render_all(REPO_ROOT) == gen.render_all(REPO_ROOT)


def test_index_has_no_duplicate_symbol_column():
    files = gen.render_all(REPO_ROOT)
    body = files["INDEX.md"]
    data_lines = [l for l in body.splitlines() if " | " in l and not l.startswith("`")]
    names = [l.split("|")[0].strip() for l in data_lines]
    # io/toplevel dedup means every index row is unique on (name, namespace).
    keyed = [tuple(p.strip() for p in l.split("|")[:2]) for l in data_lines]
    assert len(keyed) == len(set(keyed))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py -k "render_all or deterministic or duplicate" -v`
Expected: FAIL with `AttributeError: ... 'render_all'`.

- [ ] **Step 3: Write minimal implementation**

Append to `gen_reference.py`:

```python
import argparse
import sys

ALL_FILES = [
    "INDEX.md", "toplevel.md", "dataframe.md", "expressions.md", "io.md",
    "functions-str.md", "functions-datetime.md", "functions-numeric.md",
    "functions-spatial.md", "functions-list.md", "functions-agg.md",
    "functions-window.md", "functions-misc.md", "functions-media.md",
    "functions-ai.md", "functions-etc.md",
]


def render_all(repo_root: Path) -> dict[str, str]:
    symbols, unresolved = resolve_public_api(repo_root)
    toplevel_names = {s.name for s in symbols if s.namespace == "daft"}
    buckets: dict[str, list[Symbol]] = {f: [] for f in ALL_FILES if f != "INDEX.md"}
    for s in symbols:
        if s.namespace == "daft.io" and s.name in toplevel_names:
            continue  # rendered in toplevel.md
        buckets[assign_file(s)].append(s)
    files = {"INDEX.md": render_index(symbols, unresolved)}
    for fname, syms in buckets.items():
        files[fname] = render_reference(fname, syms)
    return files


def write_files(out_dir: Path, files: dict[str, str]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for fname, text in files.items():
        (out_dir / fname).write_text(text)


def check(out_dir: Path, files: dict[str, str]) -> list[str]:
    drifted = []
    for fname, text in files.items():
        path = out_dir / fname
        if not path.exists() or path.read_text() != text:
            drifted.append(fname)
    return drifted


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Generate the Daft skill reference.")
    parser.add_argument("--check", action="store_true",
                        help="Verify references/ is in sync; non-zero exit on drift.")
    args = parser.parse_args(argv)

    out_dir = Path(__file__).resolve().parent.parent / "references"
    files = render_all(REPO_ROOT)
    _, unresolved = resolve_public_api(REPO_ROOT)

    if args.check:
        drifted = check(out_dir, files)
        if drifted:
            print("DRIFT in:", ", ".join(sorted(drifted)), file=sys.stderr)
            return 1
        print("references/ in sync (16 files).")
        return 0

    write_files(out_dir, files)
    for fname in ALL_FILES:
        n = files[fname].count("\n## ") if fname != "INDEX.md" else \
            len([l for l in files[fname].splitlines() if " | " in l and not l.startswith("`")])
        print(f"  wrote references/{fname:<26} {n}")
    if unresolved:
        print("  UNRESOLVED:", ", ".join(sorted(unresolved)), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py -k "render_all or deterministic or duplicate" -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add .claude/skills/daft-etl-pipelines/scripts/gen_reference.py \
        .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py
git commit -m "feat(skill): CLI, whole-corpus render, determinism, --check"
```

---

## Task 5: Generate references + spec-mandated structural tests + commit output

**Files:**
- Create (generated, force-added): `.claude/skills/daft-etl-pipelines/references/*.md` (16 files)
- Modify: `.claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py`

**Interfaces:**
- Consumes: `main`, generated `references/`.
- Produces: the committed reference corpus and the 5 spec structural tests (coverage, no-unresolved, anchor integrity, determinism, no-repr-leakage) run against the on-disk output.

- [ ] **Step 1: Write the failing tests (over on-disk output)**

Append to `test_gen_reference.py`:

```python
REF_DIR = Path(__file__).resolve().parent.parent / "references"


def test_output_on_disk_matches_generator():
    # Spec test 4 (determinism) + proves references/ is committed in sync.
    files = gen.render_all(REPO_ROOT)
    assert gen.check(REF_DIR, files) == []


def test_index_has_no_unresolved_section():
    # Spec test 2.
    assert "## Unresolved" not in (REF_DIR / "INDEX.md").read_text()


def test_every_index_anchor_resolves():
    # Spec test 3: each file#anchor points to a real heading.
    index = (REF_DIR / "INDEX.md").read_text()
    headings = {}
    for f in REF_DIR.glob("*.md"):
        if f.name == "INDEX.md":
            continue
        headings[f.name] = {
            line[3:].strip().lower()
            for line in f.read_text().splitlines()
            if line.startswith("## ")
        }
    for line in index.splitlines():
        if " | " not in line or line.startswith("`"):
            continue
        target = line.split("|")[-1].strip()
        fname, _, anch = target.partition("#")
        assert fname in headings, f"missing file {fname}"
        assert anch in headings[fname], f"dangling anchor {target}"


def test_no_repr_leakage():
    # Spec test 5.
    for f in REF_DIR.glob("*.md"):
        text = f.read_text()
        assert "<ast." not in text
        assert "object at 0x" not in text


def test_coverage_counts():
    # Spec test 1, over on-disk files.
    index = (REF_DIR / "INDEX.md").read_text()
    rows = [l for l in index.splitlines() if " | " in l and not l.startswith("`")]
    # 871 records minus the 23 io names collapsed into toplevel = 848 rows.
    assert len(rows) == 848
    df = (REF_DIR / "dataframe.md").read_text().count("\n## ")
    ex = (REF_DIR / "expressions.md").read_text().count("\n## ")
    assert df == 98
    assert ex == 247
```

Note on the index row count: 871 `(namespace, name)` records minus the 23 names shared between `daft` and `daft.io` (collapsed into one `toplevel.md` row each) = 848 index rows. If Daft's API changes this number, reconcile against the live value rather than assuming the literal is wrong — the count is `len({(s.namespace, s.name) for s in symbols}) - len(io∩daft public names)`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py -k "on_disk or unresolved or anchor or repr or coverage" -v`
Expected: FAIL — `references/` does not exist yet (`test_output_on_disk_matches_generator` reports all 16 files drifted).

- [ ] **Step 3: Generate the references**

Run: `python3 .claude/skills/daft-etl-pipelines/scripts/gen_reference.py`
Expected stdout: 16 `wrote references/...` lines, no `UNRESOLVED` on stderr.

Then confirm the drift check passes:
Run: `python3 .claude/skills/daft-etl-pipelines/scripts/gen_reference.py --check`
Expected: `references/ in sync (16 files).`, exit 0.

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py -v`
Expected: PASS (all tests from Tasks 1-5).

If `test_coverage_counts` fails on the row count, read the actual value from the failure and reconcile: it must equal `len({(s.namespace, s.name) for s in symbols}) - (# names in both daft and daft.io)`. Fix the assertion, not the generator, if the discrepancy is only the hardcoded literal.

- [ ] **Step 5: Commit (force-add the generated markdown)**

```bash
git add .claude/skills/daft-etl-pipelines/scripts/test_gen_reference.py
git add -f .claude/skills/daft-etl-pipelines/references/
git commit -m "feat(skill): generate committed Daft API reference (16 files)"
```

---

## Task 6: Hand-write SKILL.md

**Files:**
- Create (force-added): `.claude/skills/daft-etl-pipelines/SKILL.md`

**Interfaces:**
- Consumes: the generated `references/INDEX.md` (referenced by path from prose).
- Produces: the always-loaded router. No test cycle (prose); verified by the Task 7 behavioral checklist.

- [ ] **Step 1: Write SKILL.md**

Create `.claude/skills/daft-etl-pipelines/SKILL.md` with exactly this content:

````markdown
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
````

- [ ] **Step 2: Verify the referenced symbols exist in the generated index**

Run:
```bash
for s in date hour list_sort list_count contains year explode unpivot pivot count_rows; do
  grep -qi "^$s " .claude/skills/daft-etl-pipelines/references/INDEX.md \
    && echo "OK  $s" || echo "MISSING $s"
done
```
Expected: every line `OK`. If any is `MISSING`, correct the SKILL.md example to a symbol that exists (grep the index for the right name) — do not leave a nonexistent symbol in the guidance.

- [ ] **Step 3: Commit (force-add)**

```bash
git add -f .claude/skills/daft-etl-pipelines/SKILL.md
git commit -m "feat(skill): hand-written SKILL.md router and transform guide"
```

---

## Task 7: Full verification and behavioral spot-check

**Files:** none created; this task validates the whole skill.

**Interfaces:** consumes everything.

- [ ] **Step 1: Run the full generator test suite**

Run: `python3 -m pytest .claude/skills/daft-etl-pipelines/scripts/ -v`
Expected: all tests PASS.

- [ ] **Step 2: Confirm drift check is clean**

Run: `python3 .claude/skills/daft-etl-pipelines/scripts/gen_reference.py --check`
Expected: `references/ in sync (16 files).`, exit 0.

- [ ] **Step 3: Confirm the commit set is correct**

Run: `git status --short && git ls-files .claude/skills/daft-etl-pipelines/ | wc -l`
Expected: clean working tree; `git ls-files` lists `SKILL.md`, `scripts/gen_reference.py`, `scripts/test_gen_reference.py`, and 16 `references/*.md` = 19 tracked files.

- [ ] **Step 4: Behavioral spot-check (manual, against the index)**

For each prompt, confirm the index steers to the right symbol and away from the wrong one:

| Task | Wrong output to avoid | Grep to confirm right answer exists |
|---|---|---|
| lowercase a string column | `.str.lower()` | `grep -i '^lower ' references/INDEX.md` → Expression `lower` |
| extract year from timestamp | `.dt.year()` | `grep -i '^year ' references/INDEX.md` → Expression `year` |
| sort a list column | `.list.sort()` | `grep -i '^list_sort ' references/INDEX.md` |
| count rows | `collect()` then `len(...)` | `grep -i '^count_rows ' references/INDEX.md` → `DataFrame` |

Expected: each right-hand grep returns a row; SKILL.md section 3 and 4 already document the wrong forms.

- [ ] **Step 5: Final commit if anything changed**

If Steps 1-4 required fixes:
```bash
git add -A .claude/skills/daft-etl-pipelines/scripts/
git add -f .claude/skills/daft-etl-pipelines/SKILL.md .claude/skills/daft-etl-pipelines/references/
git commit -m "fix(skill): reconcile reference and guidance after verification"
```
Otherwise, working tree is already clean — nothing to commit.

---

## Self-Review

**Spec coverage:**
- Namespace-mirrored layout + 16 files + INDEX.md → Tasks 3, 4, 5. ✓
- Grep-first pipe-delimited index → Task 3 (`render_index`), Task 6 (protocol). ✓
- Generator: pure `ast`, no `daft` import, recursive incl. `ai/` → Tasks 1-4, Global Constraints. ✓
- Resolution via import chain + Rust stub for native symbols → Task 1 (`_import_map`, `_stub_symbols`). This *extends* the spec's resolution pass, which did not anticipate Rust-native symbols; flagged in handoff. ✓
- Signature via `ast.unparse`, strip self, class `__init__` → Task 2. ✓
- Drop `Examples:` → Task 2 (`extract_docstring`), Global Constraints. ✓
- Determinism (alpha sort, no timestamps, byte-identical) → Task 4, Task 5 test. ✓
- `--check` drift → Task 4, Task 7. ✓
- Failure modes: unresolved section, no-docstring tag, parse-skip → Task 1 (`_parse` returns None), Task 3 (`_(no docstring)_`), Task 3/4 (`## Unresolved`). ✓
- 5 structural tests (coverage, no-unresolved, anchor integrity, determinism, no-repr) → Task 5. ✓
- SKILL.md 6 sections (lookup, two styles, no accessors, laziness, transforms, cross-links) → Task 6. ✓
- Behavioral spot-check table → Task 7. ✓
- Commit mechanics / `git add -f` → Tasks 5, 6, 7. ✓

**Placeholder scan:** No TBD/TODO; every code step carries full code; every test step names the exact `pytest` invocation and expected result. ✓

**Type consistency:** `Symbol` fields (`name`, `namespace`, `source_module`, `node`, `is_class`) used identically in Tasks 1-4. `resolve_public_api` returns `(symbols, unresolved)` everywhere. `render_all` keys match `ALL_FILES` and the Task 5 test's `expected` set. `assign_file` return values match `FUNCTIONS_FILE_BY_MODULE` values and the `ALL_FILES` list. ✓

**One deviation from spec to flag:** the spec's resolution pass described locating nodes only in `.py` modules; implementation adds `daft/daft/__init__.pyi` parsing because 13 io/toplevel config symbols are Rust-native. Without it, `io.md` would be almost empty. This strengthens the design and is surfaced in the execution handoff.
