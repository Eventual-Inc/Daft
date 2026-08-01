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


def test_first_sentence_does_not_clip_at_abbreviation():
    # Regression: IOConfig's docstring is "Configuration for the native I/O
    # layer, e.g. credentials for accessing cloud storage systems." — the
    # naive "first period ends the sentence" rule clipped this to "...e.g."
    # Find IOConfig via the Rust stub directly (it's not in an __all__ list
    # resolved by resolve_public_api's namespaces alone for this doc check;
    # go through the actual symbol used in INDEX.md/toplevel.md instead).
    symbols, _ = gen.resolve_public_api(REPO_ROOT)
    io_config = next(s for s in symbols if s.name == "IOConfig" and s.namespace == "daft")
    doc = gen.extract_docstring(io_config)
    fs = gen.first_sentence(doc)
    assert fs == (
        "Configuration for the native I/O layer, e.g. credentials for "
        "accessing cloud storage systems."
    )
    assert not fs.endswith("e.g.")

    # Ordinary sentences still split on the real terminal period.
    assert gen.first_sentence("Short doc, i.e. a thing. Next sentence.") == "Short doc, i.e. a thing."
    assert gen.first_sentence("Normal sentence. Second one.") == "Normal sentence."


def test_name_imported_callables_resolve_as_defs_not_submodules():
    # Regression for the resolution-ordering bug in _locate: a name-import of
    # a symbol from a subpackage (e.g. `from daft.sql import sql, sql_expr`,
    # `from daft.udf import udf, udaf, ...`) must win over the sibling-module
    # probe, since the imported name shadows any coincidentally-named sibling
    # package/module (daft/sql/, daft/udf/). Import-map resolution must run
    # BEFORE the submodule probe in _locate, and recursively at every hop.
    for name in ("sql", "udf", "udaf"):
        sym = _sym(name, "daft")
        assert sym.kind == "def", f"{name} resolved as {sym.kind}, expected def"
        assert gen.extract_signature(sym) != "", f"{name} has no signature"

    # The genuine module-import submodules (`from daft import io`, etc., which
    # map back to daft/__init__.py itself and so correctly fall through to
    # the submodule probe) must stay kind="submodule". Enumerated from the
    # actual resolved daft namespace (not assumed): {context, datasets,
    # functions, io, metrics, runners} — 6 names, not 7 ("session" resolves
    # to a real `def session(...)` in daft/session.py, not a submodule).
    symbols, _ = gen.resolve_public_api(REPO_ROOT)
    daft_submodules = {s.name for s in symbols if s.namespace == "daft" and s.kind == "submodule"}
    assert daft_submodules == {"context", "datasets", "functions", "io", "metrics", "runners"}

    # Unchanged behaviors: bare alias still followed to a def; decorator
    # instance assign still an object.
    assert _sym("range", "daft").kind == "def"
    assert _sym("func", "daft").kind == "object"

    # Totals unchanged by the reorder.
    by_ns = {}
    for s in symbols:
        by_ns.setdefault(s.namespace, set()).add(s.name)
    assert len(by_ns["daft"]) == 133
    assert len(by_ns["daft.functions"]) == 356
    assert len(by_ns["daft.io"]) == 37
    assert len(by_ns["DataFrame"]) == 98
    assert len(by_ns["Expression"]) == 247
    assert len({(s.namespace, s.name) for s in symbols}) == 871
    _, unresolved = gen.resolve_public_api(REPO_ROOT)
    assert unresolved == []


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
