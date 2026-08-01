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
