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


def _exists_case_sensitive(path: Path, base: Path) -> bool:
    """Case-sensitive existence check for `path`, a descendant of `base`
    (assumed to already exist with correct case).

    macOS's default APFS volumes are case-insensitive, so plain
    `Path.exists()` would let e.g. a `DataFrame` symbol falsely match a
    `dataframe/` package directory. Walk each path segment below `base`,
    confirming its exact-case name is present in its parent directory's
    listing."""
    if not path.exists():
        return False
    current = base
    for part in path.relative_to(base).parts:
        try:
            names = {p.name for p in current.iterdir()}
        except OSError:
            return False
        if part not in names:
            return False
        current = current / part
    return True


def _submodule_file(module_path: Path, name: str) -> Path | None:
    pkg_dir = module_path.parent
    for cand in (pkg_dir / f"{name}.py", pkg_dir / name / "__init__.py"):
        if _exists_case_sensitive(cand, pkg_dir):
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
