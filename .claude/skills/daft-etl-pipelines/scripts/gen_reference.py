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
