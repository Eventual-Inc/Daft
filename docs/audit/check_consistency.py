#!/usr/bin/env python3
"""Cross-reference docs against each other and source code to find inconsistencies."""

import ast
import re
import sys
from collections import defaultdict
from pathlib import Path

DOCS_DIR = Path(__file__).parent.parent
SKIP_DIRS = {"superpowers", "audit"}
LEGACY_UDF_OK = {"custom-code/udfs.md", "custom-code/migration.md"}
BAD_PARAMS = {
    "read_sql": {"partition_on": "partition_col"},
}


def iter_doc_files() -> list[Path]:
    return [p for p in DOCS_DIR.rglob("*.md") if not any(part in SKIP_DIRS for part in p.parts)]


def extract_code_blocks(text: str) -> list[tuple[int, str]]:
    """Return list of (start_line, code) for ```python or ```py fenced blocks."""
    blocks = []
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        if re.match(r"^```py(?:thon)?\s*$", lines[i]):
            start = i + 1
            i += 1
            chunk = []
            while i < len(lines) and not lines[i].startswith("```"):
                chunk.append(lines[i])
                i += 1
            blocks.append((start + 1, "\n".join(chunk)))
        i += 1
    return blocks


def check_syntax(files: list[Path]) -> list[str]:
    issues = []
    for path in files:
        text = path.read_text(errors="replace")
        rel = path.relative_to(DOCS_DIR)
        for lineno, code in extract_code_blocks(text):
            try:
                ast.parse(code)
            except SyntaxError as e:
                issues.append(f"SYNTAX: {rel}:{lineno + (e.lineno or 1) - 1} - {e.msg}")
    return issues


def check_api_names(files: list[Path], daft_mod) -> list[str]:
    issues = []
    for path in files:
        text = path.read_text(errors="replace")
        rel = path.relative_to(DOCS_DIR)
        for lineno, code in extract_code_blocks(text):
            try:
                tree = ast.parse(code)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if (
                    isinstance(node, ast.Attribute)
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "daft"
                    and not node.attr.startswith("_")
                    and not hasattr(daft_mod, node.attr)
                ):
                    issues.append(f"API_NAME: {rel}:{lineno + node.col_offset - 1} - daft.{node.attr} does not exist")
    return issues


def check_import_consistency(files: list[Path]) -> list[str]:
    # Map: imported_name -> list of (module_path, rel_file_path)
    imports: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for path in files:
        text = path.read_text(errors="replace")
        rel = str(path.relative_to(DOCS_DIR))
        for _, code in extract_code_blocks(text):
            try:
                tree = ast.parse(code)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("daft."):
                    for alias in node.names:
                        name = alias.asname or alias.name
                        imports[name].append((node.module, rel))

    issues = []
    for name, sources in imports.items():
        modules = {m for m, _ in sources}
        if len(modules) > 1:
            files_str = ", ".join(sorted({f for _, f in sources}))
            modules_str = " vs ".join(sorted(modules))
            issues.append(f"IMPORT: {files_str} - '{name}' imported from multiple paths: {modules_str}")
    return issues


def check_legacy_udf(files: list[Path]) -> list[str]:
    issues = []
    for path in files:
        rel = path.relative_to(DOCS_DIR)
        if str(rel) in LEGACY_UDF_OK:
            continue
        text = path.read_text(errors="replace")
        for i, line in enumerate(text.splitlines(), 1):
            if "@daft.udf(" in line:
                issues.append(f"LEGACY_UDF: {rel}:{i} - @daft.udf( used outside legacy pages")
    return issues


def check_bad_params(files: list[Path]) -> list[str]:
    issues = []
    for path in files:
        text = path.read_text(errors="replace")
        rel = path.relative_to(DOCS_DIR)
        for lineno, code in extract_code_blocks(text):
            try:
                tree = ast.parse(code)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func_name = None
                if isinstance(node.func, ast.Attribute):
                    func_name = node.func.attr
                elif isinstance(node.func, ast.Name):
                    func_name = node.func.id
                if func_name not in BAD_PARAMS:
                    continue
                for kw in node.keywords:
                    if kw.arg in BAD_PARAMS[func_name]:
                        correct = BAD_PARAMS[func_name][kw.arg]
                        line = lineno + (node.lineno or 1) - 1
                        issues.append(f"BAD_PARAM: {rel}:{line} - {func_name}({kw.arg}=...) should be {correct}=")
    return issues


def main():
    import daft

    files = iter_doc_files()
    all_issues = (
        check_syntax(files)
        + check_api_names(files, daft)
        + check_import_consistency(files)
        + check_legacy_udf(files)
        + check_bad_params(files)
    )

    if not all_issues:
        print("OK: No consistency issues found.")
        sys.exit(0)

    for issue in sorted(all_issues):
        print(issue)
    print(f"\nTotal: {len(all_issues)} issue(s)")
    sys.exit(1)


if __name__ == "__main__":
    main()
