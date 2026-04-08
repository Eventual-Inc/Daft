#!/usr/bin/env python3
"""Check that the public Daft API is covered in the documentation."""

import inspect
import re
import sys
from pathlib import Path

import tomllib

DOCS_DIR = Path(__file__).parent.parent
PROJECT_ROOT = DOCS_DIR.parent
META_EXTRAS = {"all", "dev", "docs", "testing", "benchmarking"}


def scan_doc_refs(paths: list[Path]) -> set[str]:
    """Extract API names from mkdocstrings directives, backtick refs, and link refs."""
    refs: set[str] = set()
    for path in paths:
        if not path.exists():
            continue
        text = path.read_text()
        # ::: daft.foo or ::: daft.DataFrame.foo
        refs.update(m.group(1) for m in re.finditer(r":::\s+([\w.]+)", text))
        # `daft.foo` or `foo`
        refs.update(m.group(1) for m in re.finditer(r"`([\w.]+)`", text))
        # [text][daft.foo.bar]
        refs.update(m.group(1) for m in re.finditer(r"\]\[([\w.]+)\]", text))
    return refs


def check_io(daft_mod) -> list[str]:
    issues = []
    io_pages = [DOCS_DIR / "api" / "io.md", DOCS_DIR / "connectors" / "index.md"]
    refs = scan_doc_refs(io_pages)

    read_funcs = [n for n in dir(daft_mod) if n.startswith("read_") or n.startswith("from_")]
    for name in sorted(read_funcs):
        full = f"daft.{name}"
        if not any(name in r or full in r for r in refs):
            issues.append(f"  [io] Undocumented I/O function: {full}")

    write_methods = [n for n in dir(daft_mod.DataFrame) if n.startswith("write_")]
    for name in sorted(write_methods):
        full = f"daft.dataframe.DataFrame.{name}"
        short = f"DataFrame.{name}"
        if not any(name in r or full in r or short in r for r in refs):
            issues.append(f"  [io] Undocumented write method: daft.DataFrame.{name}")

    return issues


def check_install_extras() -> list[str]:
    issues = []
    pyproject = PROJECT_ROOT / "pyproject.toml"
    with open(pyproject, "rb") as f:
        data = tomllib.load(f)

    toml_extras = {k for k in data.get("project", {}).get("optional-dependencies", {}) if k not in META_EXTRAS}

    install_md = DOCS_DIR / "install.md"
    text = install_md.read_text()
    doc_extras = set(re.findall(r'data-extra="([^"]+)"', text))

    for extra in sorted(toml_extras - doc_extras):
        issues.append(f"  [install] Extra not documented: {extra}")
    for extra in sorted(doc_extras - toml_extras):
        issues.append(f"  [install] Phantom extra (docs but no pyproject entry): {extra}")

    return issues


def check_udf_params(daft_mod) -> list[str]:
    issues = []
    udf_pages = [
        DOCS_DIR / "custom-code" / "func.md",
        DOCS_DIR / "custom-code" / "cls.md",
        DOCS_DIR / "api" / "udf.md",
    ]
    text = "\n".join(p.read_text() for p in udf_pages if p.exists())

    decorators = {
        "daft.func": daft_mod.func,
        "daft.cls": daft_mod.cls,
        "daft.method": daft_mod.method,
        "daft.udf": daft_mod.udf,
    }

    for dec_name, dec in decorators.items():
        try:
            sig = inspect.signature(dec)
        except (ValueError, TypeError):
            continue
        for param in sig.parameters:
            if param.startswith("_"):
                continue
            if param not in text:
                issues.append(f"  [udf] Parameter `{param}` of {dec_name} not mentioned in UDF docs")

    return issues


def main():
    import daft

    all_issues: dict[str, list[str]] = {}

    io_issues = check_io(daft)
    if io_issues:
        all_issues["I/O Functions & Write Methods"] = io_issues

    install_issues = check_install_extras()
    if install_issues:
        all_issues["Install Extras"] = install_issues

    udf_issues = check_udf_params(daft)
    if udf_issues:
        all_issues["UDF Decorator Parameters"] = udf_issues

    if not all_issues:
        print("OK: No API coverage issues found.")
        sys.exit(0)

    print("API Coverage Issues Found:")
    for category, issues in all_issues.items():
        print(f"\n{category}:")
        for issue in issues:
            print(issue)

    total = sum(len(v) for v in all_issues.values())
    print(f"\nTotal: {total} issue(s)")
    sys.exit(1)


if __name__ == "__main__":
    main()
