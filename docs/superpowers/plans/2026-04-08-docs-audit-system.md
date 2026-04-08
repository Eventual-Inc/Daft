# Docs Audit System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a two-tier automated docs audit system: deterministic CI scripts in the Daft repo + an agent-based task runner in a separate repo.

**Architecture:** Tier 1 lives in `docs/audit/` -- three Python scripts using `griffe` + `ast` + `inspect` for static analysis, plus `pytest-markdown-docs` for code block execution with mock AI providers. Tier 2 lives in a new `Eventual-Inc/daft-docs-audit` private repo -- a runner that gives Claude hard tasks with docs-only context and executes the generated code.

**Tech Stack:** Python, griffe, ast, inspect, pytest-markdown-docs, interrogate, anthropic SDK, GitHub Actions

---

### Task 1: Enable mkdocs strict mode

**Files:**
- Modify: `mkdocs.yml`

- [ ] **Step 1: Add strict mode to mkdocs.yml**

At the top level of `mkdocs.yml`, after the `site_url` line, add:

```yaml
strict: true
```

- [ ] **Step 2: Build docs locally to see what breaks**

Run:
```bash
DAFT_DASHBOARD_SKIP_BUILD=1 make build && JUPYTER_PLATFORM_DIRS=1 uv run mkdocs build -f mkdocs.yml 2>&1 | head -80
```

Expected: Some warnings may become errors. Note them -- these are real issues we'll need to fix or suppress.

- [ ] **Step 3: If there are spurious warnings, add them to validation overrides**

If strict mode produces false-positive errors (e.g., from generated pages or external links), add targeted overrides in `mkdocs.yml`:

```yaml
validation:
  nav:
    omitted_files: info
    not_found: warn
  links:
    not_found: warn
```

Tune until `mkdocs build` passes with strict mode on.

- [ ] **Step 4: Commit**

```bash
git add mkdocs.yml
git commit -m "docs: enable strict mode in mkdocs build"
```

---

### Task 2: Create docs/audit/ directory and `check_api_coverage.py`

**Files:**
- Create: `docs/audit/__init__.py`
- Create: `docs/audit/check_api_coverage.py`

- [ ] **Step 1: Create the audit package**

```python
# docs/audit/__init__.py
```

(Empty file to make it a package.)

- [ ] **Step 2: Write `check_api_coverage.py`**

```python
#!/usr/bin/env python3
"""Check that public Daft APIs are documented.

Compares the live daft module's public API surface against what's
referenced in the documentation markdown files.
"""

import ast
import importlib
import inspect
import re
import sys
from pathlib import Path

DOCS_DIR = Path(__file__).parent.parent
PROJECT_ROOT = DOCS_DIR.parent

# Pages where we expect to find API documentation
IO_PAGE = DOCS_DIR / "api" / "io.md"
DATAFRAME_PAGE = DOCS_DIR / "api" / "dataframe.md"
CONNECTOR_INDEX = DOCS_DIR / "connectors" / "index.md"
INSTALL_PAGE = DOCS_DIR / "install.md"
AI_PAGES = list((DOCS_DIR / "ai-functions").glob("*.md"))
UDF_PAGES = [
    DOCS_DIR / "custom-code" / "func.md",
    DOCS_DIR / "custom-code" / "cls.md",
    DOCS_DIR / "api" / "udf.md",
]

# Threshold for pass/fail
COVERAGE_THRESHOLD = 0.80


def get_public_functions(module_name: str, prefix: str = "") -> set[str]:
    """Get all public functions from a module matching a prefix."""
    mod = importlib.import_module(module_name)
    names = set()
    for name in dir(mod):
        if name.startswith("_"):
            continue
        if prefix and not name.startswith(prefix):
            continue
        obj = getattr(mod, name)
        if callable(obj) or isinstance(obj, property):
            names.add(name)
    return names


def get_public_methods(cls) -> set[str]:
    """Get all public methods from a class."""
    methods = set()
    for name in dir(cls):
        if name.startswith("_"):
            continue
        obj = getattr(cls, name, None)
        if obj is None:
            continue
        if callable(obj) or isinstance(obj, property):
            methods.add(name)
    return methods


def get_function_params(fn) -> set[str]:
    """Get parameter names from a function signature."""
    try:
        sig = inspect.signature(fn)
        return {p for p in sig.parameters if p not in ("self", "cls")}
    except (ValueError, TypeError):
        return set()


def scan_page_for_references(page: Path) -> set[str]:
    """Scan a markdown page for daft API references."""
    if not page.exists():
        return set()
    content = page.read_text()
    refs = set()
    # Match mkdocstrings directives like ::: daft.read_csv
    for m in re.finditer(r":::\s+daft\.(\w+(?:\.\w+)*)", content):
        refs.add(m.group(1).split(".")[-1])
    # Match backtick references like `read_csv` or `daft.read_csv`
    for m in re.finditer(r"`(?:daft\.)?(\w+)`", content):
        refs.add(m.group(1))
    # Match markdown links like [read_csv][daft.io.read_csv]
    for m in re.finditer(r"\[`?(\w+)`?\]\[daft\.\w+\.(\w+)\]", content):
        refs.add(m.group(1))
        refs.add(m.group(2))
    return refs


def scan_pages_for_references(pages: list[Path]) -> set[str]:
    """Scan multiple pages for daft API references."""
    refs = set()
    for page in pages:
        refs |= scan_page_for_references(page)
    return refs


def get_pyproject_extras() -> set[str]:
    """Get all optional dependency keys from pyproject.toml."""
    import tomllib

    pyproject = PROJECT_ROOT / "pyproject.toml"
    with open(pyproject, "rb") as f:
        data = tomllib.load(f)
    return set(data.get("project", {}).get("optional-dependencies", {}).keys())


def scan_install_page_extras() -> set[str]:
    """Scan the install page for documented extras."""
    if not INSTALL_PAGE.exists():
        return set()
    content = INSTALL_PAGE.read_text()
    extras = set()
    # Match data-extra="xxx" attributes in HTML
    for m in re.finditer(r'data-extra="([^"]+)"', content):
        extras.add(m.group(1))
    # Match pip install daft[xxx] patterns
    for m in re.finditer(r'daft\[([^\]]+)\]', content):
        for extra in m.group(1).split(","):
            extras.add(extra.strip())
    return extras


def check_io_coverage() -> list[str]:
    """Check that all read_*/from_*/write_* functions are documented."""
    import daft

    issues = []

    # Top-level read/from functions
    top_level = set()
    for name in dir(daft):
        if name.startswith("_"):
            continue
        if name.startswith("read_") or name.startswith("from_"):
            top_level.add(name)

    io_refs = scan_page_for_references(IO_PAGE)
    connector_refs = scan_page_for_references(CONNECTOR_INDEX)
    all_documented = io_refs | connector_refs

    for name in sorted(top_level - all_documented):
        issues.append(f"UNDOCUMENTED: daft.{name} not found in api/io.md or connectors/index.md")

    # DataFrame write methods
    df_writes = {n for n in get_public_methods(daft.DataFrame) if n.startswith("write_")}
    io_write_refs = {r for r in io_refs if r.startswith("write_")}
    connector_write_refs = {r for r in connector_refs if r.startswith("write_")}
    all_write_documented = io_write_refs | connector_write_refs

    for name in sorted(df_writes - all_write_documented):
        issues.append(f"UNDOCUMENTED: DataFrame.{name} not found in api/io.md or connectors/index.md")

    return issues


def check_dataframe_coverage() -> list[str]:
    """Check that DataFrame methods are documented."""
    import daft

    issues = []
    methods = get_public_methods(daft.DataFrame)
    df_refs = scan_page_for_references(DATAFRAME_PAGE)

    # The dataframe page uses ::: daft.DataFrame which renders all methods,
    # so we just check the directive exists
    content = DATAFRAME_PAGE.read_text() if DATAFRAME_PAGE.exists() else ""
    if "::: daft.DataFrame" not in content and "::: daft.dataframe" not in content:
        for name in sorted(methods):
            issues.append(f"UNDOCUMENTED: DataFrame.{name} - dataframe.md has no auto-doc directive")

    return issues


def check_install_extras() -> list[str]:
    """Check that all pyproject.toml extras are documented on the install page."""
    issues = []
    pyproject_extras = get_pyproject_extras()
    install_extras = scan_install_page_extras()

    # Exclude meta-extras and internal ones
    skip = {"all", "dev", "docs", "testing", "benchmarking"}
    pyproject_extras -= skip

    for name in sorted(pyproject_extras - install_extras):
        issues.append(f"UNDOCUMENTED: pip extra '{name}' not found on install page")

    # Check for phantom extras (documented but don't exist)
    for name in sorted(install_extras - pyproject_extras - skip):
        issues.append(f"PHANTOM: pip extra '{name}' documented on install page but not in pyproject.toml")

    return issues


def check_udf_param_coverage() -> list[str]:
    """Check that UDF decorator parameters are documented."""
    import daft

    issues = []
    udf_refs = scan_pages_for_references(UDF_PAGES)

    # Check @daft.func parameters
    func_params = get_function_params(daft.func.__call__)
    func_params.discard("fn")  # positional arg, not a kwarg users set
    for param in sorted(func_params - udf_refs):
        issues.append(f"UNDOCUMENTED: @daft.func parameter '{param}' not found in UDF docs")

    # Check @daft.cls parameters
    cls_params = get_function_params(daft.cls)
    cls_params.discard("class_")
    for param in sorted(cls_params - udf_refs):
        issues.append(f"UNDOCUMENTED: @daft.cls parameter '{param}' not found in UDF docs")

    return issues


def main() -> int:
    all_issues: list[str] = []

    print("=" * 60)
    print("API Coverage Check")
    print("=" * 60)

    checks = [
        ("I/O Functions", check_io_coverage),
        ("DataFrame Methods", check_dataframe_coverage),
        ("Install Extras", check_install_extras),
        ("UDF Parameters", check_udf_param_coverage),
    ]

    for name, check_fn in checks:
        print(f"\n--- {name} ---")
        issues = check_fn()
        all_issues.extend(issues)
        if issues:
            for issue in issues:
                print(f"  {issue}")
        else:
            print("  All covered!")

    print(f"\n{'=' * 60}")
    print(f"Total issues: {len(all_issues)}")

    if all_issues:
        print("\nFailing due to undocumented APIs.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 3: Run and verify it catches known issues**

```bash
cd /Users/colinho/Desktop/Daft
DAFT_DASHBOARD_SKIP_BUILD=1 make build && python docs/audit/check_api_coverage.py
```

Expected: Should report undocumented items like `daft.read_text`, `daft.read_mcap`, `daft.read_table`, `daft.from_files`, `DataFrame.write_sql`, `DataFrame.write_json`, phantom extra `sentence-transformers`, and missing extras like `sql`, `postgres`, `google`, `video`, `audio`.

- [ ] **Step 4: Commit**

```bash
git add docs/audit/__init__.py docs/audit/check_api_coverage.py
git commit -m "docs(audit): add API coverage checker

Compares daft module public API against docs pages. Catches
undocumented read/write functions, missing install extras,
and undocumented UDF decorator parameters."
```

---

### Task 3: Write `check_consistency.py`

**Files:**
- Create: `docs/audit/check_consistency.py`

- [ ] **Step 1: Write the script**

```python
#!/usr/bin/env python3
"""Check documentation for internal consistency.

Validates that code examples use correct API names, consistent import
patterns, and match actual function signatures.
"""

import ast
import importlib
import inspect
import re
import sys
from collections import defaultdict
from pathlib import Path

DOCS_DIR = Path(__file__).parent.parent

# Pages that are explicitly about the legacy UDF API
LEGACY_UDF_PAGES = {
    DOCS_DIR / "custom-code" / "udfs.md",
    DOCS_DIR / "custom-code" / "migration.md",
}


def extract_code_blocks(md_path: Path) -> list[tuple[int, str]]:
    """Extract Python code blocks from a markdown file. Returns (line_num, code)."""
    content = md_path.read_text()
    blocks = []
    in_block = False
    block_lines = []
    block_start = 0

    for i, line in enumerate(content.split("\n"), 1):
        if re.match(r"^```(?:python|py)\s*$", line.strip()):
            in_block = True
            block_start = i
            block_lines = []
        elif in_block and line.strip() == "```":
            in_block = False
            code = "\n".join(block_lines)
            if code.strip():
                blocks.append((block_start, code))
        elif in_block:
            block_lines.append(line)

    return blocks


def check_syntax(md_path: Path) -> list[str]:
    """Check that all Python code blocks parse without syntax errors."""
    issues = []
    for line_num, code in extract_code_blocks(md_path):
        try:
            ast.parse(code)
        except SyntaxError as e:
            issues.append(
                f"SYNTAX ERROR: {md_path.relative_to(DOCS_DIR)}:{line_num} - {e.msg}"
            )
    return issues


def check_api_names(md_path: Path) -> list[str]:
    """Check that daft.X() calls reference real APIs."""
    import daft

    issues = []
    for line_num, code in extract_code_blocks(md_path):
        try:
            tree = ast.parse(code)
        except SyntaxError:
            continue  # Caught by check_syntax

        for node in ast.walk(tree):
            # Check daft.X attribute access
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                if node.value.id == "daft" and not node.attr.startswith("_"):
                    if not hasattr(daft, node.attr):
                        issues.append(
                            f"BAD API: {md_path.relative_to(DOCS_DIR)}:{line_num} - "
                            f"daft.{node.attr} does not exist"
                        )
    return issues


def check_import_consistency(all_md_files: list[Path]) -> list[str]:
    """Check that the same function isn't imported differently across pages."""
    # Map function_name -> set of (import_path, file)
    import_map: dict[str, list[tuple[str, Path]]] = defaultdict(list)

    for md_path in all_md_files:
        for line_num, code in extract_code_blocks(md_path):
            try:
                tree = ast.parse(code)
            except SyntaxError:
                continue

            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith("daft"):
                    for alias in node.names:
                        name = alias.asname or alias.name
                        import_path = f"from {node.module} import {alias.name}"
                        import_map[name].append((import_path, md_path))

    issues = []
    for name, imports in import_map.items():
        unique_paths = set(path for path, _ in imports)
        if len(unique_paths) > 1:
            files = set(str(f.relative_to(DOCS_DIR)) for _, f in imports)
            paths_str = ", ".join(sorted(unique_paths))
            issues.append(
                f"INCONSISTENT IMPORT: '{name}' imported as: {paths_str} "
                f"(in {', '.join(sorted(files))})"
            )

    return issues


def check_legacy_decorators(md_path: Path) -> list[str]:
    """Flag legacy @daft.udf usage in non-legacy pages."""
    if md_path in LEGACY_UDF_PAGES:
        return []

    issues = []
    content = md_path.read_text()
    for i, line in enumerate(content.split("\n"), 1):
        if "@daft.udf(" in line or "@daft.udf\n" in line:
            issues.append(
                f"LEGACY API: {md_path.relative_to(DOCS_DIR)}:{i} - "
                f"uses @daft.udf (legacy) outside of legacy docs page"
            )
    return issues


def check_param_names(md_path: Path) -> list[str]:
    """Check that keyword arguments in code blocks match real function signatures."""
    issues = []

    # Known function -> param name corrections
    known_bad_params = {
        "read_sql": {"partition_on": "partition_col"},
    }

    for line_num, code in extract_code_blocks(md_path):
        try:
            tree = ast.parse(code)
        except SyntaxError:
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            # Get function name from call
            func_name = None
            if isinstance(node.func, ast.Attribute):
                func_name = node.func.attr
            elif isinstance(node.func, ast.Name):
                func_name = node.func.id

            if func_name and func_name in known_bad_params:
                for kw in node.keywords:
                    if kw.arg in known_bad_params[func_name]:
                        correct = known_bad_params[func_name][kw.arg]
                        issues.append(
                            f"BAD PARAM: {md_path.relative_to(DOCS_DIR)}:{line_num} - "
                            f"{func_name}() uses '{kw.arg}', should be '{correct}'"
                        )

    return issues


def main() -> int:
    all_issues: list[str] = []
    md_files = sorted(DOCS_DIR.rglob("*.md"))
    # Exclude non-doc files
    md_files = [f for f in md_files if "superpowers" not in str(f) and "audit" not in str(f)]

    print("=" * 60)
    print("Consistency Check")
    print("=" * 60)

    # Per-file checks
    for md_path in md_files:
        file_issues = []
        file_issues.extend(check_syntax(md_path))
        file_issues.extend(check_api_names(md_path))
        file_issues.extend(check_legacy_decorators(md_path))
        file_issues.extend(check_param_names(md_path))

        if file_issues:
            all_issues.extend(file_issues)
            for issue in file_issues:
                print(f"  {issue}")

    # Cross-file checks
    print("\n--- Import Consistency ---")
    import_issues = check_import_consistency(md_files)
    all_issues.extend(import_issues)
    for issue in import_issues:
        print(f"  {issue}")
    if not import_issues:
        print("  All consistent!")

    print(f"\n{'=' * 60}")
    print(f"Total issues: {len(all_issues)}")

    if all_issues:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run and verify it catches known issues**

```bash
python docs/audit/check_consistency.py
```

Expected: Should catch the `daft.from_glob_paths` (doesn't exist), `partition_on` bad param name, legacy `@daft.udf` in non-legacy pages, inconsistent `prompt` import paths.

- [ ] **Step 3: Commit**

```bash
git add docs/audit/check_consistency.py
git commit -m "docs(audit): add consistency checker

Validates syntax of code blocks, checks daft.X() calls exist,
flags legacy @daft.udf in non-legacy pages, detects inconsistent
imports across pages, and validates parameter names."
```

---

### Task 4: Write `check_completeness.py`

**Files:**
- Create: `docs/audit/check_completeness.py`

- [ ] **Step 1: Write the script**

```python
#!/usr/bin/env python3
"""Check documentation for structural completeness.

Validates that all markdown files are in navigation, no placeholder
pages exist, and stale roadmap claims are flagged.
"""

import re
import sys
from pathlib import Path

DOCS_DIR = Path(__file__).parent.parent
SUMMARY_FILE = DOCS_DIR / "SUMMARY.md"

# Files that are intentionally not in navigation
NAV_EXCLUSIONS = {
    "SUMMARY.md",
    "robots.txt",
}

# Directories to skip entirely
SKIP_DIRS = {"superpowers", "audit", "gen_pages", "templates", "overrides", "plugins", "css", "js", "img"}

# Minimum word count for a page to not be flagged as a placeholder
MIN_WORD_COUNT = 50

# Patterns that indicate stale "not yet implemented" claims
STALE_PATTERNS = [
    r"not\s+(?:yet\s+)?(?:supported|implemented|available)",
    r"on\s+the\s+roadmap",
    r"(?:will|to)\s+be\s+(?:added|implemented|supported)\s+in\s+(?:a\s+)?future",
    r"does\s+not\s+(?:yet\s+)?(?:have|support|exist)",
    r"currently\s+(?:does\s+not|doesn't)\s+(?:support|have)",
]


def get_nav_files() -> set[str]:
    """Get all files referenced in SUMMARY.md."""
    if not SUMMARY_FILE.exists():
        return set()
    content = SUMMARY_FILE.read_text()
    files = set()
    for m in re.finditer(r"\(([^)]+\.md)\)", content):
        files.add(m.group(1))
    return files


def check_nav_coverage() -> list[str]:
    """Check that every .md file in docs/ is in SUMMARY.md."""
    issues = []
    nav_files = get_nav_files()
    # Also include index.md files referenced as directory links
    nav_files.add("index.md")

    for md_path in sorted(DOCS_DIR.rglob("*.md")):
        rel = md_path.relative_to(DOCS_DIR)
        # Skip excluded dirs and files
        if any(part in SKIP_DIRS for part in rel.parts):
            continue
        if str(rel) in NAV_EXCLUSIONS:
            continue

        rel_str = str(rel)
        if rel_str not in nav_files:
            issues.append(f"NOT IN NAV: {rel_str} exists but is not referenced in SUMMARY.md")

    return issues


def check_placeholder_pages() -> list[str]:
    """Flag pages with minimal content."""
    issues = []
    for md_path in sorted(DOCS_DIR.rglob("*.md")):
        rel = md_path.relative_to(DOCS_DIR)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue
        if str(rel) in NAV_EXCLUSIONS:
            continue

        content = md_path.read_text()
        # Strip frontmatter, HTML comments, and code blocks
        content = re.sub(r"---.*?---", "", content, flags=re.DOTALL)
        content = re.sub(r"<!--.*?-->", "", content, flags=re.DOTALL)
        content = re.sub(r"```.*?```", "", content, flags=re.DOTALL)
        content = re.sub(r":::.*", "", content)  # mkdocstrings directives

        words = content.split()
        if len(words) < MIN_WORD_COUNT:
            issues.append(f"PLACEHOLDER: {rel} has only {len(words)} words of content")

    return issues


def check_commented_content() -> list[str]:
    """Flag large HTML comments that may be accidentally hidden content."""
    issues = []
    for md_path in sorted(DOCS_DIR.rglob("*.md")):
        rel = md_path.relative_to(DOCS_DIR)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue

        content = md_path.read_text()
        for m in re.finditer(r"<!--(.*?)-->", content, re.DOTALL):
            comment = m.group(1).strip()
            if len(comment) > 200:
                # Find line number
                line_num = content[:m.start()].count("\n") + 1
                preview = comment[:80].replace("\n", " ")
                issues.append(
                    f"HIDDEN CONTENT: {rel}:{line_num} - large comment ({len(comment)} chars): "
                    f'"{preview}..."'
                )

    return issues


def check_stale_claims() -> list[str]:
    """Flag 'not yet supported' claims that may be stale."""
    issues = []
    for md_path in sorted(DOCS_DIR.rglob("*.md")):
        rel = md_path.relative_to(DOCS_DIR)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue

        content = md_path.read_text()
        lines = content.split("\n")
        for i, line in enumerate(lines, 1):
            # Skip lines inside code blocks
            if line.strip().startswith("```") or line.strip().startswith(":::"):
                continue
            for pattern in STALE_PATTERNS:
                if re.search(pattern, line, re.IGNORECASE):
                    preview = line.strip()[:100]
                    issues.append(
                        f"STALE?: {rel}:{i} - possibly outdated claim: \"{preview}\""
                    )
                    break  # One match per line is enough

    return issues


def check_connector_index() -> list[str]:
    """Check that every connector page is listed in connectors/index.md."""
    issues = []
    connector_dir = DOCS_DIR / "connectors"
    if not connector_dir.exists():
        return issues

    index_path = connector_dir / "index.md"
    if not index_path.exists():
        issues.append("MISSING: connectors/index.md does not exist")
        return issues

    index_content = index_path.read_text()

    for md_path in sorted(connector_dir.glob("*.md")):
        if md_path.name in ("index.md", "custom.md", "custom-catalogs.md"):
            continue
        name = md_path.stem
        # Check if the connector is referenced in the index
        if name not in index_content and name.replace("_", "-") not in index_content:
            issues.append(
                f"NOT IN INDEX: connectors/{md_path.name} exists but not referenced "
                f"in connectors/index.md"
            )

    return issues


def main() -> int:
    all_issues: list[str] = []

    print("=" * 60)
    print("Completeness Check")
    print("=" * 60)

    checks = [
        ("Navigation Coverage", check_nav_coverage),
        ("Placeholder Pages", check_placeholder_pages),
        ("Hidden Content", check_commented_content),
        ("Stale Claims", check_stale_claims),
        ("Connector Index", check_connector_index),
    ]

    for name, check_fn in checks:
        print(f"\n--- {name} ---")
        issues = check_fn()
        all_issues.extend(issues)
        if issues:
            for issue in issues:
                print(f"  {issue}")
        else:
            print("  All complete!")

    print(f"\n{'=' * 60}")
    print(f"Total issues: {len(all_issues)}")

    # Stale claims are warnings, not errors
    errors = [i for i in all_issues if not i.startswith("STALE?")]
    if errors:
        print(f"\nFailing due to {len(errors)} non-stale issues.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Run and verify**

```bash
python docs/audit/check_completeness.py
```

Expected: Should flag `custom-code/apis.md` and `custom-code/gpu.md` as placeholder pages, `sql/identifiers.md` as not in nav, commented-out FAQ on homepage, and the SQL page's stale `write_sql` roadmap claim.

- [ ] **Step 3: Commit**

```bash
git add docs/audit/check_completeness.py
git commit -m "docs(audit): add completeness checker

Validates nav coverage, flags placeholder pages, detects hidden
HTML comments with substantial content, flags stale roadmap claims,
and checks connector index completeness."
```

---

### Task 5: Set up mock AI provider and pytest-markdown-docs

**Files:**
- Create: `docs/audit/conftest.py`
- Create: `docs/audit/mock_provider.py`

- [ ] **Step 1: Write the mock provider**

This must conform to Daft's `Provider` ABC. Study the actual interface at `daft/ai/provider.py` and the protocol types at `daft/ai/protocols.py`.

```python
# docs/audit/mock_provider.py
"""Mock AI provider for documentation testing.

Returns deterministic responses without making any external API calls.
Used by pytest-markdown-docs to execute code blocks in doc pages,
and by the Tier 2 agent task runner.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from typing_extensions import Unpack

import numpy as np

from daft.ai.provider import Provider
from daft.ai.protocols import (
    Prompter,
    PrompterDescriptor,
    TextClassifier,
    TextClassifierDescriptor,
    TextEmbedder,
    TextEmbedderDescriptor,
)
from daft.ai.typing import (
    ClassifyTextOptions,
    Embedding,
    EmbeddingDimensions,
    EmbedTextOptions,
    Label,
    Options,
    PromptOptions,
    UDFOptions,
)
from daft.datatype import DataType


class MockTextEmbedder:
    """Returns deterministic embedding vectors."""

    def __init__(self, dimensions: int = 1536):
        self.dimensions = dimensions

    def embed_text(self, text: list[str]) -> list[Embedding]:
        return [np.full(self.dimensions, 0.1, dtype=np.float32) for _ in text]


@dataclass
class MockTextEmbedderDescriptor(TextEmbedderDescriptor):
    _dimensions: int = 1536

    def get_provider(self) -> str:
        return "mock"

    def get_model(self) -> str:
        return "mock-embed"

    def get_options(self) -> Options:
        return {}

    def get_dimensions(self) -> EmbeddingDimensions:
        return EmbeddingDimensions(size=self._dimensions, dtype=DataType.float32())

    def instantiate(self) -> TextEmbedder:
        return MockTextEmbedder(self._dimensions)


class MockTextClassifier:
    """Returns the first label for any input."""

    def classify_text(self, text: list[str], labels: Label | list[Label]) -> list[Label]:
        label_list = [labels] if isinstance(labels, str) else labels
        return [label_list[0]] * len(text)


@dataclass
class MockTextClassifierDescriptor(TextClassifierDescriptor):
    def get_provider(self) -> str:
        return "mock"

    def get_model(self) -> str:
        return "mock-classify"

    def get_options(self) -> Options:
        return {}

    def instantiate(self) -> TextClassifier:
        return MockTextClassifier()


class MockPrompter:
    """Returns deterministic prompt responses."""

    def __init__(self, return_format: Any | None = None):
        self.return_format = return_format

    async def prompt(self, messages: tuple[Any, ...]) -> Any:
        if self.return_format is not None:
            return '{"result": "mock", "name": "mock", "category": "mock", "price": 0.0}'
        return "Mock response"


@dataclass
class MockPrompterDescriptor(PrompterDescriptor):
    _return_format: Any | None = None

    def get_provider(self) -> str:
        return "mock"

    def get_model(self) -> str:
        return "mock-prompt"

    def get_options(self) -> Options:
        return {}

    def is_async(self) -> bool:
        return True

    def instantiate(self) -> Prompter:
        return MockPrompter(self._return_format)


class MockProvider(Provider):
    """Mock provider that returns deterministic responses for all AI operations."""

    @property
    def name(self) -> str:
        return "mock"

    def get_text_embedder(
        self, model: str | None = None, dimensions: int | None = None, **options: Unpack[EmbedTextOptions]
    ) -> TextEmbedderDescriptor:
        return MockTextEmbedderDescriptor(_dimensions=dimensions or 1536)

    def get_text_classifier(
        self, model: str | None = None, **options: Unpack[ClassifyTextOptions]
    ) -> TextClassifierDescriptor:
        return MockTextClassifierDescriptor()

    def get_prompter(
        self,
        model: str | None = None,
        return_format: Any | None = None,
        system_message: str | None = None,
        **options: Unpack[PromptOptions],
    ) -> PrompterDescriptor:
        return MockPrompterDescriptor(_return_format=return_format)
```

- [ ] **Step 2: Write conftest.py**

```python
# docs/audit/conftest.py
"""Pytest configuration for docs code block testing.

Registers mock AI providers and sets up environment for
pytest-markdown-docs execution.
"""

import os

import pytest


@pytest.fixture(autouse=True, scope="session")
def setup_mock_providers():
    """Register mock AI provider and set fake API keys."""
    import daft

    from docs.audit.mock_provider import MockProvider

    # Set fake API keys so code that checks env vars doesn't fail
    os.environ.setdefault("OPENAI_API_KEY", "mock-key-for-testing")
    os.environ.setdefault("OPENROUTER_API_KEY", "mock-key-for-testing")
    os.environ.setdefault("GOOGLE_API_KEY", "mock-key-for-testing")
    os.environ.setdefault("HF_TOKEN", "mock-key-for-testing")

    # Register mock provider as both "openai" and "mock"
    mock = MockProvider()
    daft.attach_provider(mock, alias="mock")
    daft.attach_provider(mock, alias="openai")
    daft.set_provider("mock")

    yield

    # Cleanup
    for key in ("OPENAI_API_KEY", "OPENROUTER_API_KEY", "GOOGLE_API_KEY", "HF_TOKEN"):
        os.environ.pop(key, None)


# Paths to existing test fixtures
FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "tests", "assets")
FIXTURE_CSV = os.path.join(FIXTURE_DIR, "mvp.csv")
FIXTURE_PARQUET = os.path.join(FIXTURE_DIR, "parquet-data", "mvp.parquet")
FIXTURE_JSON = os.path.join(FIXTURE_DIR, "json-data", "sample1.json")
FIXTURE_IMAGES = os.path.join(FIXTURE_DIR, "..", "cookbook", "assets", "images")
```

- [ ] **Step 3: Test the mock provider works**

Create a small test file to verify:

```bash
cd /Users/colinho/Desktop/Daft
python -c "
import daft
from docs.audit.mock_provider import MockProvider
mock = MockProvider()
daft.attach_provider(mock, alias='openai')
daft.set_provider('mock')

from daft.functions import prompt, embed_text
df = daft.from_pydict({'text': ['hello', 'world']})
result = df.with_column('response', prompt(daft.col('text'))).collect()
print(result)
print('Mock provider works!')
"
```

Expected: Should print a DataFrame with mock responses without any API calls.

- [ ] **Step 4: Commit**

```bash
git add docs/audit/mock_provider.py docs/audit/conftest.py
git commit -m "docs(audit): add mock AI provider and pytest conftest

MockProvider implements Daft's Provider ABC with deterministic
responses. conftest.py auto-registers it for pytest-markdown-docs
code block execution."
```

---

### Task 6: Add pytest-markdown-docs and interrogate to dependencies

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add dependencies to docs extras**

In `pyproject.toml`, find the `[project.optional-dependencies]` section and the `docs` key. Add `pytest-markdown-docs` and `interrogate`:

```toml
docs = [
    # ... existing deps ...
    "pytest-markdown-docs>=0.6.1",
    "interrogate>=1.7.0",
]
```

- [ ] **Step 2: Sync environment**

```bash
uv sync --all-extras --all-groups
```

- [ ] **Step 3: Verify pytest-markdown-docs works on a simple doc page**

```bash
pytest --markdown-docs docs/quickstart.md -v --tb=short 2>&1 | head -40
```

Expected: Should attempt to execute code blocks from the quickstart. Some will fail (e.g., HuggingFace downloads) -- that's fine for now. The point is that the plugin loads and runs.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "docs(audit): add pytest-markdown-docs and interrogate to deps"
```

---

### Task 7: Add Makefile targets and CI workflow

**Files:**
- Modify: `Makefile`
- Create: `.github/workflows/docs-audit.yml`

- [ ] **Step 1: Add Makefile targets**

Add after the existing `docs-serve` target:

```makefile
.PHONY: docs-audit
docs-audit: .venv ## Run all docs audit checks (no API key needed)
	$(VENV_BIN)/python docs/audit/check_api_coverage.py
	$(VENV_BIN)/python docs/audit/check_consistency.py
	$(VENV_BIN)/python docs/audit/check_completeness.py

.PHONY: docs-audit-code
docs-audit-code: .venv ## Run docs code block validation
	$(VENV_BIN)/pytest --markdown-docs docs/ -v --tb=short

.PHONY: docs-audit-coverage
docs-audit-coverage: .venv ## Run docs API coverage check
	$(VENV_BIN)/python docs/audit/check_api_coverage.py
```

- [ ] **Step 2: Create CI workflow**

```yaml
# .github/workflows/docs-audit.yml
name: Docs Audit

on:
  pull_request:
    paths:
      - 'docs/**'
      - 'daft/**'
      - 'pyproject.toml'
  push:
    branches: [main]
    paths:
      - 'docs/**'
      - 'daft/**'
      - 'pyproject.toml'

permissions:
  contents: read

concurrency:
  group: docs-audit-${{ github.event.pull_request.number || github.ref }}
  cancel-in-progress: true

jobs:
  tier1-audit:
    runs-on: ubuntu-latest
    env:
      python-version: "3.12"
    steps:
      - uses: actions/checkout@v6
      - uses: moonrepo/setup-rust@v1
        with:
          cache: false
      - uses: Swatinem/rust-cache@v2
        with:
          shared-key: ${{ runner.os }}-dev-build
          cache-all-crates: "true"
          cache-workspace-crates: "true"
          save-if: false
      - name: Setup Python and uv
        uses: astral-sh/setup-uv@v7
        with:
          python-version: ${{ env.python-version }}
          enable-cache: true
          cache-dependency-glob: "**/pyproject.toml"
      - name: Setup Virtual Env
        run: |
          uv venv --seed .venv
          echo "$GITHUB_WORKSPACE/.venv/bin" >> $GITHUB_PATH
      - name: Install dependencies
        run: |
          source activate
          uv sync --no-install-project --all-extras --all-groups
      - name: Build Daft
        env:
          DAFT_DASHBOARD_SKIP_BUILD: "1"
        run: |
          source activate
          maturin develop --uv
      - name: Check API coverage
        run: python docs/audit/check_api_coverage.py
      - name: Check consistency
        run: python docs/audit/check_consistency.py
      - name: Check completeness
        run: python docs/audit/check_completeness.py
      - name: Check docstring coverage
        run: interrogate daft/ --fail-under 80 --ignore-init-module --ignore-init-method --ignore-magic --ignore-private --exclude tests --exclude docs --exclude benchmarking -v
```

- [ ] **Step 3: Commit**

```bash
git add Makefile .github/workflows/docs-audit.yml
git commit -m "docs(audit): add Makefile targets and CI workflow

- make docs-audit: runs all Tier 1 checks
- make docs-audit-code: runs pytest-markdown-docs
- make docs-audit-coverage: runs API coverage check
- CI workflow runs on PRs touching docs/ or daft/"
```

---

### Task 8: Run full audit and create baseline

**Files:**
- (no new files -- validation run)

- [ ] **Step 1: Run the full audit suite locally**

```bash
make docs-audit 2>&1 | tee /tmp/docs-audit-baseline.txt
```

- [ ] **Step 2: Review output and categorize issues**

The first run will likely report many issues. Categorize them:
- **Fix now**: Broken code (syntax errors, wrong API names, wrong param names)
- **Fix later**: Missing documentation (undocumented APIs, placeholder pages)
- **Suppress**: Known intentional gaps (e.g., experimental APIs not yet documented)

- [ ] **Step 3: Add suppression lists for known intentional gaps**

If some APIs are intentionally undocumented (experimental, internal), add them to exclusion lists at the top of each script. For example, in `check_api_coverage.py`:

```python
# APIs that are intentionally undocumented (experimental/internal)
SUPPRESSED_APIS = {
    "write_bigtable",
    "write_clickhouse",
    "write_turbopuffer",
    # Add others as needed after reviewing baseline
}
```

- [ ] **Step 4: Re-run and verify clean output**

```bash
make docs-audit
```

Expected: Should pass (exit 0) with only the suppressed items hidden. Any remaining failures are real issues to fix.

- [ ] **Step 5: Commit suppressions**

```bash
git add docs/audit/
git commit -m "docs(audit): add baseline suppressions for intentional gaps"
```

---

### Task 9: Create the daft-docs-audit repo structure

**Files:**
- Create: `daft-docs-audit/` (new repo)

This task creates the separate private repo for the agent-based Tier 2 auditor.

- [ ] **Step 1: Create the repo on GitHub**

```bash
gh repo create Eventual-Inc/daft-docs-audit --private --description "Weekly agent-based documentation audit for Daft" --clone
cd daft-docs-audit
```

- [ ] **Step 2: Create `pyproject.toml`**

```toml
[project]
name = "daft-docs-audit"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "anthropic>=0.52.0",
    "pyyaml>=6.0",
]

[build-system]
requires = ["setuptools"]
build-backend = "setuptools.backends._legacy:_Backend"
```

- [ ] **Step 3: Create `tasks.yaml`**

```yaml
tasks:
  - id: gpu-image-classifier-pipeline
    name: "Build a GPU image classification pipeline with UDFs"
    category: multi-step-pipeline
    execute: true
    docs_context:
      - custom-code/func.md
      - custom-code/cls.md
      - modalities/images.md
      - api/udf.md
    prompt: |
      Using only the Daft documentation provided, write a complete Python script that:
      1. Creates a DataFrame with image file paths using daft.from_pydict
      2. Downloads and decodes the images using daft functions
      3. Writes a @daft.cls class UDF with a setup method that initializes
         a mock classifier (just return a fixed string, no real ML needed)
      4. Specifies GPU resource requests on the @daft.cls decorator
      5. Applies the UDF to the image column
      6. Collects and prints results
      The script must parse without syntax errors and use only real Daft APIs.

  - id: complex-sql-query
    name: "Write a complex Daft SQL query"
    category: sql
    execute: true
    docs_context:
      - sql/index.md
      - sql/statements/select.md
      - sql/window_functions.md
      - configuration/sessions-usage.md
    prompt: |
      Using only the Daft documentation provided, write a complete Python script that:
      1. Creates two DataFrames: orders (id, customer_id, amount, date) and
         customers (id, name, region) using daft.from_pydict with 5+ rows each
      2. Registers them as tables in a Daft session
      3. Writes a SQL query that:
         - Uses a CTE to compute per-customer totals
         - Joins with the customers table
         - Applies ROW_NUMBER() window function partitioned by region
         - Filters with HAVING on the grouped results
      4. Executes the query and prints results
      The script must parse without syntax errors and use only real Daft APIs.

  - id: migrate-legacy-udf
    name: "Migrate a legacy UDF to the new API"
    category: migration
    execute: true
    docs_context:
      - custom-code/migration.md
      - custom-code/func.md
      - custom-code/cls.md
    prompt: |
      Using only the Daft documentation provided, migrate the following
      legacy UDF to the new Daft API. Preserve GPU resource requests,
      concurrency settings, and add error handling:

      ```python
      @daft.udf(return_dtype=daft.DataType.string(), num_gpus=1)
      def classify(images: daft.Series):
          model = load_model()
          return [model.predict(img) for img in images.to_pylist()]
      ```

      Write a complete runnable script that:
      1. Defines the migrated UDF using @daft.cls and @daft.method
      2. Creates a test DataFrame with 5 string values
      3. Applies the UDF (use a mock model that returns "class_A")
      4. Collects and prints results

  - id: async-api-udf-with-retries
    name: "Write an async UDF that calls an API with error handling"
    category: cross-cutting
    execute: true
    docs_context:
      - custom-code/func.md
      - custom-code/cls.md
      - optimization/memory.md
    prompt: |
      Using only the Daft documentation provided, write a complete Python script that:
      1. Defines an async @daft.func that simulates calling an external HTTP API
         (use asyncio.sleep(0.01) as a mock call, return a fixed string)
      2. Configures max_concurrency on the UDF
      3. Configures max_retries and on_error handling
      4. Creates a test DataFrame with 10 rows using daft.from_pydict
      5. Applies the async UDF
      6. Collects and prints results
      The script must parse without syntax errors and use only real Daft APIs.

  - id: scaling-oom-fix
    name: "Fix an OOM pipeline with proper configuration"
    category: scaling
    execute: true
    docs_context:
      - optimization/memory.md
      - optimization/partitioning.md
      - optimization/join-strategies.md
    prompt: |
      Using only the Daft documentation provided, write a complete Python script that:
      1. Creates two DataFrames with 100 rows each using daft.from_pydict
         (columns: id, value, category)
      2. Uses daft.set_execution_config to configure memory-friendly settings
         (e.g., batch size, shuffle algorithm, any other documented params)
      3. Performs an inner join on the id column
      4. Collects and prints results
      The script must parse without syntax errors and use only real Daft APIs.

  - id: batch-inference-structured-output
    name: "Set up batch inference with structured output"
    category: ai-functions
    execute: true
    docs_context:
      - ai-functions/prompt.md
      - ai-functions/providers.md
      - quickstart.md
    prompt: |
      Using only the Daft documentation provided, write a complete Python script that:
      1. Configures a provider using daft.set_provider
      2. Creates a DataFrame with 5 product descriptions using daft.from_pydict
      3. Defines a Pydantic BaseModel for structured output
         (fields: name str, category str, price float)
      4. Uses daft.functions.prompt() with return_format set to the Pydantic model
      5. Collects and prints results
      The script must parse without syntax errors and use only real Daft APIs.

  - id: embed-and-search
    name: "Build a text embedding and semantic search pipeline"
    category: ai-functions
    execute: true
    docs_context:
      - ai-functions/embed.md
      - modalities/embeddings.md
      - api/ai.md
    prompt: |
      Using only the Daft documentation provided, write a complete Python script that:
      1. Creates a DataFrame with 10 text documents using daft.from_pydict
      2. Embeds them using embed_text with a specified model
      3. Creates a second DataFrame with a query string
      4. Embeds the query
      5. Joins and computes cosine_distance between query and documents
      6. Sorts by distance ascending and takes top 3
      7. Collects and prints results
      The script must parse without syntax errors and use only real Daft APIs.

  - id: connector-s3-auth-delta
    name: "Configure S3 auth and read/write Delta Lake"
    category: connector-auth
    execute: false
    docs_context:
      - connectors/delta_lake.md
      - connectors/aws.md
      - api/io.md
    prompt: |
      Using only the Daft documentation provided, write a complete Python script that:
      1. Creates an IOConfig with S3Config specifying access key, secret key,
         and region (use placeholder values)
      2. Reads a Delta Lake table from an S3 path using the IOConfig
      3. Filters rows and selects columns
      4. Writes results to a local Delta Lake path with mode="overwrite"
      The script must parse without syntax errors and use only real Daft APIs.
```

- [ ] **Step 4: Commit initial repo structure**

```bash
git add pyproject.toml tasks.yaml
git commit -m "Initial task definitions for agent-based docs audit"
```

---

### Task 10: Write the agent task runner

**Files:**
- Create: `daft-docs-audit/run_agent_tasks.py`

- [ ] **Step 1: Write the runner**

```python
#!/usr/bin/env python3
"""Agent-based documentation audit runner.

Gives Claude hard tasks with only Daft docs as context, then validates
and optionally executes the generated code.
"""

import ast
import importlib
import os
import re
import subprocess
import sys
import textwrap
from datetime import datetime, timezone
from pathlib import Path

import yaml
from anthropic import Anthropic

DAFT_DIR = Path(os.environ.get("DAFT_DIR", "Daft"))
DOCS_DIR = DAFT_DIR / "docs"
REPORTS_DIR = Path("reports")

SYSTEM_PROMPT = textwrap.dedent("""\
    You are a Python developer using the Daft data framework. You have access
    ONLY to the documentation provided below. Do not use any prior knowledge
    about Daft beyond what is in these docs.

    Write complete, runnable Python code to accomplish the task. The code must:
    - Include all necessary imports
    - Be a single script that can be run with `python script.py`
    - Use only APIs that are documented in the provided docs

    If the documentation is insufficient to complete any step, explain exactly
    what information is missing in a comment starting with "# DOC GAP:".

    Return ONLY a single Python code block.
""")


def load_tasks(path: str = "tasks.yaml") -> list[dict]:
    with open(path) as f:
        data = yaml.safe_load(f)
    return data["tasks"]


def load_doc_context(docs_context: list[str]) -> str:
    """Load markdown files as context for the agent."""
    parts = []
    for rel_path in docs_context:
        full_path = DOCS_DIR / rel_path
        if full_path.exists():
            content = full_path.read_text()
            parts.append(f"--- {rel_path} ---\n{content}")
        else:
            parts.append(f"--- {rel_path} ---\n(FILE NOT FOUND)")
    return "\n\n".join(parts)


def extract_code_block(response: str) -> str | None:
    """Extract the first Python code block from the response."""
    m = re.search(r"```python\n(.*?)```", response, re.DOTALL)
    if m:
        return m.group(1).strip()
    # Try without language specifier
    m = re.search(r"```\n(.*?)```", response, re.DOTALL)
    if m:
        return m.group(1).strip()
    return None


def validate_syntax(code: str) -> str | None:
    """Return error message if code has syntax errors, else None."""
    try:
        ast.parse(code)
        return None
    except SyntaxError as e:
        return f"SyntaxError: {e.msg} (line {e.lineno})"


def validate_api_names(code: str) -> list[str]:
    """Check that daft.X() calls reference real APIs."""
    issues = []
    try:
        import daft

        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                if node.value.id == "daft" and not node.attr.startswith("_"):
                    if not hasattr(daft, node.attr):
                        issues.append(f"daft.{node.attr} does not exist")
    except Exception:
        pass
    return issues


def extract_doc_gaps(code: str) -> list[str]:
    """Extract DOC GAP comments from generated code."""
    gaps = []
    for line in code.split("\n"):
        if "# DOC GAP:" in line:
            gap = line.split("# DOC GAP:")[1].strip()
            gaps.append(gap)
    return gaps


def execute_code(code: str, timeout: int = 30) -> tuple[bool, str]:
    """Execute code in a subprocess. Returns (success, output)."""
    # Prepend mock provider setup
    preamble = textwrap.dedent("""\
        import sys, os
        sys.path.insert(0, os.environ.get("DAFT_DIR", "Daft"))
        from docs.audit.mock_provider import MockProvider
        import daft
        mock = MockProvider()
        daft.attach_provider(mock, alias="openai")
        daft.attach_provider(mock, alias="mock")
        daft.set_provider("mock")
        os.environ["OPENAI_API_KEY"] = "mock-key"
    """)
    full_code = preamble + "\n" + code

    try:
        result = subprocess.run(
            [sys.executable, "-c", full_code],
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "DAFT_DIR": str(DAFT_DIR)},
        )
        if result.returncode == 0:
            return True, result.stdout
        return False, result.stderr
    except subprocess.TimeoutExpired:
        return False, f"Timed out after {timeout}s"


def run_task(client: Anthropic, task: dict) -> dict:
    """Run a single audit task. Returns result dict."""
    print(f"  Running: {task['name']}...")

    context = load_doc_context(task["docs_context"])

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": f"## Documentation\n\n{context}\n\n## Task\n\n{task['prompt']}",
            }
        ],
    )

    response = message.content[0].text
    code = extract_code_block(response)

    if code is None:
        return {
            "id": task["id"],
            "status": "fail",
            "error": "No code block found in response",
            "gaps": [],
            "code": "",
        }

    # Validate syntax
    syntax_err = validate_syntax(code)
    if syntax_err:
        return {
            "id": task["id"],
            "status": "fail",
            "error": syntax_err,
            "gaps": extract_doc_gaps(code),
            "code": code,
        }

    # Validate API names
    api_issues = validate_api_names(code)
    if api_issues:
        return {
            "id": task["id"],
            "status": "fail",
            "error": "Bad API references: " + "; ".join(api_issues),
            "gaps": extract_doc_gaps(code),
            "code": code,
        }

    # Execute if enabled
    if task.get("execute", True):
        success, output = execute_code(code)
        gaps = extract_doc_gaps(code)

        if not success:
            return {
                "id": task["id"],
                "status": "fail",
                "error": output[:500],
                "gaps": gaps,
                "code": code,
            }

        if gaps:
            return {
                "id": task["id"],
                "status": "partial",
                "error": None,
                "gaps": gaps,
                "code": code,
            }

        return {
            "id": task["id"],
            "status": "pass",
            "error": None,
            "gaps": [],
            "code": code,
        }

    # Static validation only
    gaps = extract_doc_gaps(code)
    status = "partial" if gaps else "pass"
    return {
        "id": task["id"],
        "status": status,
        "error": None,
        "gaps": gaps,
        "code": code,
    }


def generate_report(results: list[dict], daft_sha: str) -> str:
    """Generate markdown report from results."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    passes = sum(1 for r in results if r["status"] == "pass")
    partials = sum(1 for r in results if r["status"] == "partial")
    fails = sum(1 for r in results if r["status"] == "fail")
    total = len(results)
    score = round(passes / total * 100) if total else 0

    lines = [
        f"# Docs Audit Report - {now}",
        "",
        f"Daft commit: `{daft_sha}`",
        f"Tasks: {total} | Pass: {passes} | Partial: {partials} | Fail: {fails}",
        f"Score: {score}%",
        "",
    ]

    # Failures
    failures = [r for r in results if r["status"] == "fail"]
    if failures:
        lines.append("## Failures\n")
        for r in failures:
            lines.append(f"### {r['id']} (fail)")
            lines.append(f"**Error:** {r['error']}")
            if r["gaps"]:
                lines.append(f"**Doc gaps:** {'; '.join(r['gaps'])}")
            lines.append(f"\n<details><summary>Generated code</summary>\n")
            lines.append(f"```python\n{r['code']}\n```\n</details>\n")

    # Partial passes
    partials_list = [r for r in results if r["status"] == "partial"]
    if partials_list:
        lines.append("## Partial Passes\n")
        for r in partials_list:
            lines.append(f"### {r['id']} (partial)")
            lines.append(f"**Gaps noted:** {'; '.join(r['gaps'])}\n")

    # Passes
    pass_list = [r for r in results if r["status"] == "pass"]
    if pass_list:
        lines.append("## Passes\n")
        for r in pass_list:
            lines.append(f"- {r['id']}")
        lines.append("")

    return "\n".join(lines)


def main():
    # Get Daft commit SHA
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=DAFT_DIR, text=True
        ).strip()
    except Exception:
        sha = "unknown"

    client = Anthropic()
    tasks = load_tasks()

    print(f"Running {len(tasks)} audit tasks against Daft @ {sha}...")
    results = []
    for task in tasks:
        result = run_task(client, task)
        results.append(result)
        status_icon = {"pass": "OK", "partial": "~~", "fail": "FAIL"}[result["status"]]
        print(f"  [{status_icon}] {task['id']}")

    # Generate and save report
    report = generate_report(results, sha)
    REPORTS_DIR.mkdir(exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    report_path = REPORTS_DIR / f"{now}.md"
    report_path.write_text(report)
    print(f"\nReport saved to {report_path}")

    # Exit with failure if any tasks failed
    if any(r["status"] == "fail" for r in results):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Commit**

```bash
git add run_agent_tasks.py
git commit -m "Add agent task runner

Calls Claude API with docs-only context, validates and executes
generated code against Daft's mock providers, reports pass/fail."
```

---

### Task 11: Add weekly CI workflow to daft-docs-audit

**Files:**
- Create: `daft-docs-audit/.github/workflows/weekly-audit.yml`

- [ ] **Step 1: Create the workflow**

```yaml
name: Weekly Docs Audit
on:
  schedule:
    - cron: '0 6 * * 0'  # Sunday 6am UTC
  workflow_dispatch: {}

jobs:
  agent-audit:
    runs-on: ubuntu-latest
    env:
      python-version: "3.12"
    steps:
      - uses: actions/checkout@v6

      - name: Clone Daft
        run: git clone --depth 1 https://github.com/Eventual-Inc/Daft.git

      - uses: moonrepo/setup-rust@v1
        with:
          cache: false

      - uses: Swatinem/rust-cache@v2
        with:
          shared-key: ${{ runner.os }}-dev-build
          cache-all-crates: "true"
          save-if: false
          workspaces: "Daft"

      - name: Setup Python and uv
        uses: astral-sh/setup-uv@v7
        with:
          python-version: ${{ env.python-version }}
          enable-cache: true

      - name: Install Daft
        run: |
          cd Daft
          uv venv --seed .venv
          source .venv/bin/activate
          uv sync --no-install-project --all-extras --all-groups
          DAFT_DASHBOARD_SKIP_BUILD=1 maturin develop --uv
        env:
          VIRTUAL_ENV: Daft/.venv

      - name: Install runner deps
        run: |
          source Daft/.venv/bin/activate
          uv pip install anthropic pyyaml

      - name: Run agent tasks
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          DAFT_DIR: Daft
          VIRTUAL_ENV: Daft/.venv
        run: |
          source Daft/.venv/bin/activate
          python run_agent_tasks.py

      - name: Commit report
        run: |
          git config user.name "Docs Audit Bot"
          git config user.email "noreply@eventual.ai"
          git add reports/
          git diff --cached --quiet || git commit -m "docs audit: $(date +%Y-%m-%d)"
          git push
```

- [ ] **Step 2: Create README**

```markdown
# daft-docs-audit

Weekly agent-based documentation audit for [Daft](https://github.com/Eventual-Inc/Daft).

Gives Claude hard tasks with only Daft docs as context, executes the generated code
against mock AI providers, and reports pass/fail.

## Setup

1. Add `ANTHROPIC_API_KEY` as a repository secret
2. The weekly workflow runs automatically on Sunday mornings
3. Reports are committed to `reports/`

## Running locally

```bash
git clone https://github.com/Eventual-Inc/Daft.git
pip install anthropic pyyaml
DAFT_DIR=Daft ANTHROPIC_API_KEY=your-key python run_agent_tasks.py
```

## Adding tasks

Edit `tasks.yaml` to add new audit tasks. Each task should exercise
a documentation weak spot that's hard to catch with static analysis.
```

- [ ] **Step 3: Add ANTHROPIC_API_KEY secret to the repo**

```bash
gh secret set ANTHROPIC_API_KEY --repo Eventual-Inc/daft-docs-audit
```

(Enter the API key when prompted.)

- [ ] **Step 4: Commit and push**

```bash
git add .github/workflows/weekly-audit.yml README.md
git commit -m "Add weekly CI workflow and README"
git push
```

---

### Task 12: Run end-to-end validation

**Files:**
- (no new files -- validation run)

- [ ] **Step 1: In the Daft repo, run full Tier 1 audit**

```bash
cd /Users/colinho/Desktop/Daft
make docs-audit
```

All three scripts should exit 0 (with suppressions for intentional gaps).

- [ ] **Step 2: Test pytest-markdown-docs on a subset of pages**

```bash
pytest --markdown-docs docs/quickstart.md docs/custom-code/func.md -v --tb=short 2>&1 | tail -30
```

Review which code blocks pass/fail. Add `<!--pytest-markdown-docs:skip-->` markers above code blocks that intentionally can't run (e.g., they reference external data, models, etc.).

- [ ] **Step 3: In the daft-docs-audit repo, do a dry run**

```bash
cd daft-docs-audit
DAFT_DIR=/Users/colinho/Desktop/Daft ANTHROPIC_API_KEY=$ANTHROPIC_API_KEY python run_agent_tasks.py
```

Review the generated report in `reports/`.

- [ ] **Step 4: Commit any skip markers added to docs**

```bash
cd /Users/colinho/Desktop/Daft
git add docs/
git commit -m "docs: add pytest-markdown-docs skip markers for non-runnable examples"
```
