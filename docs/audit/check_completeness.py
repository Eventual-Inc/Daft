#!/usr/bin/env python3
"""Verify structural completeness of the documentation."""

import re
import sys
from pathlib import Path

DOCS_DIR = Path(__file__).parent.parent
SKIP_DIRS = {"superpowers", "audit", "gen_pages", "templates", "overrides", "plugins", "css", "js", "img"}
SKIP_FILES = {"SUMMARY.md", "robots.txt"}
CONNECTOR_SKIP = {"index.md", "custom.md", "custom-catalogs.md"}
STALE_PHRASES = [
    r"not yet supported",
    r"on the roadmap",
    r"does not yet (?:have|support)",
    r"will be added in a future",
]


def iter_doc_files() -> list[Path]:
    return [
        p for p in DOCS_DIR.rglob("*.md") if not any(part in SKIP_DIRS for part in p.parts) and p.name not in SKIP_FILES
    ]


def strip_code_blocks(text: str) -> str:
    return re.sub(r"```.*?```", "", text, flags=re.DOTALL)


def strip_content(text: str) -> str:
    text = re.sub(r"^---\s*\n.*?\n---\s*\n", "", text, flags=re.DOTALL)  # frontmatter
    text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)  # html comments
    text = strip_code_blocks(text)
    text = re.sub(r"^:::.*$", "", text, flags=re.MULTILINE)  # mkdocstrings
    return text


def check_nav_coverage(files: list[Path]) -> list[str]:
    summary = (DOCS_DIR / "SUMMARY.md").read_text(errors="replace")
    # Strip HTML comments in SUMMARY before parsing links (commented-out nav)
    summary_no_comments = re.sub(r"<!--.*?-->", "", summary, flags=re.DOTALL)
    nav_refs = set(re.findall(r"\(([^)]+\.md)\)", summary_no_comments))

    issues = []
    for path in files:
        rel = str(path.relative_to(DOCS_DIR))
        if rel not in nav_refs:
            issues.append(f"NAV_MISSING: {rel} - not referenced in SUMMARY.md")
    return issues


def check_placeholder_pages(files: list[Path]) -> list[str]:
    issues = []
    for path in files:
        text = path.read_text(errors="replace")
        clean = strip_content(text)
        words = clean.split()
        if len(words) < 50:
            rel = path.relative_to(DOCS_DIR)
            issues.append(f"PLACEHOLDER: {rel} - only {len(words)} words of real content")
    return issues


def check_hidden_content(files: list[Path]) -> list[str]:
    issues = []
    for path in files:
        text = path.read_text(errors="replace")
        rel = path.relative_to(DOCS_DIR)
        for m in re.finditer(r"<!--(.*?)-->", text, flags=re.DOTALL):
            if len(m.group(0)) > 200:
                lineno = text[: m.start()].count("\n") + 1
                preview = m.group(1).strip()[:80].replace("\n", " ")
                issues.append(f"HIDDEN: {rel}:{lineno} - {preview!r}")
    return issues


def check_stale_claims(files: list[Path]) -> list[str]:
    pattern = re.compile("|".join(STALE_PHRASES), re.IGNORECASE)
    issues = []
    for path in files:
        text = path.read_text(errors="replace")
        rel = path.relative_to(DOCS_DIR)
        prose = strip_code_blocks(text)
        for i, line in enumerate(prose.splitlines(), 1):
            if pattern.search(line):
                issues.append(f"STALE: {rel}:{i} - {line.strip()[:100]}")
    return issues


def check_connector_index(files: list[Path]) -> list[str]:
    index_path = DOCS_DIR / "connectors" / "index.md"
    if not index_path.exists():
        return ["CONNECTOR_INDEX: connectors/index.md not found"]
    index_text = index_path.read_text(errors="replace")
    issues = []
    for path in files:
        if path.parent.name != "connectors" or path.name in CONNECTOR_SKIP:
            continue
        stem = path.stem
        if stem not in index_text:
            rel = path.relative_to(DOCS_DIR)
            issues.append(f"CONNECTOR_INDEX: {rel} - stem '{stem}' not found in connectors/index.md")
    return issues


def main():
    files = iter_doc_files()

    nav_issues = check_nav_coverage(files)
    placeholder_issues = check_placeholder_pages(files)
    hidden_issues = check_hidden_content(files)
    stale_issues = check_stale_claims(files)
    connector_issues = check_connector_index(files)

    sections = [
        ("Nav Coverage", nav_issues),
        ("Placeholder Pages", placeholder_issues),
        ("Hidden Content", hidden_issues),
        ("Connector Index", connector_issues),
        ("Stale Claims (warnings)", stale_issues),
    ]

    total_errors = 0
    for label, issues in sections:
        if not issues:
            print(f"OK: {label}")
            continue
        print(f"\n=== {label} ({len(issues)}) ===")
        for issue in sorted(issues):
            print(issue)
        if label != "Stale Claims (warnings)":
            total_errors += len(issues)

    if total_errors:
        print(f"\nFAIL: {total_errors} non-stale issue(s) found.")
        sys.exit(1)
    else:
        print("\nPASS: No blocking issues found.")


if __name__ == "__main__":
    main()
