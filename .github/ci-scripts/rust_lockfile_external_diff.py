#!/usr/bin/env python3
"""Diff external (registry/git) crates between two Cargo.lock files.

Workspace/path packages have no `source` in the lockfile and are ignored.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import tomllib


def external_package_keys(lock_path: Path) -> set[tuple[str, str, str]]:
    raw = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    packages = raw.get("package", [])
    if not isinstance(packages, list):
        return set()
    keys: set[tuple[str, str, str]] = set()
    for pkg in packages:
        if not isinstance(pkg, dict):
            continue
        src = pkg.get("source")
        if not src:
            continue
        name = pkg.get("name")
        version = pkg.get("version")
        if name is None or version is None:
            continue
        keys.add((str(name), str(version), str(src)))
    return keys


def line_for_entry(name: str, ver: str, src: str) -> str:
    return f"- `{name}` {ver} — `{src}`"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True, help="Base branch Cargo.lock")
    parser.add_argument("--head", type=Path, required=True, help="PR head Cargo.lock")
    parser.add_argument(
        "--max-new",
        type=int,
        default=25,
        help="Maximum allowed new external crates vs base (default: 25)",
    )
    parser.add_argument(
        "--markdown-summary",
        action="store_true",
        help="Print markdown report to stdout (e.g. for GITHUB_STEP_SUMMARY)",
    )
    args = parser.parse_args()

    base_keys = external_package_keys(args.base)
    head_keys = external_package_keys(args.head)
    added = sorted(head_keys - base_keys)
    removed = sorted(base_keys - head_keys)
    over = len(added) > args.max_new

    if args.markdown_summary:
        print("## Rust external dependency diff\n")
        print(f"Head: `{args.head}` vs base: `{args.base}`.\n")
        print(f"- **New external crates:** {len(added)}")
        print(f"- **Removed external crates:** {len(removed)}")
        print(f"- **Budget:** at most {args.max_new} new crates allowed.\n")
        if added:
            print("### Added\n")
            for name, ver, src in added:
                print(line_for_entry(name, ver, src))
            print()
        if removed:
            print("### Removed\n")
            for name, ver, src in removed:
                print(line_for_entry(name, ver, src))
            print()
        if over:
            print(
                f"**FAILED:** {len(added)} new crates exceeds limit of {args.max_new}.\n",
            )
        else:
            print("**OK:** within budget.\n")
    else:
        print(f"New external crates: {len(added)}")
        print(f"Removed external crates: {len(removed)}")
        if added:
            print("\nAdded:")
            for name, ver, src in added:
                print(line_for_entry(name, ver, src))
        if removed:
            print("\nRemoved:")
            for name, ver, src in removed:
                print(line_for_entry(name, ver, src))
        if over:
            print(
                f"\nERROR: {len(added)} new crates exceeds limit of {args.max_new}.",
                file=sys.stderr,
            )

    return 1 if over else 0


if __name__ == "__main__":
    raise SystemExit(main())
