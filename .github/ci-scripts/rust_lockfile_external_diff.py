#!/usr/bin/env python3
"""Diff external (registry/git) crates between two Cargo.lock files.

Workspace/path packages have no `source` in the lockfile and are ignored.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import tomllib


def external_package_keys(lock_path: Path) -> set[tuple[str, str]]:
    raw = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    packages = raw.get("package", [])
    if not isinstance(packages, list):
        return set()
    keys: set[tuple[str, str]] = set()
    for pkg in packages:
        if not isinstance(pkg, dict):
            continue
        name = pkg.get("name")
        version = pkg.get("version")
        if name is None or version is None:
            continue
        keys.add((str(name), str(version)))
    return keys


def line_for_entry(name: str, ver: str) -> str:
    return f"- `{name}`: {ver}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=str, required=True, help="Base branch SHA")
    parser.add_argument("--head", type=str, required=True, help="PR head SHA")
    parser.add_argument("--base-lockfile", type=Path, required=True, help="Base branch Cargo.lock")
    parser.add_argument("--head-lockfile", type=Path, required=True, help="PR head Cargo.lock")
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

    base_keys = external_package_keys(args.base_lockfile)
    head_keys = external_package_keys(args.head_lockfile)
    added = sorted(head_keys - base_keys)
    removed = sorted(base_keys - head_keys)
    over = (len(added) - len(removed)) > args.max_new

    if args.markdown_summary:
        print("## Rust Dependency Diff\n")
        print(f"Head: `{args.head}` vs Base: `{args.base}`.\n")
        print(f"- **New Crates:** {len(added)}")
        print(f"- **Removed Crates:** {len(removed)}")
        if added:
            print("### Added\n")
            for name, ver in added:
                print(line_for_entry(name, ver))
            print()
        if removed:
            print("### Removed\n")
            for name, ver in removed:
                print(line_for_entry(name, ver))
            print()

        print()
        if over:
            print(
                f"**FAILED:** {len(added) - len(removed)} new crates exceeds limit of {args.max_new}.\n",
            )
        else:
            print("**OK:** Within budget.\n")
    else:
        print(f"New Crates: {len(added)}")
        print(f"Removed Crates: {len(removed)}")
        if added:
            print("\nAdded:")
            for name, ver in added:
                print(line_for_entry(name, ver))
        if removed:
            print("\nRemoved:")
            for name, ver in removed:
                print(line_for_entry(name, ver))
        if over:
            print(
                f"\nERROR: {len(added) - len(removed)} new crates exceeds limit of {args.max_new}.",
                file=sys.stderr,
            )

    return 1 if over else 0


if __name__ == "__main__":
    exit(main())
