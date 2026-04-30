#!/usr/bin/env python3
"""Update downstream Cargo.toml files to replace common-* deps with daft-common."""

import os
import re
import glob

# Common crates being merged (exclude common-macros, common-image, common-arrow-ffi)
MERGED_CRATES = {
    "common-error", "common-display", "common-daft-config", "common-io-config",
    "common-py-serde", "common-runtime", "common-file-formats", "common-treenode",
    "common-partitioning", "common-resource-request", "common-metrics",
    "common-system-info", "common-tracing", "common-version", "common-logging",
    "common-hashable-float-wrapper", "common-ndarray", "common-pattern",
    "common-checkpoint-config",
}

def process_cargo_toml(path):
    with open(path) as f:
        content = f.read()

    # Check if this file references any merged crates
    has_merged = any(c in content for c in MERGED_CRATES)
    if not has_merged:
        return False

    lines = content.split('\n')
    new_lines = []
    added_daft_common = False
    in_features = False
    has_python_feature_propagation = False
    removed_feature_lines = []

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Track if we're in [features] section
        if stripped.startswith('[features]'):
            in_features = True
        elif stripped.startswith('[') and not stripped.startswith('[features'):
            in_features = False

        # Remove common-* dependency lines
        is_merged_dep = False
        for crate in MERGED_CRATES:
            # Match patterns like: common-error = {path = "...", ...}
            # or: common-error = {workspace = true}
            # or: common-error.workspace = true
            if stripped.startswith(f'{crate} =') or stripped.startswith(f'{crate}.'):
                is_merged_dep = True
                break

        if is_merged_dep:
            # Add daft-common dep once (right where first common-* dep was)
            if not added_daft_common:
                new_lines.append('daft-common = {workspace = true}')
                added_daft_common = True
            i += 1
            continue

        # Handle feature propagation lines like "common-error/python"
        if in_features:
            # Check for common-*/python or common-*/feature patterns
            merged_feature_pattern = '|'.join(re.escape(c) for c in MERGED_CRATES)
            if re.search(f'"{merged_feature_pattern}/', stripped):
                # Check if it's a python feature
                if '/python' in stripped:
                    has_python_feature_propagation = True
                removed_feature_lines.append(stripped)
                i += 1
                continue
            # Also handle: "dep:common-py-serde" or "dep:common-error"
            for crate in MERGED_CRATES:
                dep_pattern = f'"dep:{crate}"'
                if dep_pattern in stripped:
                    removed_feature_lines.append(stripped)
                    is_merged_dep = True
                    break
            if is_merged_dep:
                i += 1
                continue

        new_lines.append(line)
        i += 1

    # If we removed feature propagation for python, add "daft-common/python" once
    if has_python_feature_propagation:
        # Find the python feature line and add daft-common/python
        final_lines = []
        added_dc_python = False
        for line in new_lines:
            final_lines.append(line)
            if not added_dc_python and 'python = [' in line:
                # Add after the python = [ line
                final_lines.append('  "daft-common/python",')
                added_dc_python = True
        new_lines = final_lines

    result = '\n'.join(new_lines)

    # Clean up: if daft-common = {workspace = true} already exists, remove duplicates
    # Also ensure no double blank lines

    with open(path, 'w') as f:
        f.write(result)

    return True


# Find all downstream Cargo.toml files
cargo_files = glob.glob('src/*/Cargo.toml')
# Exclude common/* and daft-common
cargo_files = [f for f in cargo_files if '/common/' not in f and '/daft-common/' not in f]

updated = 0
for path in sorted(cargo_files):
    if process_cargo_toml(path):
        print(f"Updated: {path}")
        updated += 1

print(f"\nTotal updated: {updated}")
