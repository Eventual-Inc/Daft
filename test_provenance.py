#!/usr/bin/env python3
"""Test script for provenance tracking in Project operations."""

from __future__ import annotations

import daft

# Create a dataframe with provenance
data = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
    {"id": 3, "name": "Charlie"},
]

# Create dataframe with provenance function
df = daft.from_pylist(data, provenance_fn=lambda row: f"source1_row_{row['id']}")

print("=== Original DataFrame ===")
print(f"Column names: {df.column_names()}")
print(f"Schema fields: {[f.name for f in df.schema()]}")
print()

# Perform a projection/select operation
df_projected = df.select(["name"])

print("=== After Project (select name only) ===")
print(f"Column names: {df_projected.column_names()}")
print(f"Schema fields: {[f.name for f in df_projected.schema()]}")
print()

# Check if provenance is in the underlying schema
# Note: Once step 2 is implemented, this will be filtered from column_names()
# but we can still check the underlying schema
print("=== Checking for __provenance in schema ===")
schema_field_names = [f.name for f in df_projected.schema()]
has_provenance = "__provenance" in schema_field_names
print(f"__provenance in schema: {has_provenance}")

if has_provenance:
    print("✓ Provenance was automatically forwarded through Project!")
else:
    print("✗ Provenance was NOT forwarded (this is a bug)")

print()
print("=== Full DataFrame Preview ===")
df_projected.show()
