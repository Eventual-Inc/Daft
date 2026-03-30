# Fault Tolerance for File Reads

When reading large collections of files, some files may be unreadable — corrupt, truncated, or deleted between the time Daft lists them and the time it reads them. By default, Daft raises an error and halts the query. The `ignore_corrupt_files` option changes that behavior: qualifying files are silently skipped and the query continues with the remaining data.

## Enabling `ignore_corrupt_files`

Pass `ignore_corrupt_files=True` to any of the supported reader functions:

```python
import daft

# Parquet / CSV (glob-based)
df = daft.read_parquet("s3://my-bucket/data/**/*.parquet", ignore_corrupt_files=True)
df = daft.read_csv("s3://my-bucket/data/**/*.csv", ignore_corrupt_files=True)

# Iceberg
import pyiceberg
table = pyiceberg.table.StaticTable.from_metadata("s3://bucket/iceberg/metadata.json")
df = daft.read_iceberg(table, ignore_corrupt_files=True)

# Lance
df = daft.read_lance("s3://my-bucket/data.lance", ignore_corrupt_files=True)

df.collect()
```

## What counts as "corrupt"

Daft skips a file when it encounters a problem that is specific to the file itself and cannot be resolved by retrying:

| Category | Examples |
|---|---|
| **Invalid format** | Bad Parquet magic bytes, truncated footer, mismatched row/column counts |
| **Corrupt data** | Unreadable row group, invalid CSV encoding, wrong field count in a row |
| **Missing file** | File deleted between listing and reading (e.g. concurrent compaction or partition overwrite) |

Daft does **not** skip files for transient infrastructure problems, because those can and should be retried:

| Category | Examples |
|---|---|
| **Network errors** | Connection reset, read timeout, throttled I/O |
| **Permission errors** | Access denied, insufficient credentials |

This distinction matters. Silently retrying a permission error would mask a misconfiguration that needs human attention.

## Observability: knowing what was skipped

`ignore_corrupt_files` is designed around the principle that **errors should be visible, not hidden**. Daft provides two complementary observability mechanisms.

### Python warning logs

Daft emits a `WARNING`-level log message for every skipped file, including the file path and the reason:

```
WARNING daft.io - Skipping corrupt Parquet file s3://my-bucket/data/bad.parquet: ...
WARNING daft.io - Skipping corrupt CSV chunk in s3://my-bucket/data/partial.csv: ...
```

You can see these with standard Python logging:

```python
import logging
logging.basicConfig(level=logging.WARNING)
```

### `df.skipped_files` — programmatic access

After calling `.collect()`, the `skipped_files` property returns the list of skipped `(path, reason)` pairs as structured data, so your pipeline code can act on them:

```python
df = daft.read_parquet("s3://my-bucket/data/**/*.parquet", ignore_corrupt_files=True)
df.collect()

skipped = df.skipped_files  # list[tuple[str, str]]
for path, reason in skipped:
    print(f"Skipped: {path}\n  Reason: {reason}")
```

`skipped_files` is available after any action that triggers execution (`.collect()`, `.write_parquet()`, etc.).

## Handling skipped files in production

Because `skipped_files` is plain Python data, you can plug it directly into your existing alerting or data-quality workflows:

```python
import daft

df = daft.read_parquet("s3://my-bucket/nightly/**/*.parquet", ignore_corrupt_files=True)
df.write_parquet("s3://my-bucket/processed/")

skipped = df.skipped_files
if skipped:
    # Option 1: send an alert
    send_alert(f"{len(skipped)} file(s) skipped during nightly run", details=skipped)

    # Option 2: push to a dead-letter queue for later reprocessing
    for path, reason in skipped:
        dead_letter_queue.put({"path": path, "reason": reason, "run": TODAY})
```

This pattern — **errors visible, impact contained, tooling to fix** — lets automated batch jobs complete reliably while still surfacing problems for human review.

!!! warning "Do not use `ignore_corrupt_files` as a catch-all"
    This option is designed for files that are genuinely unreadable. It should not be used to suppress transient I/O errors (network issues, throttling) — Daft already retries those automatically. If you find yourself needing `ignore_corrupt_files` for a large fraction of your files, investigate the root cause rather than silencing the errors.

## Supported formats

| Format | File-level skip | Within-file error skip | `skipped_files` reported |
|---|---|---|---|
| Parquet (`read_parquet`) | ✅ (bad footer, wrong magic bytes, file too small) | ✅ (corrupt row group data) | ✅ |
| CSV (`read_csv`) | ✅ (unreadable file, truncated) | ✅ (bad encoding, wrong field count in chunk) | ✅ |
| Iceberg (`read_iceberg`) | ✅ (same as Parquet — data files go through the Rust Parquet reader) | ✅ | ✅ |
| Lance (`read_lance`) | ✅ (corrupt fragment skipped, scan continues with remaining fragments) | ✅ | ❌ (warning logged instead; Lance reads run in Python) |

!!! note "Iceberg delete files"
    Corruption in Iceberg *delete files* is not covered. If a delete file is unreadable, Daft will raise an error regardless of `ignore_corrupt_files`. Delete files are small metadata structures and corruption there generally indicates a more serious catalog inconsistency.

!!! note "Paimon LSM-merge fallback"
    For Apache Paimon primary-key tables whose splits require LSM-tree merging, Daft delegates to pypaimon's native reader. `ignore_corrupt_files` applies only to the native Parquet path (append-only tables and single-file PK splits).
