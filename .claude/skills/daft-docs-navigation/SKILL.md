---
name: "daft-docs-navigation"
description: "Navigate Daft documentation. Invoke when user asks general questions about APIs, concepts, or examples, or wants to search the docs."
---

# Daft Docs Navigation

Navigate Daft's documentation for APIs, concepts, and examples.

## Documentation Structure

1.  **Live Site**: [`https://docs.daft.ai`](https://docs.daft.ai) (Primary source, searchable).
2.  **Source Repo**: `docs/` directory in the repository.
    -   If `docs/` is missing or empty, clone the repo: `git clone https://github.com/Eventual-Inc/Daft.git`

## Key Locations in `docs/`

-   **API Reference**: `api/` (e.g., `api/io.md` for reading/writing).
-   **Optimization**: `optimization/` (e.g., `optimization/partitioning.md`).
-   **Distributed**: `distributed/` (e.g., `distributed/ray.md`).
-   **UDFs**: `custom-code/` (e.g., `func.md`, `cls.md`).
-   **Connectors**: `connectors/` (e.g., S3, Lance).
-   **Table of Contents**: `docs/SUMMARY.md`.

## Usage

**Search for API Usage:**
```bash
grep_search(pattern='from_glob', path='docs/')
```

**Browse Topics:**
```bash
read(file_path='docs/SUMMARY.md')
```
