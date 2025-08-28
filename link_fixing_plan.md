# Plan to fix links to core_concepts.md

This document tracks the project to update all links pointing to `core_concepts.md`.

| File | Test Link | Link | New Link | Status |
|---|---|---|---|---|
| `docs/api/udf.md` | http://127.0.0.1:8000/en/stable/api/udf/ | `../core_concepts.md#user-defined-functions-udf` | `../custom-code/udfs.md` | Done ([PR #5022](https://github.com/Eventual-Inc/Daft/pull/5022)) |
| `docs/api/aggregations.md` | http://127.0.0.1:8000/en/stable/api/aggregations/ | `../core_concepts.md#aggregations-and-grouping` | | Done - Line Removed Entirely |
| `docs/api/dataframe.md` | http://127.0.0.1:8000/en/stable/api/dataframe/ | `../core_concepts.md#dataframe` | | Done - Redundant Line Removed |
| `docs/api/datatypes.md` | http://127.0.0.1:8000/en/stable/api/datatypes/ | `../core_concepts.md#datatypes` | | Done - Redundant Line Removed |
| `docs/api/expressions.md` | http://127.0.0.1:8000/en/stable/api/expressions/ | `../core_concepts.md#expressions` | | Done - Redundant Line Removed |
| `docs/api/schema.md` | http://127.0.0.1:8000/en/stable/api/schema/ | `../core_concepts.md#schemas-and-types` | | Done - Redundant Line Removed |
| `docs/api/window.md` | http://127.0.0.1:8000/en/stable/api/window/ | `../core_concepts.md/#window-functions` | `../examples/window-functions.md` | Done - Redirected to Tutorial |
| `docs/migration/dask_migration.md` | http://127.0.0.1:8000/en/stable/migration/dask_migration/ | `../core_concepts.md#expressions` | `../api/expressions.md` | File Removed - Both files were redirected and have been deleted |
| `docs/migration/dask_migration.md` | http://127.0.0.1:8000/en/stable/migration/dask_migration/ | `../core_concepts.md#datatypes` | `../api/datatypes.md` | File Removed - Both files were redirected and have been deleted |
| `docs/migration/dask_migration.md` | http://127.0.0.1:8000/en/stable/migration/dask_migration/ | `../core_concepts.md#user-defined-functions-udf` | `../custom-code/udfs.md` | File Removed - Both files were redirected and have been deleted |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#expressions` (line 292) | `api/expressions.md` | Done - Link Updated |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#dataframe` (line 518) | | Done - Entire Section Removed |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#expressions` (line 519) | | Done - Entire Section Removed |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#reading-data` (line 520) | | Done - Entire Section Removed |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#reading-data` (line 521) | | Done - Entire Section Removed |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#datatypes` (line 522) | | Done - Entire Section Removed |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#sql` (line 523) | | Done - Entire Section Removed |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#aggregations-and-grouping` (line 524) | | Done - Entire Section Removed |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#window-functions` (line 525) | | Done - Entire Section Removed |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#user-defined-functions-udf` (line 526) | | Done - Entire Section Removed |
| `docs/quickstart.md` | http://127.0.0.1:8000/en/stable/quickstart/ | `core_concepts.md#multimodal-data` (line 527) | | Done - Entire Section Removed |
