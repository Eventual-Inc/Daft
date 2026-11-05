# Schema

Daft can display your DataFrame's schema without materializing it. Under the hood, it performs intelligent sampling of your data to determine the appropriate schema, and if you make any modifications to your DataFrame it can infer the resulting types based on the operation.

::: daft.schema.Schema
    options:
        filters: ["!^_"]

!!! note "Migrating from `daft.logical.schema`"

    `Schema` has moved to `daft.schema`, which is the canonical import. The old import path `daft.logical.schema.Schema` still works as a compatibility alias.
