# Schema

Daft can display your DataFrame's schema without materializing it. Under the hood, it performs intelligent sampling of your data to determine the appropriate schema, and if you make any modifications to your DataFrame it can infer the resulting types based on the operation. Learn more about [Schemas](../core_concepts.md#schemas-and-types) in Daft User Guide.

::: daft.logical.schema.Schema
    options:
        filters: ["!^_"]
