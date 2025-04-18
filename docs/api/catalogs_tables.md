# Catalogs and Tables

Daft integrates with various catalog implementations using its `Catalog` and `Table` interfaces. These are high-level APIs to manage catalog objects (tables and namespaces), while also making it easy to leverage Daft's existing `daft.read_` and `df.write_` APIs for open table formats like [Iceberg](../integrations/iceberg.md) and [Delta Lake](../integrations/delta_lake.md). Learn more about [Catalogs & Tables](../catalogs.md) in Daft User Guide.

::: daft.catalog.Catalog
    options:
        filters: ["!^_"]

::: daft.catalog.Identifier
    options:
        filters: ["!^_"]

::: daft.catalog.Table
    options:
        filters: ["!^_"]
