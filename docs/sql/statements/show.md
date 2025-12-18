# SHOW Statement

The `SHOW` statement lists catalog tables and supports filtering by namespace and SQL `LIKE` patterns.

## Syntax

```sql
SHOW TABLES [ {FROM|IN} <catalog>[.<namespace>] ] [ LIKE <pattern> ]
```

| Parameter     | Description                                                         |
| ------------- | ------------------------------------------------------------------- |
| `<catalog>`   | Catalog name to query. If omitted, the current catalog must be set. |
| `<namespace>` | Optional namespace inside the catalog.                              |
| `<pattern>`   | Pattern to match table names. Pattern syntax is catalog-dependent. Native/memory catalogs and Postgres support SQL `LIKE` syntax (`%`, `_`, `\`). Other catalogs like Iceberg, S3 Tables, and Glue use different pattern matching (e.g., prefix matching, catalog-specific expressions). |

!!! note "Note"

    Pattern support is catalog-dependent. Native/memory catalogs and Postgres support SQL `LIKE` syntax (`%`, `_`, `\`). Other catalogs like Iceberg and S3 Tables use prefix matching, while Glue uses AWS Glue expression syntax. Please see issue [#4007](https://github.com/Eventual-Inc/Daft/issues/4007) for better pattern support.

## Examples

Show tables in the current catalog.

```sql
SHOW TABLES;
```

Show tables in the current catalog matching the pattern.

```sql
SHOW TABLES LIKE 'foo%'
```

Show tables in catalog `my_catalog` and schema `public`.

```sql
SHOW TABLES IN my_catalog.public;
```

Show tables in catalog `my_catalog` under `public` matching the pattern.

```sql
SHOW TABLES IN my_catalog.public LIKE 'foo%';
```
