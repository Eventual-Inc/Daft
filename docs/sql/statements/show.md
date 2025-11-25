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
| `<pattern>`   | SQL `LIKE` pattern applied to table names.                          |

!!! note "Note"

    Pattern support is currently dependent upon the underlying catalog's list support. For example, Iceberg and S3 Tables list by prefix rather than with a regular expression. Please see issue [#4007](https://github.com/Eventual-Inc/Daft/issues/4007) for better pattern support.

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
