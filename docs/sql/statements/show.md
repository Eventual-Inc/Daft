# SHOW Statement

The `SHOW` statement is used to list tables in a catalog.

## Syntax

```sql
SHOW TABLES [ {FROM|IN} <catalog> ] [ LIKE <pattern> ]
```

| Parameter   | Description                                   |
|-------------|-----------------------------------------------|
| `<catalog>` | `catalog` name                                |
| `<pattern>` | `pattern` string to match e.g. a table prefix |

!!! note "Note"

    Pattern support is currently dependent upon the underlying catalog's list support. For example, Iceberg and S3 Tables list by prefix rather than with a regular expression. Please see issue [#4007](https://github.com/Eventual-Inc/Daft/issues/4007) for better pattern support.

## Examples

Show tables in the current catalog.

```sql
SHOW TABLES;
```

Show tables in the current catalog matching the pattern.

```sql
SHOW TABLES LIKE 'foo'
```

Show tables in catalog `my_catalog`.

```sql
SHOW TABLES IN my_catalog;
```

Show tables in catalog `my_catalog` matching the pattern.

```sql
SHOW TABLES IN my_catalog LIKE 'foo';
```
