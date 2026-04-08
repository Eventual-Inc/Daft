# DESCRIBE Statement

The `DESCRIBE` statement shows the schema of a table or the execution plan of a query.

## DESCRIBE TABLE

Shows the schema (column names and data types) of a table.

### Syntax

```sql
DESCRIBE [TABLE] <table_name>
```

| Parameter      | Description                                                      |
| -------------- | ---------------------------------------------------------------- |
| `<table_name>` | Name of the table to describe. Can include catalog and namespace. |

### Examples

Describe the schema of table `T`.

```sql
DESCRIBE T;
```

Describe the schema with the optional `TABLE` keyword.

```sql
DESCRIBE TABLE T;
```

Describe a table in a specific catalog and namespace.

```sql
DESCRIBE my_catalog.my_namespace.my_table;
```

### Output

Returns a DataFrame with columns:

| Column        | Description                |
| ------------- | -------------------------- |
| `column_name` | Name of the column         |
| `type`        | Data type of the column    |

## DESCRIBE SELECT

Shows the query execution plan for a SELECT statement.

### Syntax

```sql
DESCRIBE <select_statement>
```

| Parameter            | Description                           |
| -------------------- | ------------------------------------- |
| `<select_statement>` | The SELECT statement to describe.     |

### Examples

Describe the execution plan for a simple query.

```sql
DESCRIBE SELECT * FROM T;
```

Describe the execution plan for a query with joins.

```sql
DESCRIBE SELECT a.*, b.name FROM T a JOIN S b ON a.id = b.id;
```

## Unsupported Features

The following features are not currently supported:

- `EXPLAIN` - Use `DESCRIBE` instead
- `DESCRIBE ANALYZE` - Performance analysis is not yet implemented
- `DESCRIBE VERBOSE` - Verbose output is not yet implemented
- `DESCRIBE FORMAT` - Custom output formats are not yet implemented

!!! warning "Work in Progress"

    The SQL Reference documents are a work in progress.
