# SET Statement

The `SET` statement changes session variables.

## Examples

Set identifier resolution to case-insensitive matching.

```sql
SET identifier_mode = 'insensitive';
```

Set identifier resolution to the Postgres-style normalize mode. `normalized` is accepted as an alias for `normalize`.

```sql
SET identifier_mode = 'normalize';
```

Set the current catalog.

```sql
SET catalog = my_catalog;
```

Clear the current catalog.

```sql
SET catalog = NULL;
```

Session options may be qualified with `daft.` and quoted as identifiers:

```sql
SET daft.identifier_mode = 'sensitive';
SET "catalog" = 'my_catalog';
```

## Supported Options

| Option | Values | Description |
| --- | --- | --- |
| `identifier_mode` | `sensitive` (default), `insensitive`, `normalize` | Controls how SQL identifiers are resolved. See [Identifiers](../identifiers.md). |
| `catalog` | catalog name, or `NULL` | Sets or clears the session's current catalog. |

Both options also accept a `daft.` prefix, for example `SET daft.catalog = my_catalog`.

## Rules

1. Option names are matched case-insensitively on each identifier part. Quoted names with different spelling (such as `"identifier _mode"`) are not rewritten into a supported option.
2. If `identifier_mode` is given an unknown value, this raises an error.
3. If `catalog` is set to a name that is not attached, this raises an error.
4. `SET catalog = NULL` clears `current_catalog`.

## Syntax

```mkeenan
set_statement
    'SET' option_name [ 'TO' | '=' ] option_value

option_name
    ident
    'daft' '.' ident

option_value
    string
    ident
    'NULL'
```

!!! warning "Work in Progress"

    The SQL Reference documents are a work in progress.
