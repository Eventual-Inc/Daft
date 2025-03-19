# USE Statement

The `USE` statement sets the current catalog and namespace.

## Examples

Set the session's `current_catalog` to `my_catalog`.

```sql
USE my_catalog;
```

Set the session's `current_catalog` to `my_catalog`, and `current_namespace` to `my_namespace`.

```sql
USE my_catalog.my_namespace;
```

## Rules

1. If the catalog does not exist, this will raise an error.
2. If no namespace is given, then `current_namespace` is set to `NULL`.

## Syntax

```mkeenan
use_statement
    'USE' catalog_ident
    'USE' catalog_ident '.' namespace_ident

catalog_ident
    simple_ident

namespace_ident
    ident
```

!!! warning "Work in Progress"

    The SQL Reference documents are a work in progress.
