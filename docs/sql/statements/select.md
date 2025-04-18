# SELECT Statement

The `SELECT` statement is used to query tables in some catalog.

## Examples

Evaluate a single expression.

```sql
SELECT 1 + 1;
```

Select all columns and rows from table `T`.

```sql
SELECT * FROM T;
```

Select columns `a`, `b`, and `c` from table `T`.

```sql
SELECT a, b, c FROM T;
```

Select columns, applying scalar functions _foo_ and _bar_.

```sql
SELECT foo(a), bar(b) FROM T;
```

Count the number of rows whose column  `a` is non-null.

```sql
SELECT COUNT(a) FROM T;
```

Count the number of rows in each group `b`.

```sql
SELECT COUNT(*), b FROM T GROUP BY b;
```

!!! warning "Work in Progress"

    The SQL Reference documents are a work in progress.

<!-- ## Syntax

The basic structure of the `SELECT` statement is as follows,

```
[WITH]
SELECT select_list [FROM from_source]
[WHERE predicate]
```

### SELECT Clause

```mckeeman
select_clause
    'SELECT' select_items
    'SELECT' select_items FROM from_source

select_items
    select_item
    select_item ',' select_items
```
 -->
