# Reading from MongoDB

!!! warning "Experimental"

    This connector is experimental and the API may change in future releases.

Daft can read MongoDB collections into DataFrames with
[`daft.read_mongodb()`][daft.io.read_mongodb].

The MongoDB connector is designed for operational databases: it requires an
explicit schema, does not sample the collection during planning, and only
creates distributed read tasks when you provide partition ranges.

## Basic Usage

=== "🐍 Python"

    ```python
    import daft

    schema = {
        "_id": daft.DataType.string(),
        "tenant_id": daft.DataType.string(),
        "created_at": daft.DataType.timestamp("ms"),
        "amount": daft.DataType.float64(),
    }

    df = daft.read_mongodb(
        uri="mongodb://localhost:27017",
        database="app",
        collection="orders",
        schema=schema,
    )
    ```

Daft converts BSON values into the provided schema at execution time. Missing
fields and BSON null values are read as nulls.

## Predicate and Projection Pushdown

Pass a MongoDB filter document to reduce the data scanned by the server. Daft
also attempts to push supported [`where`][daft.DataFrame.where] predicates and
[`select`][daft.DataFrame.select] projections into the MongoDB `find` operation.
Unsupported Daft predicates should be left above the scan by enabling strict
filter pushdown with
`daft.context.set_planning_config(enable_strict_filter_pushdown=True)`.

=== "🐍 Python"

    ```python
    df = daft.read_mongodb(
        uri="mongodb://localhost:27017",
        database="app",
        collection="orders",
        schema=schema,
        filter={"status": "paid"},
        projection={"_id": 1, "tenant_id": 1, "created_at": 1, "amount": 1},
        hint={"status": 1, "created_at": 1},
        max_time_ms=30_000,
    )

    result = df.where(daft.col("amount") >= 100).select("tenant_id", "amount")
    ```

Use MongoDB Extended JSON for values that are not plain JSON, such as ObjectIds.
The connector supports `$oid`, `$numberLong`, `$numberInt`, `$numberDouble`,
`$date` with integer milliseconds, and canonical `$binary` values:

=== "🐍 Python"

    ```python
    df = daft.read_mongodb(
        uri="mongodb://localhost:27017",
        database="app",
        collection="orders",
        schema=schema,
        filter={"_id": {"$oid": "000000000000000000000000"}},
    )
    ```

If the Daft schema does not include `_id`, Daft excludes MongoDB's default `_id`
field from the server projection unless your `projection` explicitly includes it.
Projection documents follow MongoDB's normal rule that simple inclusion and
exclusion fields cannot be mixed, except for `_id`. Inclusion-style projections
are best for minimizing network transfer because Daft can add selected columns to
the MongoDB projection; exclusion-style projections are respected as written.

## Distributed Reads

To read in parallel or distribute reads across Ray workers, provide a
`partition_field` and explicit half-open `partition_ranges`. Daft creates one
MongoDB scan task per range and combines the range with the user and pushed-down
filters.

=== "🐍 Python"

    ```python
    import datetime

    utc = datetime.timezone.utc

    df = daft.read_mongodb(
        uri="mongodb://localhost:27017",
        database="app",
        collection="orders",
        schema=schema,
        filter={"status": "paid"},
        partition_field="created_at",
        partition_ranges=[
            (datetime.datetime(2024, 1, 1, tzinfo=utc), datetime.datetime(2024, 2, 1, tzinfo=utc)),
            (datetime.datetime(2024, 2, 1, tzinfo=utc), datetime.datetime(2024, 3, 1, tzinfo=utc)),
            (datetime.datetime(2024, 3, 1, tzinfo=utc), datetime.datetime(2024, 4, 1, tzinfo=utc)),
        ],
        hint={"created_at": 1},
        batch_size=1000,
        max_time_ms=30_000,
    )
    ```

For operational clusters, partition on an indexed field and size ranges so each
task performs a bounded index scan. If `partition_ranges` is omitted, Daft
creates a single scan task and does not issue a metadata query to infer ranges.
The `partition_field` can be omitted from the Daft schema when it is only needed
to bound the MongoDB queries and should not be returned in the DataFrame.

## Operational Safety

- Provide `schema`; the connector does not infer schema from collection contents.
- Use MongoDB URI options such as `readPreference=secondaryPreferred` when
  reads should avoid the primary.
- Prefer indexed `filter` and `partition_field` values.
- Pass `hint` when you need MongoDB to use a specific index.
- Use `max_time_ms` to bound each server-side `find`.
- Tune `batch_size` to control cursor batch size.
- Avoid unbounded reads from production collections unless that is intentional.
