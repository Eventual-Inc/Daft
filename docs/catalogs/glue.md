# Glue

!!! warning "Warning"

    These APIs are early in their development. Please feel free to [open feature requests and file issues](https://github.com/Eventual-Inc/Daft/issues/new/choose). We'd love hear want you would like, thank you! 🤘

Daft integrates with [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html) using the [Daft Catalog](index.md) interface. Daft provides builtin support for a handful of tables (details below), but you may provide your own `GlueTable` implementations and register them to your `GlueCatalog` instance. Note that in Daft, the AWS Glue service-level *Data Catalog* maps to a Daft *Catalog* and a Glue *Database* is a *Namespace* — this can be confusing and is important to remember.

| Daft      | Glue         |
|-----------|--------------|
| Catalog   | Data Catalog |
| Namespace | Database     |
| Table     | Table        |

## Example

=== "🐍 Python"

    ```python
    from daft.catalog.__glue import load_glue

    # Load a glue catalog instance
    catalog = load_glue(
        name="my_glue_catalog",
        region_name="us-west-2"
    )

    # Load a glue table
    tbl = catalog.get_table("my_namespace.my_table")

    # Read the table as a DataFrame
    df = tbl.read()
    df.show()
    ```

## Support

Glue supports many different table [classifications](https://docs.aws.amazon.com/glue/latest/dg/add-classifier.html#classifier-built-in) along with various table formats like [Iceberg](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html), [Delta Lake](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html), and [Hudi](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-hudi.html).

### Formats

Daft has preliminary read support for the CSV and Parquet formats, but does not yet support reading with Hive-style partitioning. Daft does support
reading and writing both Iceberg and Delta Lake. We do not currently support creating Glue tables.

| Table Format | Support     | AWS Documentation                                                                                             |
|--------------|-------------|---------------------------------------------------------------------------------------------------------------|
| CSV          | read        | [Documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-csv-home.html)     |
| Parquet      | read        | [Documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-parquet-home.html) |
| Iceberg      | read, write | [Documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)      |
| Delta Lake   | read, write | [Documentation](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html)   |


## Type System

The [Glue Catalog Type System](https://docs.aws.amazon.com/glue/latest/dg/glue-types.html#glue-types-catalog) is based on the [Apache Hive Type System](https://cwiki.apache.org/confluence/display/hive/languagemanual+types); however, the exact types are not validated and are dependent upon the underlying table format.

> The Data Catalog does not validate types written to type fields. When AWS Glue components read and write to the Data Catalog, they will be compatible with each other. AWS Glue components also aim to preserve a high degree of compatibility with the Hive types. However, AWS Glue components do not guarantee compatibility with all Hive types.

| Table Format | Type System                                                                                                                              |
|--------------|------------------------------------------------------------------------------------------------------------------------------------------|
| CSV          | [Glue CSV Type Reference](https://docs.aws.amazon.com/glue/latest/webapi/API_CsvClassifier.html#Glue-Type-CsvClassifier-CustomDatatypes) |
| Parquet      | [Arrow Type System Reference](https://arrow.apache.org/docs/python/api/datatypes.html)                                                   |
| Iceberg      | [Iceberg Type System Reference](../io/iceberg.md#type-system)                                                                                |
| Delta Lake   | [Delta Lake Type System Reference](../io/delta_lake.md#type-system)                                                                          |


## Custom Table Implementation

!!! warning "Warning"

    This is not considered a stable API, it is just a patch technique!

To implement a custom table format, you must implement the `GlueTable` abstract class and register to your `GlueCatalog` instance. To register,
you must append your custom table's class to the catalog's `_table_impls` field.

The Daft `GlueCatalog._table_impls` field holds a list of `GlueTable` implementation classes. When we resolve a table, we call Glue's `GetTable` API and call `from_table_info` with the [Glue Table object](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-tables.html#aws-glue-api-catalog-tables-Table). It is expected that each `GlueTable` implementation will throw a `ValueError` if the table metadata does not match.

```python
from daft.catalog.__glue import GlueCatalog, GlueTable, load_glue

class GlueTestTable(GlueTable):
    """GlueTestTable shows how we register custom table implementations."""

    @classmethod
    def from_table_info(cls, catalog: GlueCatalog, table: dict[str,Any]) -> GlueTable:
        if bool(table["Parameters"].get("pytest")):
            return cls(catalog, table)
        raise ValueError("Expected Parameter pytest='True'")


    def read(self, **options) -> DataFrame:
        raise NotImplementedError

    def write(self, df: DataFrame, mode: Literal['append'] | Literal['overwrite'] = "append", **options) -> None:
        raise NotImplementedError

gc = load_glue("my_glue_catalog", region="us-west-2")
gc._table_impls.append(GlueTestTable) # !! REGISTER GLUE TEST TABLE !!
```
