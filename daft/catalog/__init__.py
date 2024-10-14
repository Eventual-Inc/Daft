"""The `daft.catalog` module contains functionality for Data Catalogs.

A Data Catalog can be understood as a system/service for users to discover, access and query their data.
Most commonly, users' data is represented as a "table". Some more modern Data Catalogs such as Unity Catalog
also expose other types of data including files, ML models, registered functions and more.

Examples of Data Catalogs include AWS Glue, Hive Metastore, Apache Iceberg REST and Unity Catalog.

Daft manages Data Catalogs by registering them in an internal meta-catalog, called the "DaftMetaCatalog". This
is simple a collection of data catalogs, which Daft will attempt to detect from a users' current environment.

**Data Catalog**

Daft recognizes a default catalog which it will attempt to use when no specific catalog name is provided.

```python
# This will hit the default catalog
daft.read_table("my_db.my_namespace.my_table")
```

**Named Tables**

Daft allows also the registration of named tables, which have no catalog associated with them.

Note that named tables take precedence over the default catalog's table names when resolving names.

```python
df = daft.from_pydict({"foo": [1, 2, 3]})

daft.catalog.register_named_table(
    "my_table",
    df,
)

# Your table is now accessible from Daft-SQL, or Daft's `read_table`
df1 = daft.read_table("my_table")
df2 = daft.sql("SELECT * FROM my_table")
```
"""

from __future__ import annotations

from daft.daft import catalog as native_catalog
from daft.logical.builder import LogicalPlanBuilder

from daft.dataframe import DataFrame

_PYICEBERG_AVAILABLE = False
try:
    from pyiceberg.catalog import Catalog as PyIcebergCatalog

    _PYICEBERG_AVAILABLE = True
except ImportError:
    pass

_UNITY_AVAILABLE = False
try:
    from daft.unity_catalog import UnityCatalog

    _UNITY_AVAILABLE = True
except ImportError:
    pass

__all__ = [
    "read_table",
    "register_python_catalog",
    "unregister_catalog",
    "register_table",
]

# Forward imports from the native catalog which don't require Python wrappers
from daft.daft.catalog import unregister_catalog


def read_table(name: str, catalog_name: str | None = None) -> DataFrame:
    """Finds a table with the specified name and reads it as a DataFrame

    The order of finding the table is:

    1. Any tables registered with `daft.register_named_table()`
    2. Tables in the configured default Data Catalog

    Args:
        name: The name of the table to read. This can be a fully-qualified name (e.g. "catalog.database.table") or a simple name (e.g. "table").
        catalog_name: The name of the catalog to find this table from. If not provided, this will first search the available registered named tables, then the default catalog (if configured).

    Returns:
        A DataFrame containing the data from the specified table.
    """
    native_logical_plan_builder = native_catalog.read_table(name, catalog_name=catalog_name)
    return DataFrame(LogicalPlanBuilder(native_logical_plan_builder))


def register_table(name: str, dataframe: DataFrame) -> str:
    """Register a DataFrame as a named table.

    This function registers a DataFrame as a named table, making it accessible
    via Daft-SQL or Daft's `read_table` function.

    Args:
        name (str): The name to register the table under.
        dataframe (daft.DataFrame): The DataFrame to register as a table.

    Returns:
        str: The name of the registered table.

    Example:
        >>> df = daft.from_pydict({"foo": [1, 2, 3]})
        >>> daft.catalog.register_table("my_table", df)
        >>> daft.read_table("my_table")
    """
    return native_catalog.register_table(name, dataframe._builder._builder)


def register_python_catalog(catalog: PyIcebergCatalog | UnityCatalog, name: str | None = None) -> str:
    """Registers a Python catalog with Daft

    Currently supports:

    * [PyIceberg Catalogs](https://py.iceberg.apache.org/api/)
    * [Unity Catalog](https://www.getdaft.io/projects/docs/en/latest/user_guide/integrations/unity-catalog.html)

    Args:
        catalog (PyIcebergCatalog | UnityCatalog): The Python catalog to register.
        name (str | None, optional): The name to register the catalog under. If None, this catalog is registered as the default catalog.

    Returns:
        str: The name of the registered catalog.

    Raises:
        ValueError: If an unsupported catalog type is provided.

    Example:
        >>> from pyiceberg.catalog import load_catalog
        >>> catalog = load_catalog("my_catalog")
        >>> daft.catalog.register_python_catalog(catalog, "my_daft_catalog")

    """
    python_catalog: PyIcebergCatalog
    if _PYICEBERG_AVAILABLE and isinstance(catalog, PyIcebergCatalog):
        from daft.catalog.pyiceberg import PyIcebergCatalogAdaptor

        python_catalog = PyIcebergCatalogAdaptor(catalog)
    elif _UNITY_AVAILABLE and isinstance(catalog, UnityCatalog):
        from daft.catalog.unity import UnityCatalogAdaptor

        python_catalog = UnityCatalogAdaptor(catalog)
    else:
        raise ValueError(f"Unsupported catalog type: {type(catalog)}")

    return native_catalog.register_python_catalog(python_catalog, name)
