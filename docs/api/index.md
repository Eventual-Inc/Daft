# Daft API Documentation

Welcome to Daft Python API Documentation. For Daft User Guide, head to [User Guide](../index.md).

<div class="grid cards" markdown>

- [**DataFrame Creation**](dataframe_creation.md)

    Variety of approaches to creating a DataFrame from various data sources like in-memory data, files, data catalogs, and integrations.

- [**DataFrame**](dataframe.md)

    Available DataFrame methods that are enqueued in the DataFrame's internal query plan and executed when Execution DataFrame methods are called.

- [**Expressions**](expressions.md)

    Expressions allow you to express some computation that needs to happen in a DataFrame.

- [**Functions**](functions.md)

    Daft Functions provide a set of built-in operations that can be applied to DataFrame columns.

- [**Sessions**](sessions.md)

    Sessions enable you to attach catalogs, tables, and create temporary objects which are accessible through both the Python and SQL APIs.

- [**Catalogs & Tables**](catalogs_tables.md)

    Daft integrates with various catalog implementations using its Catalog and Table interfaces to manage catalog objects like tables and namespaces.

- [**Schema**](schema.md)

    Daft can display your DataFrame's schema without materializing it by performing intelligent sampling of your data to determine appropriate schema.

- [**Data Types**](datatypes.md)

    Daft provides simple DataTypes that are ubiquituous in many DataFrames such as numbers, strings, dates, tensors, and images.

- [**Groupby**](groupby.md)

    When performing aggregations such as sum, mean and count, Daft enables you to group data by certain keys and aggregate within those keys.

- [**User-Defined Functions**](udf.md)

    User-Defined Functions (UDFs) are a mechanism to run Python code on the data that lives in a DataFrame.

- [**Series**](series.md)

    Series expose methods which invoke high-performance kernels for manipulation of a column of data.

- [**Configuration**](config.md)

    Configure the execution backend, Daft in various ways during execution, and how Daft interacts with storage.

- [**Miscellaneous**](misc.md)

</div>
