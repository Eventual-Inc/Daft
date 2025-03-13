# DataFrame

Most DataFrame methods are **lazy**, meaning that they do not execute computation immediately when invoked. Instead, these operations are enqueued in the DataFrame's internal query plan, and are only executed when Execution DataFrame methods are called.

::: daft.DataFrame
    options:
        filters: ["!^_", "__getitem__", "__len__", "__contains__", "__iter__"]