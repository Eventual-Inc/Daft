# isort: dont-add-import: from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Union

from daft.api_annotations import PublicAPI

if TYPE_CHECKING:
    import dask
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    from ray.data.dataset import Dataset as RayDataset

    from daft.dataframe import DataFrame


InputListType = Union[list, "np.ndarray", "pa.Array", "pa.ChunkedArray"]


@PublicAPI
def from_pylist(data: List[Dict[str, Any]]) -> "DataFrame":
    """Creates a DataFrame from a list of dictionaries.

    Args:
        data: List of dictionaries, where each key is a column name.

    Returns:
        DataFrame: DataFrame created from list of dictionaries.

    Example:
        ``` py linenums="1"
        import daft

        df = daft.from_pylist([{"foo": 1}, {"foo": 2}])
        df.show()
        ```
        ```
        ╭───────╮
        │ foo   │
        │ ---   │
        │ Int64 │
        ╞═══════╡
        │ 1     │
        ├╌╌╌╌╌╌╌┤
        │ 2     │
        ╰───────╯

        (Showing first 2 of 2 rows)
        ```
    """
    from daft import DataFrame

    return DataFrame._from_pylist(data)


@PublicAPI
def from_pydict(data: Dict[str, InputListType]) -> "DataFrame":
    """Creates a DataFrame from a Python dictionary.

    Args:
        data: Key -> Sequence[item] of data. Each Key is created as a column, and must have a value that is
        a Python list, Numpy array or PyArrow array. Values must be equal in length across all keys.

    Returns:
        DataFrame: DataFrame created from dictionary of columns


    Example:
        ``` py linenums="1"
        import daft

        df = daft.from_pydict({"foo": [1, 2]})
        df.show()
        ```
        ```
        ╭───────╮
        │ foo   │
        │ ---   │
        │ Int64 │
        ╞═══════╡
        │ 1     │
        ├╌╌╌╌╌╌╌┤
        │ 2     │
        ╰───────╯

        (Showing first 2 of 2 rows)
        ```
    """
    from daft import DataFrame

    return DataFrame._from_pydict(data)


@PublicAPI
def from_arrow(data: Union["pa.Table", List["pa.Table"], Iterable["pa.Table"]]) -> "DataFrame":
    """Creates a DataFrame from a pyarrow Table.

    Args:
        data: pyarrow Table(s) that we wish to convert into a Daft DataFrame.

    Returns:
        DataFrame: DataFrame created from the provided pyarrow Table.

    Example:
        ``` py linenums="1"
        import pyarrow as pa
        import daft

        t = pa.table({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})
        df = daft.from_arrow(t)
        df.show()
        ```
        ```
        ╭───────┬──────╮
        │ a     ┆ b    │
        │ ---   ┆ ---  │
        │ Int64 ┆ Utf8 │
        ╞═══════╪══════╡
        │ 1     ┆ foo  │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ 2     ┆ bar  │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ 3     ┆ baz  │
        ╰───────┴──────╯

        (Showing first 3 of 3 rows)
        ```
    """
    from daft import DataFrame

    return DataFrame._from_arrow(data)


@PublicAPI
def from_pandas(data: Union["pd.DataFrame", List["pd.DataFrame"]]) -> "DataFrame":
    """Creates a Daft DataFrame from a pandas DataFrame.

    Args:
        data: pandas DataFrame(s) that we wish to convert into a Daft DataFrame.

    Returns:
        DataFrame: Daft DataFrame created from the provided pandas DataFrame.


    Example:
        ``` py linenums="1"
        import pandas as pd
        import daft

        pd_df = pd.DataFrame({"a": [1, 2, 3], "b": ["foo", "bar", "baz"]})
        df = daft.from_pandas(pd_df)
        df.show()
        ```
        ```
        ╭───────┬──────╮
        │ a     ┆ b    │
        │ ---   ┆ ---  │
        │ Int64 ┆ Utf8 │
        ╞═══════╪══════╡
        │ 1     ┆ foo  │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ 2     ┆ bar  │
        ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌┤
        │ 3     ┆ baz  │
        ╰───────┴──────╯

        (Showing first 3 of 3 rows)
        ```
    """
    from daft import DataFrame

    return DataFrame._from_pandas(data)


@PublicAPI
def from_ray_dataset(ds: "RayDataset") -> "DataFrame":
    """Creates a DataFrame from a Ray Dataset.

    !!! note "This function can only work if Daft is running using the RayRunner."

    Args:
        ds: The Ray Dataset to create a Daft DataFrame from.
    """
    from daft import DataFrame

    return DataFrame._from_ray_dataset(ds)


@PublicAPI
def from_dask_dataframe(ddf: "dask.DataFrame") -> "DataFrame":
    """Creates a Daft DataFrame from a Dask DataFrame.

    The provided Dask DataFrame must have been created using [Dask-on-Ray](https://docs.ray.io/en/latest/ray-more-libs/dask-on-ray.html).

    !!! note "This function can only work if Daft is running using the RayRunner"

    Args:
        ddf: The Dask DataFrame to create a Daft DataFrame from.
    """
    from daft import DataFrame

    return DataFrame._from_dask_dataframe(ddf)
