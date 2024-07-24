# isort: dont-add-import: from __future__ import annotations


from daft.api_annotations import PublicAPI
from daft.daft import sql as _sql


@PublicAPI
def sql(sql: str) -> str:
    """Create a DataFrame from an SQL query.

    Args:
        sql (str): SQL query to execute

    Returns:
        DataFrame: Dataframe containing the results of the query
    """
    return _sql(sql)
