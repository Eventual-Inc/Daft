from typing import Dict, List

from daft.dataframe import DataFrame


def test_projection_scan_pushdown(valid_data: List[Dict[str, float]]) -> None:
    df1 = DataFrame.from_pylist(valid_data)

    df2 = DataFrame.from_pylist(valid_data)

    df3 = df1.join(df2, "variety")
    df3.collect()
    # import ipdb
    # ipdb.set_trace()
