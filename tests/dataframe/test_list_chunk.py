from __future__ import annotations

from daft.expressions import col


def test_list_chunk_groupby_materialization(make_df):
    df = make_df(
        {
            "id": [0, 1, 2, 3],
            "src": [[1, 2, 3, 4], [5, 6, 7, 8], [1, 2, 3, 4], [9, 10, 11, 12]],
        }
    )
    df = df.with_column("bands", col("src").chunk(2))
    out = df.groupby("bands").agg(col("id").list_agg().alias("ids")).to_pydict()
    assert {tuple(tuple(chunk) for chunk in bands) for bands in out["bands"]} == {
        ((1, 2), (3, 4)),
        ((5, 6), (7, 8)),
        ((9, 10), (11, 12)),
    }
