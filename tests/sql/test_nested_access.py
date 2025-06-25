from __future__ import annotations

import daft


def test_nested_access():
    df = daft.from_pydict(
        {
            "json": ['{"a": 1, "b": {"c": 2}}', '{"a": 3, "b": {"c": 4}}', '{"a": 5, "b": {"c": 6}}'],
            "list": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            "dict": [{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 5, "b": 6}],
        }
    )

    actual = daft.sql(
        """
    select
        jq(json, '.b.c') as json_b_c,
        list[1] as list_1,
        list[0:1] as list_slice,
        dict['a'] as dict_a,
        struct_get(dict, 'a') as dict_a_2,
        cast(list as int[3])[1] as fsl_1,
        cast(list as int[3])[0:1] as fsl_slice
    from test
    """,
        test=df,
    ).collect()

    expected = df.select(
        daft.col("json").jq(".b.c").alias("json_b_c"),
        daft.col("list").list.get(1).alias("list_1"),
        daft.col("list").list.slice(0, 1).alias("list_slice"),
        daft.col("dict").struct.get("a").alias("dict_a"),
        daft.col("dict").struct.get("a").alias("dict_a_2"),
        daft.col("list").cast(daft.DataType.fixed_size_list(daft.DataType.int32(), 3)).list.get(1).alias("fsl_1"),
        daft.col("list")
        .cast(daft.DataType.fixed_size_list(daft.DataType.int32(), 3))
        .list.slice(0, 1)
        .alias("fsl_slice"),
    ).collect()

    assert actual.to_pydict() == expected.to_pydict()
