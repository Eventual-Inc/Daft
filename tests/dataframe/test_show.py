from __future__ import annotations


def test_show_default(make_df, valid_data):
    df = make_df(valid_data)
    df_display = df._construct_show_display(8)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 3


def test_show_some(make_df, valid_data, data_source):
    df = make_df(valid_data)
    df_display = df._construct_show_display(1)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == 1
    # Limit is less than DataFrame length, so we only know full DataFrame length if it was loaded from memory, e.g. arrow.
    variant = data_source
    if variant == "parquet":
        assert df_display.preview.dataframe_num_rows is None
    elif variant == "arrow":
        assert df_display.preview.dataframe_num_rows == len(valid_data)
    assert df_display.num_rows == 1
