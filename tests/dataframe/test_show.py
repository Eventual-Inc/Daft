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


def test_show_from_cached_repr(make_df, valid_data):
    df = make_df(valid_data)
    df = df.collect()
    df.__repr__()
    collected_preview = df._preview
    df_display = df._construct_show_display(8)

    # Check that cached preview from df.__repr__() was used.
    assert df_display.preview is collected_preview
    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 3


def test_show_from_cached_repr_prefix(make_df, valid_data):
    df = make_df(valid_data)
    df = df.collect(3)
    df.__repr__()
    df_display = df._construct_show_display(2)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == 2
    # Check that a prefix of the cached preview from df.__repr__() was used, so dataframe_num_rows should be set.
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 2


def test_show_not_from_cached_repr(make_df, valid_data, data_source):
    df = make_df(valid_data)
    df = df.collect(2)
    df.__repr__()
    collected_preview = df._preview
    df_display = df._construct_show_display(8)

    variant = data_source
    if variant == "parquet":
        # Cached preview from df.__repr__() is NOT USED because data was not materialized from parquet.
        assert df_display.preview != collected_preview
    elif variant == "arrow":
        # Cached preview from df.__repr__() is USED because data was materialized from arrow.
        assert df_display.preview == collected_preview
    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 3
