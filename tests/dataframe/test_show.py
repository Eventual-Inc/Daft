from __future__ import annotations


def test_show_default(make_df, valid_data):
    df = make_df(valid_data)
    df_display = df._construct_show_display(8)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 3


def test_show_some(make_df, valid_data):
    df = make_df(valid_data)
    df_display = df._construct_show_display(1)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == 1
    # Limit is less than DataFrame length, so we only know full DataFrame length if it was loaded from memory, e.g. arrow.
    assert df_display.preview.dataframe_num_rows is (None if "Parquet" in str(df._builder) else 3)
    assert df_display.num_rows == 1


def test_show_from_cached_collect(make_df, valid_data):
    df = make_df(valid_data)
    df = df.collect()
    collected_preview = df._preview
    df_display = df._construct_show_display(8)

    # Check that cached preview from df.collect() was used.
    assert df_display.preview is collected_preview
    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 3


def test_show_from_cached_collect_prefix(make_df, valid_data):
    df = make_df(valid_data)
    df = df.collect(3)
    df_display = df._construct_show_display(2)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == 2
    # Check that a prefix of the cached preview from df.collect() was used, so dataframe_num_rows should be set.
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 2


def test_show_not_from_cached_collect(make_df, valid_data):
    df = make_df(valid_data)
    is_parquet = "Parquet" in str(df._builder)
    df = df.collect(2)
    collected_preview = df._preview
    df_display = df._construct_show_display(8)

    if is_parquet:
        # Check that cached preview from df.collect() was NOT USED, since it didn't have enough rows.
        assert df_display.preview != collected_preview
    else:
        # Check that cached preview from df.collect() was USED, since it had enough rows.
        assert df_display.preview == collected_preview
    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 3
