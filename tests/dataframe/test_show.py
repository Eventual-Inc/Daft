from __future__ import annotations

import daft


def test_show_default(valid_data):
    df = daft.from_pylist(valid_data)
    df_display = df._construct_show_display(8)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 3


def test_show_some(valid_data):
    df = daft.from_pylist(valid_data)
    df_display = df._construct_show_display(1)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == 1
    # Limit is less than DataFrame length, so we don't know the full DataFrame length.
    assert df_display.preview.dataframe_num_rows is None
    assert df_display.num_rows == 1


def test_show_from_cached_collect(valid_data):
    df = daft.from_pylist(valid_data)
    df = df.collect()
    collected_preview = df._preview
    df_display = df._construct_show_display(8)

    # Check that cached preview from df.collect() was used.
    assert df_display.preview is collected_preview
    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 3


def test_show_from_cached_collect_prefix(valid_data):
    df = daft.from_pylist(valid_data)
    df = df.collect(3)
    df_display = df._construct_show_display(2)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == 2
    # Check that a prefix of the cached preview from df.collect() was used, so dataframe_num_rows should be set.
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 2


def test_show_not_from_cached_collect(valid_data):
    df = daft.from_pylist(valid_data)
    df = df.collect(2)
    collected_preview = df._preview
    df_display = df._construct_show_display(8)

    # Check that cached preview from df.collect() was NOT used, since it didn't have enough rows.
    assert df_display.preview != collected_preview
    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows == 3
    assert df_display.num_rows == 3
