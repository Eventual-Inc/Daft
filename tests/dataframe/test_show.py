from __future__ import annotations

import pytest

import daft


def test_show_default(make_df, valid_data):
    df = make_df(valid_data)
    df_display = df._construct_show_display(8)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.partition) == len(valid_data)
    assert df_display.preview.num_rows == 3
    assert df_display.num_rows == 3


def test_show_some(make_df, valid_data, data_source):
    df = make_df(valid_data)
    df_display = df._construct_show_display(1)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.partition) == 1
    # Limit is less than DataFrame length, so we only know full DataFrame length if it was loaded from memory, e.g. arrow.
    variant = data_source
    if variant == "parquet":
        assert df_display.preview.num_rows is None
    elif variant == "arrow":
        assert df_display.preview.num_rows == len(valid_data)
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
    assert len(df_display.preview.partition) == len(valid_data)
    assert df_display.preview.num_rows == 3
    assert df_display.num_rows == 3


def test_show_from_cached_repr_prefix(make_df, valid_data):
    df = make_df(valid_data)
    df = df.collect(3)
    df.__repr__()
    df_display = df._construct_show_display(2)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.partition) == 2
    # Check that a prefix of the cached preview from df.__repr__() was used, so dataframe_num_rows should be set.
    assert df_display.preview.num_rows == 3
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
    assert len(df_display.preview.partition) == len(valid_data)
    assert df_display.preview.num_rows == 3
    assert df_display.num_rows == 3


@pytest.mark.skip("how to test?")
def test_show_as_markdown(make_df, valid_data):
    df = make_df(valid_data)
    df.show(format="markdown")


# @pytest.mark.skip("how to test?")
def test_show_with_options():
    df = daft.from_pydict(
        {
            "A": [1, 2, 3, 4],
            "B": [1.5, 2.5, 3.5, 4.5],
            "C": [True, True, False, False],
            "D": [None, None, None, None],
        }
    )
    df.show(format="markdown", null="NULL")
    # df.show(format="default")
    # df.show(format="html")


@pytest.mark.skip("how to test?")
def test_show_with_wide_columns():
    df = daft.from_pydict(
        {
            "A": [
                "This is a very long text that exceeds 120 characters. It contains a lot of information that would typically be truncated when displayed in a table format with limited width settings."
                * 2
            ],
            "B": [
                "Another extremely long piece of text that also exceeds 120 characters. This demonstrates how the show method handles wide columns with extensive content that needs to be properly formatted."
                * 2
            ],
        }
    )
    df.show()
    df.show(max_width=30)
    df.show(max_width=None)
