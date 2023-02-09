from __future__ import annotations

from daft import DataFrame


def test_show_all(valid_data):
    df = DataFrame.from_pylist(valid_data)
    df_display = df.show()

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows == len(valid_data)


def test_show_some(valid_data):
    df = DataFrame.from_pylist(valid_data)
    df_display = df.show(1)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == 1
    # Show does not know the total size of the dataset when applying a limit
    assert df_display.preview.dataframe_num_rows is None
