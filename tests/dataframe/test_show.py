from __future__ import annotations

import daft


def test_show_default(valid_data):
    df = daft.from_pylist(valid_data)
    df_display = df.show()

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == len(valid_data)
    assert df_display.preview.dataframe_num_rows is None
    assert df_display.num_rows == 8


def test_show_some(valid_data):
    df = daft.from_pylist(valid_data)
    df_display = df.show(1)

    assert df_display.schema == df.schema()
    assert len(df_display.preview.preview_partition) == 1
    assert df_display.preview.dataframe_num_rows is None
    assert df_display.num_rows == 1
