from __future__ import annotations

import daft


def test_read_huggingface_datasets_doesnt_flae():
    from daft import DataType as dt

    # run it multiple times to ensure it doesn't fail
    for _ in range(10):
        df = daft.read_parquet("hf://datasets/huggingface/documentation-images")
        schema = df.schema()
        expected = daft.Schema.from_pydict({"image": dt.struct({"bytes": dt.binary(), "path": dt.string()})})
        assert schema == expected
