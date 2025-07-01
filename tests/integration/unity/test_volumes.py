from __future__ import annotations

import pytest


@pytest.mark.integration()
def test_load_volume(local_unity_catalog):
    json_files = local_unity_catalog.load_volume("unity.default.json_files")
    assert json_files.volume_info.full_name == "unity.default.json_files"

    txt_files = local_unity_catalog.load_volume("unity.default.txt_files")
    assert txt_files.volume_info.full_name == "unity.default.txt_files"

    with pytest.raises(Exception):
        local_unity_catalog.load_volume("unity.default.nonexistent_volume")


@pytest.mark.integration()
@pytest.mark.parametrize("repartition_nparts", [1, 2, 5])
def test_read_volume(make_df, local_unity_catalog, repartition_nparts, with_morsel_size):
    df = make_df(
        {
            "id": [1, 2, 3, 4, 5],
            "files": [
                "vol+dbfs:/Volumes/unity/default/txt_files/a.txt",
                "vol+dbfs:/Volumes/unity/default/json_files/c.json",
                "vol+dbfs:/Volumes/unity/default/json_files/d.json",
                "dbfs:/Volumes/unity/default/json_files/dir1/e.json",
                "dbfs:/Volumes/unity/default/txt_files/b.txt",
            ],
        },
        repartition=repartition_nparts,
    )

    io_config = local_unity_catalog.to_io_config()

    df = df.select("id", df["files"].url.download(io_config=io_config))

    result = df.sort("id").to_pydict()
    assert result == {
        "id": [1, 2, 3, 4, 5],
        "files": [
            b"Managed volume file name a.txt",
            b'{\n  "marks" :[\n    {"name" :  "a" , "score" :  20},\n    {"name" :  "b" , "score" :  30},\n    {"name" :  "c" , "score" :  40},\n    {"name" :  "d" , "score" :  50}\n  ]\n}\n',
            b'{\n  "type": "object",\n  "properties": {\n    "name": {\n      "type": "string"\n    },\n    "age": {\n      "type": "integer"\n    },\n    "address": {\n      "type": "string"\n    }\n  }\n}\n',
            b"[1,2,3,4]\n",
            b"Managed volume file name b.txt",
        ],
    }
