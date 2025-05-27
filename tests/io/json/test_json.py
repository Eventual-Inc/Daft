import daft
from daft.daft import PyMicroPartition
from daft.dataframe import DataFrame
from daft.recordbatch import MicroPartition


def get_asset_uri(path: str) -> str:
    """Keep assets next to the tests, but use this to get the full path."""
    return f"tests/io/json/assets/{path}"

def test_read_json():

    single_json = get_asset_uri("single.json")
    """
    ```
    $ cat single.json 
    {
        "a": 1,
        "b": 2
    }
    ```
    """

    # err! read_json uses simd_json, this fails because the impl assumes jsonl.
    # df = daft.read_json(uri)
    # df.show()

    # ok. read_json_native does not use simd_json
    pt = PyMicroPartition.read_json_native(single_json)
    pt = MicroPartition._from_pymicropartition(pt)
    df = DataFrame._from_micropartitions(pt)
    df.show()
