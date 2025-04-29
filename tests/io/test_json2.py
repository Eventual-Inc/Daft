from daft.dataframe import dataframe
from daft.schema import DataType, schema
from daft.io._file import FileSource
from daft.io._json2 import JsonSource, JsonColumn, JsonColumns, JsonStrategy

# read_json(..) -> DataFrame
# json_source(..) -> JsonSource

def test_json2():

    path = "/Users/rch/pond/jsons/row_*.json"

    # create a file_source from a glob path
    file_source = FileSource(path)

    # create a json_source from a file_source
    json_source = JsonSource(
        source=file_source,
        strategy=JsonStrategy.JSON,
        columns=JsonColumns([
            JsonColumn.path("a", DataType.int64()),
            JsonColumn.path("b", DataType.bool()),
        ])
    )

    # now we can create a dataframe from the json_source
    print()
    df = dataframe(json_source)
    df.show()
