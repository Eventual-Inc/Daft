from __future__ import annotations

import pytest

from daft.expressions import col
from daft.table import MicroPartition


# Chose a non-exhaustive sample of the filters provided at https://github.com/01mf02/jaq/tree/main
@pytest.mark.parametrize(
    "data, query, expected",
    [
        # Test with object
        pytest.param(['{"col": 1}', '{"col": 2}', '{"col": 3}', None], ".col", ["1", "2", "3", None], id="object"),
        # Test with nested objects
        pytest.param(
            ['{"col": {"a": 1}}', '{"col": {"a": 2}}', '{"col": {"a": 3}}', None],
            ".col.a",
            ["1", "2", "3", None],
            id="nested object",
        ),
        # Test with array
        pytest.param(["[1, 2, 3]", "[4, 5, 6]", "[7, 8, 9]", None], ".[1]", ["2", "5", "8", None], id="array"),
        # Test with nested arrays
        pytest.param(
            ["[[1, 2, 3]]", "[[4, 5, 6]]", "[[7, 8, 9]]", None], ".[0].[1]", ["2", "5", "8", None], id="nested_array"
        ),
        # Test length
        pytest.param(['"1"', "[1, 2]", '{"a": 3, "b": 4, "c": 5}'], "length", ["1", "2", "3"], id="length"),
        # Test split
        pytest.param(
            ['"a,b,c"', '"d,e,f"', '"g,h,i"'],
            'split(",")',
            ['["a","b","c"]', '["d","e","f"]', '["g","h","i"]'],
            id="split",
        ),
        # Test map
        pytest.param(["[0, 1, 2, 3]"], "map(.*2) | [.[] | select(. < 5)]", ["[0,2,4]"], id="map"),
        # Test iteration
        pytest.param(["[1, 2, 3]", "[4, 5, 6]", "[7, 8, 9]"], ".[]", ["1\n2\n3", "4\n5\n6", "7\n8\n9"], id="iteration"),
    ],
)
def test_json_query_ok(data, query, expected):
    mp = MicroPartition.from_pydict({"col": data})
    result = mp.eval_expression_list([col("col").json.query(query)])
    assert result.to_pydict() == {"col": expected}


@pytest.mark.parametrize(
    "data, query, expected",
    [
        pytest.param(['{"col": 1}', '{"col": 2}', '{"col": 3}'], ".a", ["null", "null", "null"], id="missing key"),
        pytest.param(
            ["[1, 2, 3]", "[4, 5, 6]", "[7, 8, 9]"], ".[3]", ["null", "null", "null"], id="index out of range"
        ),
    ],
)
def test_json_query_null_results(data, query, expected):
    mp = MicroPartition.from_pydict({"col": data})
    result = mp.eval_expression_list([col("col").json.query(query)])
    assert result.to_pydict() == {"col": expected}


def test_json_query_invalid_query():
    mp = MicroPartition.from_pydict({"col": ["a", "b", "c"]})
    with pytest.raises(ValueError, match="Error parsing json query"):
        mp.eval_expression_list([col("col").json.query("")])


def test_json_query_invalid_filter():
    mp = MicroPartition.from_pydict({"col": ["a", "b", "c"]})
    with pytest.raises(ValueError, match="Error compiling json query"):
        mp.eval_expression_list([col("col").json.query("a")])


def test_json_query_invalid_json():
    mp = MicroPartition.from_pydict({"col": ["a", "b", "c"]})
    with pytest.raises(ValueError, match="DaftError::IoError"):
        mp.eval_expression_list([col("col").json.query(".a")])


def test_json_query_failed_query():
    mp = MicroPartition.from_pydict({"col": ["[1, 2, 3]"]})
    with pytest.raises(ValueError, match="Error running json query"):
        mp.eval_expression_list([col("col").json.query('split(",")')])
