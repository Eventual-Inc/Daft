from __future__ import annotations

import pytest


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_jq(series):
    items = [
        '{ "a": null, "b": true, "c": 1, "d": 1.1, "e": "ABC" }',
        '{ "a": null, "b": true, "c": 2, "d": 2.2, "e": "DEF" }',
        '{ "a": null, "b": false, "c": 3, "d": 3.3, "e": "GHI" }',
        '{ "a": null, "b": false, "c": 3, "d": 4.4, "e": "JKL" }',
        '{ "a": null, "b": null, "c": null, "d": null, "e": null }',
    ]
    # sanity selector tests for various types
    assert series(items).jq(".a") == ["null", "null", "null", "null", "null"]
    assert series(items).jq(".b") == ["true", "true", "false", "false", "null"]
    assert series(items).jq(".c") == ["1", "2", "3", "3", "null"]
    assert series(items).jq(".d") == ["1.1", "2.2", "3.3", "4.4", "null"]
    assert series(items).jq(".e") == ['"ABC"', '"DEF"', '"GHI"', '"JKL"', "null"]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_jq_with_nulls(series):
    items = [
        None,
        '{ "a": null, "b": true, "c": 1, "d": 1.1, "e": "ABC" }',
        None,
        None,
        '{ "a": null, "b": false, "c": 2, "d": 2.2, "e": "DEF" }',
        None,
    ]
    # sanity selector tests for various types
    assert series(items).jq(".a") == [None, "null", None, None, "null", None]
    assert series(items).jq(".b") == [None, "true", None, None, "false", None]
    assert series(items).jq(".c") == [None, "1", None, None, "2", None]
    assert series(items).jq(".d") == [None, "1.1", None, None, "2.2", None]
    assert series(items).jq(".e") == [None, '"ABC"', None, None, '"DEF"', None]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_jq_simple_nested_path(series):
    items = [
        '{ "a": { "b": { "c": "deep" } } }',
        '{ "a": { "b": { "c": "shallow" } } }',
        '{ "a": { "b": { "c": "medium" } } }',
        '{ "a": { "b": { "c": "high" } } }',
    ]
    assert series(items).jq(".a.b.c") == ['"deep"', '"shallow"', '"medium"', '"high"']


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_jq_array_selection(series):
    items = [
        '{ "a": { "b": [1, 2, 3] } }',
        '{ "a": { "b": [4, 5, 6] } }',
        '{ "a": { "b": [7, 8, 9] } }',
        '{ "a": { "b": [10, 11, 12] } }',
    ]
    assert series(items).jq(".a.b[0]") == ["1", "4", "7", "10"]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_jq_nested_array_selection(series):
    items = [
        '{ "a": [{ "b": "nested1" }, { "b": "nested2" }] }',
        '{ "a": [{ "b": "nested3" }, { "b": "nested4" }] }',
        '{ "a": [{ "b": "nested5" }, { "b": "nested6" }] }',
        '{ "a": [{ "b": "nested7" }, { "b": "nested8" }] }',
    ]
    assert series(items).jq(".a[0].b") == ['"nested1"', '"nested3"', '"nested5"', '"nested7"']


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_jq_nested_array_with_array_selection(series):
    items = [
        '{ "a": [{ "b": [1, 2] }, { "b": [3, 4] }] }',
        '{ "a": [{ "b": [5, 6] }, { "b": [7, 8] }] }',
        '{ "a": [{ "b": [9, 10] }, { "b": [11, 12] }] }',
        '{ "a": [{ "b": [13, 14] }, { "b": [15, 16] }] }',
    ]
    assert series(items).jq(".a[0].b[0]") == ["1", "5", "9", "13"]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_jq_array_wildcard(series):
    items = [
        '{ "a": [{ "b": "nested1" }, { "b": "nested2" }] }',
        '{ "a": [{ "b": "nested3" }, { "b": "nested4" }] }',
        '{ "a": [{ "b": "nested5" }, { "b": "nested6" }] }',
        '{ "a": [{ "b": "nested7" }, { "b": "nested8" }] }',
    ]
    assert series(items).jq(".a[].b") == [
        '["nested1","nested2"]',
        '["nested3","nested4"]',
        '["nested5","nested6"]',
        '["nested7","nested8"]',
    ]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_jq_permissive_paths(series):
    items = [
        '{ "a": 1 }',  # <-- !! MISSING !! so this will return a non-match "" rather than "null"
        '{ "a": { "b": null } }',
        '{ "a": { "b": true } }',
    ]
    assert series(items).jq(".a.b?") == ["", "null", "true"]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_jq_permissive_types(series):
    items = [
        '{ "a": true }',  # <-- type exception, so we'll get a non-match ""
        '{ "a": [ 1 ] }',  # <-- out-of-bounds, so we'll get a "null"
        '{ "a": [ 1, 2 ] }',
    ]
    assert series(items).jq("(.a | arrays?)[1]") == ["", "null", "2"]


@pytest.mark.parametrize("series", ["Expression", "SQL"], indirect=True)
def test_jq_complex_transformation(series):
    items = [
        """{
            "users": [
                {
                    "name": "Alice",
                    "scores": [85, 92, 78]
                },
                {
                    "name": "Bob",
                    "scores": [91, 88, 95]
                }
            ]
        }""",
        """{
            "users": [
                {
                    "name": "Charlie",
                    "scores": [82, 89, 94]
                },
                {
                    "name": "Dave",
                    "scores": [88, 91, 86]
                }
            ]
        }""",
    ]
    # some jq filter to get average scores because jq is not jsonpath
    assert series(items).jq(".users[] | {name: .name, avg_score: ([.scores[]] | add / length | round)}") == [
        '[{"name":"Alice","avg_score":85},{"name":"Bob","avg_score":91}]',
        '[{"name":"Charlie","avg_score":88},{"name":"Dave","avg_score":88}]',
    ]


def test_jq_with_deserialize():
    import daft
    from daft import DataType as dt
    from daft import col

    # here's our raw sample data which is just some json dump from a sensor
    df = daft.from_pydict(
        {
            "sample": [
                '{ "x": 1 }',  # missing y, we'll insert 0 in its place
                '{ "x": 1, "y": 1 }',  # ok
                '"HELLO, WORLD!"',  # you're not supposed to be here..
                '{ "x": 3, "y": 3 }',  # ok
                '{ "x": 4, "y": 4 }',  # ok
                '{ "x": false }',  # wrong data type..
            ]
        }
    )

    # select all objects, using 0 as the default for missing keys
    filter = """
        (. | objects?) | { x: .x // 0, y: .y // 0 }
    """

    # our point type is an x/y pair.
    point_t = dt.struct({"x": dt.int64(), "y": dt.int64()})

    # we have the successfully extracted each sample point, now deserialize into our type.
    points = (df.select(col("sample").jq(filter).try_deserialize("json", point_t).alias("point"))).drop_null()

    # now find the max from the origin, no need to sqrt it.
    p = col("point")
    furthest_point = (
        points.with_column("distance", p["x"] * p["x"] + p["y"] * p["y"])
        .sort("distance", desc=True)
        .limit(1)
        .select(p)
        .to_pydict()["point"][0]
    )

    assert furthest_point == {"x": 4, "y": 4}
