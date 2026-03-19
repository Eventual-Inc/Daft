from __future__ import annotations

from daft import GeospatialMode
from daft.datatype import DataType, get_super_ext_type
from daft.series import Series

DaftExtension = get_super_ext_type()


def test_point_roundtrip():
    geo_mode = GeospatialMode.from_user_defined_mode("xy", "separated")
    data = ["POINT(1 2)", "POINT(3 4)", None]
    string_series = Series.from_pylist(data, dtype=DataType.string())
    wkt_array = string_series.cast(DataType.wkt(geo_mode))
    casted = wkt_array.cast(DataType.point(geo_mode))
    wkt_roundtrip = casted.cast(DataType.wkt(geo_mode))
    new_string_series = wkt_roundtrip.cast(DataType.string())
    assert new_string_series.to_pylist() == data


def test_linestring_roundtrip():
    geo_mode = GeospatialMode.from_user_defined_mode("xy", "separated")
    data = ["LINESTRING(0 0,1 1,2 2)", "LINESTRING(3 3,4 4)", None]
    string_series = Series.from_pylist(data, dtype=DataType.string())
    wkt_array = string_series.cast(DataType.wkt(geo_mode))
    casted = wkt_array.cast(DataType.linestring(geo_mode))
    wkt_roundtrip = casted.cast(DataType.wkt(geo_mode))
    new_string_series = wkt_roundtrip.cast(DataType.string())
    assert new_string_series.to_pylist() == data


def test_polygon_roundtrip():
    geo_mode = GeospatialMode.from_user_defined_mode("xy", "separated")
    data = [
        "POLYGON((0 0,1 0,1 1,0 1,0 0))",
        "POLYGON((0 0,2 0,2 2,0 2,0 0),(0.5 0.5,1.5 0.5,1.5 1.5,0.5 1.5,0.5 0.5))",
        None,
    ]
    string_series = Series.from_pylist(data, dtype=DataType.string())
    wkt_array = string_series.cast(DataType.wkt(geo_mode))
    casted = wkt_array.cast(DataType.polygon(geo_mode))
    wkt_roundtrip = casted.cast(DataType.wkt(geo_mode))
    new_string_series = wkt_roundtrip.cast(DataType.string())
    assert new_string_series.to_pylist() == data


def test_multipoint_roundtrip():
    geo_mode = GeospatialMode.from_user_defined_mode("xy", "separated")
    data = ["MULTIPOINT((0 0),(1 1),(2 2))", "MULTIPOINT((3 3),(4 4))", None]
    string_series = Series.from_pylist(data, dtype=DataType.string())
    wkt_array = string_series.cast(DataType.wkt(geo_mode))
    casted = wkt_array.cast(DataType.multipoint(geo_mode))
    wkt_roundtrip = casted.cast(DataType.wkt(geo_mode))
    new_string_series = wkt_roundtrip.cast(DataType.string())
    assert new_string_series.to_pylist() == data


def test_multilinestring_roundtrip():
    geo_mode = GeospatialMode.from_user_defined_mode("xy", "separated")
    data = ["MULTILINESTRING((0 0,1 1),(2 2,3 3))", "MULTILINESTRING((4 4,5 5,6 6))", None]
    string_series = Series.from_pylist(data, dtype=DataType.string())
    wkt_array = string_series.cast(DataType.wkt(geo_mode))
    casted = wkt_array.cast(DataType.multilinestring(geo_mode))
    wkt_roundtrip = casted.cast(DataType.wkt(geo_mode))
    new_string_series = wkt_roundtrip.cast(DataType.string())
    assert new_string_series.to_pylist() == data


def test_multipolygon_roundtrip():
    geo_mode = GeospatialMode.from_user_defined_mode("xy", "separated")
    data = [
        "MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)),((2 2,3 2,3 3,2 3,2 2)))",
        "MULTIPOLYGON(((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1)))",
        None,
    ]
    string_series = Series.from_pylist(data, dtype=DataType.string())
    wkt_array = string_series.cast(DataType.wkt(geo_mode))
    casted = wkt_array.cast(DataType.multipolygon(geo_mode))
    wkt_roundtrip = casted.cast(DataType.wkt(geo_mode))
    new_string_series = wkt_roundtrip.cast(DataType.string())
    assert new_string_series.to_pylist() == data


def test_geometry_collection_roundtrip():
    geo_mode = GeospatialMode.from_user_defined_mode("xy", "separated")
    data = [
        "GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(1 1,2 2),POINT(2 2))",
        "GEOMETRYCOLLECTION(LINESTRING(1 1,2 2),POLYGON((1 1,2 2,3 3,1 1)))",
        None,
    ]
    string_series = Series.from_pylist(data, dtype=DataType.string())
    wkt_array = string_series.cast(DataType.wkt(geo_mode))
    casted = wkt_array.cast(DataType.geometry_collection(geo_mode))
    wkt_roundtrip = casted.cast(DataType.wkt(geo_mode))
    new_string_series = wkt_roundtrip.cast(DataType.string())
    assert new_string_series.to_pylist() == data
