import pytest

import daft


@pytest.fixture
def geo_input_df():
    return daft.from_pydict(
        {
            "wkt_in": [
                "POINT (1 2)",
                "LINESTRING (1 2, 3 4)",
                "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                None,
            ],
            "wkb_in": [
                b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@",
                b"\x01\x02\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@\x00\x00\x00\x00\x00\x00\x08@\x00\x00\x00\x00\x00\x00\x10@",
                b"\x01\x03\x00\x00\x00\x01\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                None,
            ],
            "ref": [
                "POINT (0 0)",
                "POINT (0 0)",
                "POINT (0 0)",
                "POINT (0 0)",
            ],
        }
    )


def test_geometry_xcode_roundtrip(geo_input_df):
    df = (
        geo_input_df.with_column("wkt_geo", daft.col("wkt_in").geo.decode())
        .with_column("wkb_geo", daft.col("wkb_in").geo.decode())
        .with_column("wkt_out_text", daft.col("wkt_geo").geo.encode(True))
        .with_column("wkb_out_text", daft.col("wkb_geo").geo.encode(True))
        .with_column("wkt_out_binary", daft.col("wkt_geo").geo.encode())
        .with_column("wkb_out_binary", daft.col("wkb_geo").geo.encode())
        .select("wkt_in", "wkt_out_text", "wkt_out_binary", "wkb_in", "wkb_out_text", "wkb_out_binary")
    )

    result = df.to_pydict()

    def normalize_wkt(col):
        return [(i.replace(" ", "") if i else None) for i in col]

    assert normalize_wkt(result["wkt_out_text"]) == normalize_wkt(result["wkt_in"])
    assert normalize_wkt(result["wkb_out_text"]) == normalize_wkt(result["wkt_in"])
    assert result["wkb_out_binary"] == result["wkb_in"]
    assert result["wkt_out_binary"] == result["wkb_in"]


def test_geo_area(geo_input_df):
    df = geo_input_df.select(daft.col("wkb_in").geo.decode().geo.area().alias("area"))
    result = df.to_pydict()
    assert result["area"] == [0.0, 0.0, 1.0, None]


def test_geo_distance(geo_input_df):
    df = geo_input_df.with_column("geo", daft.col("wkt_in").geo.decode()).with_column(
        "ref", daft.col("ref").geo.decode()
    )
    df = df.select(daft.col("ref").geo.dist(daft.col("geo")).alias("dist"))
    result = df.to_pydict()
    assert result["dist"] == [pytest.approx(2.2360679774)] * 2 + [0, None]
