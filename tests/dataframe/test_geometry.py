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
