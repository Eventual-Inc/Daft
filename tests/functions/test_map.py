from __future__ import annotations

import daft
from daft import col, lit


def test_map_constructor_key_value_fields():
    df = daft.from_pydict({
        "address": ["asdfasf;lkhjasfgdkjha", "zxcvbnm"],
        "city": ["Washington", "Wales"],
        "country": ["USA", "UK"],
    })

    actual = df.select(
        daft.functions.map(
            "street",
            col("address"),
            "city",
            col("city"),
            "country",
            col("country"),
        ).alias("m")
    ).collect()

    expected = daft.sql("SELECT MAP {'street': address, 'city': city, 'country': country} AS m FROM df", df=df).collect()

    assert actual.to_pydict() == expected.to_pydict()


def test_map_constructor_rejects_non_string_key_positions():
    df = daft.from_pydict({"x": [1, 2]})

    try:
        df.select(daft.functions.map(col("x"), col("x")).alias("m"))
        assert False, "Expected map constructor to reject non-string keys"
    except ValueError as e:
        assert "must be a string literal" in str(e)


def test_map_constructor_supports_evolving_keys_on_append():
    base_df = daft.from_pydict({
        "street": ["s1"],
        "city": ["Wales"],
        "country": ["UK"],
    })
    incoming_df = daft.from_pydict({
        "street": ["s2"],
        "city": ["Wales"],
        "country": ["UK"],
        "zipcode": ["234324"],
    })

    base_map_df = base_df.select(
        daft.functions.map(
            "street",
            col("street"),
            "city",
            col("city"),
            "country",
            col("country"),
        ).alias("m")
    )
    incoming_map_df = incoming_df.select(
        daft.functions.map(
            "street",
            col("street"),
            "city",
            col("city"),
            "country",
            col("country"),
            "zipcode",
            col("zipcode"),
        ).alias("m")
    )

    combined = base_map_df.concat(incoming_map_df).collect()
    zipcode_values = combined.select(col("m").map_get("zipcode").alias("zipcode")).to_pydict()["zipcode"]

    assert zipcode_values == [None, "234324"]


def test_map_constructor_supports_multiple_value_dtypes():
    df = daft.from_pydict({
        "street": ["s1", "s2"],
        "city": ["Wales", "Washington"],
        "country": ["UK", "USA"],
        "lat": [52.0, 38.9],
        "lon": [-3.0, -77.0],
        "is_capital": [False, True],
        "is_valid": [True, True],
        "line1": [["a", "b"], ["c"]],
        "line2": [["d"], ["e", "f"]],
    })

    result = df.select(
        daft.functions.map("street", col("street"), "city", col("city"), "country", col("country")).alias("address_map"),
        daft.functions.map("lat", col("lat"), "lon", col("lon")).alias("geo_map"),
        daft.functions.map("is_capital", col("is_capital"), "is_valid", col("is_valid")).alias("flag_map"),
        daft.functions.map("line1", col("line1"), "line2", col("line2")).alias("lines_map"),
    )

    actual = result.select(
        col("address_map").map_get("country").alias("country"),
        col("geo_map").map_get("lat").alias("lat"),
        col("flag_map").map_get("is_capital").alias("is_capital"),
        col("lines_map").map_get("line2").alias("line2"),
    ).to_pydict()

    assert actual == {
        "country": ["UK", "USA"],
        "lat": [52.0, 38.9],
        "is_capital": [False, True],
        "line2": [["d"], ["e", "f"]],
    }


def test_map_append_update_insert_delete_key_workflow():
    base_df = daft.from_pydict({
        "street": ["street-1"],
        "city": ["Wales"],
        "country": ["UK"],
    })
    incoming_df = daft.from_pydict({
        "street": ["street-2"],
        "city": ["Seattle"],
        "country": ["USA"],
        "zipcode": ["98101"],
    })

    base_map_df = base_df.select(
        daft.functions.map("street", col("street"), "city", col("city"), "country", col("country")).alias("m")
    )
    incoming_map_df = incoming_df.select(
        daft.functions.map(
            "street",
            col("street"),
            "city",
            col("city"),
            "country",
            col("country"),
            "zipcode",
            col("zipcode"),
        ).alias("m")
    )

    # Append incoming rows that may contain extra keys.
    appended = base_map_df.concat(incoming_map_df)

    # Update: normalize country values in-place by reconstructing the map.
    updated = appended.select(
        daft.functions.map(
            "street",
            col("m").map_get("street"),
            "city",
            col("m").map_get("city"),
            "country",
            daft.functions.when(col("m").map_get("country") == lit("UK"), then=lit("GB")).otherwise(
                col("m").map_get("country")
            ),
            "zipcode",
            col("m").map_get("zipcode"),
        ).alias("m")
    )

    # Insert: add continent for all rows.
    inserted = updated.select(
        daft.functions.map(
            "street",
            col("m").map_get("street"),
            "city",
            col("m").map_get("city"),
            "country",
            col("m").map_get("country"),
            "zipcode",
            col("m").map_get("zipcode"),
            "continent",
            lit("EU"),
        ).alias("m")
    )

    # Delete key: remove zipcode by reconstructing without that key.
    deleted = inserted.select(
        daft.functions.map(
            "street",
            col("m").map_get("street"),
            "city",
            col("m").map_get("city"),
            "country",
            col("m").map_get("country"),
            "continent",
            col("m").map_get("continent"),
        ).alias("m")
    )

    actual = deleted.select(
        col("m").map_get("country").alias("country"),
        col("m").map_get("continent").alias("continent"),
        col("m").map_get("zipcode").alias("zipcode"),
    ).to_pydict()

    assert actual == {
        "country": ["GB", "USA"],
        "continent": ["EU", "EU"],
        "zipcode": [None, None],
    }


def test_map_append_workflow_using_map_sql():
    base_df = daft.from_pydict({
        "street": ["street-1"],
        "city": ["Wales"],
        "country": ["UK"],
    })
    incoming_df = daft.from_pydict({
        "street": ["street-2"],
        "city": ["Seattle"],
        "country": ["USA"],
        "zipcode": ["98101"],
    })

    base_map_df = daft.sql("SELECT map('street', street, 'city', city, 'country', country) AS m FROM base_df", base_df=base_df)
    incoming_map_df = daft.sql(
        "SELECT map('street', street, 'city', city, 'country', country, 'zipcode', zipcode) AS m FROM incoming_df",
        incoming_df=incoming_df,
    )

    combined = base_map_df.concat(incoming_map_df)
    actual = combined.select(
        col("m").map_get("street").alias("street"),
        col("m").map_get("zipcode").alias("zipcode"),
    ).to_pydict()

    assert actual == {
        "street": ["street-1", "street-2"],
        "zipcode": [None, "98101"],
    }


def test_map_function_supports_sql_style_key_value_pairs():
    df = daft.from_pydict({"x": [1, 2], "y": [10, 20]})

    actual = df.select(daft.functions.map("a", col("x"), "b", col("y") + lit(1)).alias("m")).collect()
    expect = daft.sql("SELECT map('a', x, 'b', y + 1) AS m FROM df", df=df).collect()

    assert actual.to_pydict() == expect.to_pydict()


def test_map_extract_alias_matches_map_get():
    df = daft.from_pydict({"x": [1, 2], "y": [10, 20]})

    maps = df.select(daft.functions.map("a", col("x"), "b", col("y")).alias("m"))
    actual = maps.select(daft.functions.map_extract(col("m"), "a").alias("a")).to_pydict()
    expect = maps.select(daft.functions.map_get(col("m"), "a").alias("a")).to_pydict()

    assert actual == expect
