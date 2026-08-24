from __future__ import annotations

import daft
from daft import col, lit


# End-to-end script for map append + mutate operations.
def run_map_workflow() -> None:
    base_df = daft.from_pydict({
        "street": ["asdfasf;lkhjasfgdkjha"],
        "city": ["Washington"],
        "country": ["USA"],
    })

    incoming_df = daft.from_pydict({
        "street": ["asdfasf;lkhjasfgdkjha"],
        "city": ["Wales"],
        "country": ["UK"],
        "zipcode": ["234324"],
    })

    # Spark-style map creation with explicit key/value pairs.
    base_map_df = base_df.select(
        daft.functions.map(
            "street",
            col("street"),
            "city",
            col("city"),
            "region",
            lit("US"),
            "country",
            col("country"),
        ).alias("map_entry")
    )

    # SQL-style explicit key/value pair map creation (same shape as SQL map/create_map).
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
            "region",
            lit("EU"),
        ).alias("map_entry")
    )

    def preview(df: daft.DataFrame, title: str) -> None:
        print(title)
        df.select(
            col("map_entry").map_keys().alias("keys"),
            col("map_entry").map_get("street").alias("street"),
            col("map_entry").map_get("city").alias("city"),
            col("map_entry").map_get("country").alias("country"),
            col("map_entry").map_get("region").alias("region"),
            col("map_entry").map_get("zipcode").alias("zipcode"),
            col("map_entry").map_get("continent").alias("continent"),
        ).show(max_width=1000)

    # 1) Append operation: new rows may contain extra keys.
    preview(base_map_df, "Base DF:")
    preview(incoming_map_df, "Incoming DF:")
    appended = base_map_df.concat(incoming_map_df)
    preview(appended, "1) Appended rows (incoming rows may have extra keys):")

    # 2) Update operation: overwrite an existing key.
    updated = appended.select(
        daft.functions.map(
            "street",
            col("map_entry").map_get("street"),
            "city",
            col("map_entry").map_get("city"),
            "country",
            daft.functions.when(col("map_entry").map_get("country") == lit("UK"), then=lit("GB")).otherwise(
                col("map_entry").map_get("country")
            ),
            "region",
            col("map_entry").map_get("region"),
            "zipcode",
            col("map_entry").map_get("zipcode"),
        ).alias("map_entry")
    )
    preview(updated, "2) After update (country UK -> GB):")

    # 3) Insert operation: add a new key to each map.
    inserted = updated.select(
        daft.functions.map(
            "street",
            col("map_entry").map_get("street"),
            "city",
            col("map_entry").map_get("city"),
            "country",
            col("map_entry").map_get("country"),
            "region",
            col("map_entry").map_get("region"),
            "zipcode",
            col("map_entry").map_get("zipcode"),
            "continent",
            lit("EU"),
        ).alias("map_entry")
    )
    preview(inserted, "3) After insert (continent key added):")

    # 4) Delete operation: rebuild map without zipcode.
    deleted = inserted.select(
        daft.functions.map(
            "street",
            col("map_entry").map_get("street"),
            "city",
            col("map_entry").map_get("city"),
            "country",
            col("map_entry").map_get("country"),
            "region",
            col("map_entry").map_get("region"),
            "continent",
            col("map_entry").map_get("continent"),
        ).alias("map_entry")
    )
    preview(deleted, "4) After delete (zipcode removed):")


if __name__ == "__main__":
    run_map_workflow()
