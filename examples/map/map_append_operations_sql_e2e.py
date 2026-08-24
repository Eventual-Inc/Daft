from __future__ import annotations

import daft


# End-to-end script using SQL map style constructor.
def run_sql_map_workflow() -> None:
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

    base_map_df = daft.sql(
        """
        SELECT map(
            'street', street,
            'city', city,
            'country', country
        ) AS map_entry
        FROM base_df
        """,
        base_df=base_df,
    )

    incoming_map_df = daft.sql(
        """
        SELECT map(
            'street', street,
            'city', city,
            'country', country,
            'zipcode', zipcode
        ) AS map_entry
        FROM incoming_df
        """,
        incoming_df=incoming_df,
    )

    appended = base_map_df.concat(incoming_map_df)

    projected = appended.select(
        daft.col("map_entry").map_get("street").alias("street"),
        daft.col("map_entry").map_get("city").alias("city"),
        daft.col("map_entry").map_get("country").alias("country"),
        daft.col("map_entry").map_get("zipcode").alias("zipcode"),
    )

    print("SQL map append output:")
    projected.show()


if __name__ == "__main__":
    run_sql_map_workflow()
