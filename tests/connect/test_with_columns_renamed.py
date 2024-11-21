from __future__ import annotations


def test_with_columns_renamed(spark_session):
    # Test withColumnRenamed
    df = spark_session.range(5)
    renamed_df = df.withColumnRenamed("id", "number")

    collected = renamed_df.collect()
    assert len(collected) == 5
    assert "number" in renamed_df.columns
    assert "id" not in renamed_df.columns
    assert [row["number"] for row in collected] == list(range(5))

    # todo: this fails but is this expected or no?
    # # Test withColumnsRenamed
    # df = spark_session.range(2)
    # renamed_df = df.withColumnsRenamed({"id": "number", "id": "character"})

    # collected = renamed_df.collect()
    # assert len(collected) == 2
    # assert set(renamed_df.columns) == {"number", "character"}
    # assert "id" not in renamed_df.columns
    # assert [(row["number"], row["character"]) for row in collected] == [(0, 0), (1, 1)]
