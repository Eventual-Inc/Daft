from __future__ import annotations


def test_print_schema_range(spark_session, capsys) -> None:
    df = spark_session.range(10)
    df.printSchema()

    captured = capsys.readouterr()
    expected = "root\n" " |-- id: long (nullable = true)\n\n"
    assert captured.out == expected


def test_print_schema_simple_df(spark_session, capsys) -> None:
    data = [(1,), (2,), (3,)]
    df = spark_session.createDataFrame(data, ["value"])
    df.printSchema()

    captured = capsys.readouterr()
    expected = "root\n" " |-- value: long (nullable = true)\n\n"
    assert captured.out == expected


def test_print_schema_multiple_columns(spark_session, capsys) -> None:
    data = [(1, "a", True), (2, "b", False)]
    df = spark_session.createDataFrame(data, ["id", "name", "flag"])
    df.printSchema()

    captured = capsys.readouterr()
    expected = (
        "root\n"
        " |-- id: long (nullable = true)\n"
        " |-- name: string (nullable = true)\n"
        " |-- flag: boolean (nullable = true)\n\n"
    )
    assert captured.out == expected


def test_print_schema_floating_point(spark_session, capsys) -> None:
    data = [(1.23,), (4.56,)]
    df = spark_session.createDataFrame(data, ["amount"])
    df.printSchema()

    captured = capsys.readouterr()
    expected = "root\n" " |-- amount: double (nullable = true)\n\n"
    assert captured.out == expected


def test_print_schema_with_nulls(spark_session, capsys) -> None:
    data = [(1, None), (None, "test")]
    df = spark_session.createDataFrame(data, ["id", "value"])
    df.printSchema()

    captured = capsys.readouterr()
    expected = "root\n" " |-- id: long (nullable = true)\n" " |-- value: string (nullable = true)\n\n"
    assert captured.out == expected
