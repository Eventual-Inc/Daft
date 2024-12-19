from __future__ import annotations


def test_print_schema(spark_session: object, capsys: object) -> None:
    df = spark_session.range(10)
    df.printSchema()
    
    captured = capsys.readouterr()
    expected = (
        "root\n"
        " |-- id: long (nullable = true)\n"
    )
    assert captured.out == expected
