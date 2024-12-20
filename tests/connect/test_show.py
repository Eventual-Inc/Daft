from __future__ import annotations


def test_show(spark_session, capsys):
    df = spark_session.range(10)
    df.show()
    captured = capsys.readouterr()
    expected = (
        "╭───────╮\n"
        "│ id    │\n"
        "│ ---   │\n"
        "│ Int64 │\n"
        "╞═══════╡\n"
        "│ 0     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 1     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 2     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 3     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 4     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 5     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 6     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 7     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 8     │\n"
        "├╌╌╌╌╌╌╌┤\n"
        "│ 9     │\n"
        "╰───────╯\n"
    )
    assert captured.out == expected
