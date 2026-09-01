from __future__ import annotations

import pytest

import daft
from daft.functions import split_part


def test_endswith(test_expression):
    test_data = ["hello", "world", "python"]
    expected = [False, False, True]
    test_expression(
        data=test_data,
        expected=expected,
        name="endswith",
        fn_name="ends_with",
        sql_name="ends_with",
        args=["thon"],
    )


def test_startswith(test_expression):
    test_data = ["hello", "world", "python"]
    expected = [True, False, False]
    test_expression(
        data=test_data,
        expected=expected,
        name="startswith",
        fn_name="starts_with",
        sql_name="starts_with",
        args=["hello"],
    )


def contains(test_expression):
    test_data = ["hello", "world", "python"]
    expected = [True, False, True]
    test_expression(
        data=test_data,
        expected=expected,
        name="contains",
        args=[["lo"]],
    )


def test_upper(test_expression):
    test_data = ["hello", "world", "python"]
    expected = ["HELLO", "WORLD", "PYTHON"]
    test_expression(
        data=test_data,
        expected=expected,
        name="upper",
    )


def test_upper_full_null(test_expression):
    test_data = [None, None, None]
    expected = [None, None, None]
    test_expression(
        data=test_data,
        expected=expected,
        name="upper",
    )


def test_capitalize(test_expression):
    test_data = ["hello", "world", "python"]
    expected = ["Hello", "World", "Python"]
    test_expression(
        data=test_data,
        expected=expected,
        name="capitalize",
    )


def test_lower(test_expression):
    test_data = ["HELLO", "WORLD", "PYTHON"]
    expected = ["hello", "world", "python"]
    test_expression(
        data=test_data,
        expected=expected,
        name="lower",
    )


def test_extract(test_expression):
    test_data = ["123-456", "789-012", "345-678"]
    regex = r"(\d)(\d*)"
    expected = ["123", "789", "345"]
    test_expression(
        data=test_data,
        expected=expected,
        name="regexp_extract",
        sql_name="regexp_extract",
        args=[regex],
    )


def test_substr(test_expression):
    test_data = ["daft", "query", "engine"]
    expected = [s[2:] for s in test_data]
    test_expression(
        data=test_data,
        expected=expected,
        name="substr",
        args=[2, None],
    )


def test_regexp_replace(test_expression):
    test_data = ["123-456", "789-012", "345-678"]
    regex = r"^(\d+)-(\d+)$"
    replace = "\\2"
    expected = ["456", "012", "678"]
    test_expression(
        data=test_data,
        expected=expected,
        name="regexp_replace",
        args=[regex, replace],
    )


def test_hamming_distance_str(test_binary_expression):
    left = ["ronald", "ronald", "ronald", "ronald", "ronald", None]
    right = ["ronald", "renuld", "ronaldo", "r", None, None]
    expected = [0, 2, None, None, None, None]

    test_binary_expression(
        left=left,
        right=right,
        name="hamming_distance_str",
        expected=expected,
    )


def test_translate(test_expression):
    test_data = ["AaBbCc", "abc", "hello"]
    # Spark-parity: 'a'->'1', 'b'->'2', 'c'->'3', 'o'->'0'.
    # Characters not in `from` (e.g. 'h', 'e', 'l') are left unchanged.
    expected = ["A1B2C3", "123", "hell0"]
    test_expression(
        data=test_data,
        expected=expected,
        name="translate",
        args=["abco", "1230"],
    )


def test_substring_index_positive(test_expression):
    test_data = ["www.apache.org", "a.b.c.d", "noseparator"]
    expected = ["www.apache", "a.b", "noseparator"]
    test_expression(
        data=test_data,
        expected=expected,
        name="substring_index",
        args=[".", 2],
    )


def test_substring_index_negative(test_expression):
    test_data = ["www.apache.org", "a.b.c.d", "noseparator"]
    # Spark-parity: with count=-2 return the substring after the
    # 2nd-to-last delimiter (i.e. the last 2 tokens joined by the delimiter).
    expected = ["apache.org", "c.d", "noseparator"]
    test_expression(
        data=test_data,
        expected=expected,
        name="substring_index",
        args=[".", -2],
    )


def test_soundex(test_expression):
    # Spark-parity expectations: non-letter starts pass through, interior
    # non-letters act as separators.
    test_data = ["Robert", "Rupert", "3M", "S3S"]
    expected = ["R163", "R163", "3M", "S200"]
    test_expression(
        data=test_data,
        expected=expected,
        name="soundex",
    )


def test_ascii(test_expression):
    # Spark returns the *signed* first byte: non-ASCII -> negative; empty -> 0.
    test_data = ["A", "abc", "", "é"]
    expected = [65, 97, 0, -61]
    test_expression(
        data=test_data,
        expected=expected,
        name="ascii",
        sql_name="ascii",
    )


def test_chr_func_dataframe_and_sql():
    # `chr_func` (and the underlying `chr` SQL function) take an integer column,
    # so they are exercised directly rather than through `test_expression`.
    import daft
    from daft import col, sql
    from daft.functions import chr_func

    data = [65, 97, 322, -1]
    expected = ["A", "a", "B", ""]  # 322 % 256 == 66 -> 'B'; negative -> ''.

    df = daft.from_pydict({"x": data})
    df_res = df.select(chr_func(col("x"))).to_pydict()["x"]
    sql_res = sql("select chr(x) as x from df").to_pydict()["x"]

    assert df_res == expected
    assert sql_res == expected


def test_space_dataframe_and_sql():
    import daft
    from daft import col, sql
    from daft.functions import space

    data = [0, 1, 3, -2]
    expected = ["", " ", "   ", ""]  # negative inputs collapse to empty.

    df = daft.from_pydict({"x": data})
    df_res = df.select(space(col("x"))).to_pydict()["x"]
    sql_res = sql("select space(x) as x from df").to_pydict()["x"]

    assert df_res == expected
    assert sql_res == expected


def test_split_part_positive(test_expression):
    # Spark-parity: out-of-range parts return an empty string; nulls stay null.
    test_data = ["a,b,c", "x,y", "nodelim", None]
    expected = ["b", "y", "", None]
    test_expression(
        data=test_data,
        expected=expected,
        name="split_part",
        args=[",", 2],
    )


def test_split_part_negative(test_expression):
    # Spark-parity: negative parts are counted backward from the end.
    test_data = ["a,b,c", "x,y", "nodelim", None]
    expected = ["c", "y", "nodelim", None]
    test_expression(
        data=test_data,
        expected=expected,
        name="split_part",
        args=[",", -1],
    )


def test_split_part_empty_delimiter(test_expression):
    # Spark-parity: an empty delimiter means the string is not split.
    test_data = ["abc", ""]
    expected = ["abc", ""]
    test_expression(
        data=test_data,
        expected=expected,
        name="split_part",
        args=["", 1],
    )


def test_split_part_null_part(test_expression):
    # Spark-parity: a null part returns null for every row.
    test_data = ["a,b,c", "x,y", None]
    expected = [None, None, None]
    test_expression(
        data=test_data,
        expected=expected,
        name="split_part",
        args=[",", None],
    )


def test_split_part_zero_errors():
    df = daft.from_pydict({"x": ["a,b,c"]})
    with pytest.raises(Exception, match="part must not be 0"):
        df.select(split_part(df["x"], ",", 0)).collect()
