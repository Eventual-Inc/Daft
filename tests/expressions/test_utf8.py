from __future__ import annotations


def test_endswith(test_expression):
    test_data = ["hello", "world", "python"]
    expected = [False, False, True]
    test_expression(
        data=test_data, expected=expected, name="endswith", sql_name="ends_with", namespace="str", args=["thon"]
    )


def test_startswith(test_expression):
    test_data = ["hello", "world", "python"]
    expected = [True, False, False]
    test_expression(
        data=test_data, expected=expected, name="startswith", sql_name="starts_with", namespace="str", args=["hello"]
    )


def contains(test_expression):
    test_data = ["hello", "world", "python"]
    expected = [True, False, True]
    test_expression(
        data=test_data, expected=expected, name="contains", sql_name="contains", namespace="str", args=[["lo"]]
    )


def test_upper(test_expression):
    test_data = ["hello", "world", "python"]
    expected = ["HELLO", "WORLD", "PYTHON"]
    test_expression(
        data=test_data,
        expected=expected,
        name="upper",
        namespace="str",
    )


def test_upper_full_null(test_expression):
    test_data = [None, None, None]
    expected = [None, None, None]
    test_expression(
        data=test_data,
        expected=expected,
        name="upper",
        namespace="str",
    )


def test_capitalize(test_expression):
    test_data = ["hello", "world", "python"]
    expected = ["Hello", "World", "Python"]
    test_expression(
        data=test_data,
        expected=expected,
        name="capitalize",
        namespace="str",
    )


def test_lower(test_expression):
    test_data = ["HELLO", "WORLD", "PYTHON"]
    expected = ["hello", "world", "python"]
    test_expression(
        data=test_data,
        expected=expected,
        name="lower",
        namespace="str",
    )


def test_extract(test_expression):
    test_data = ["123-456", "789-012", "345-678"]
    regex = r"(\d)(\d*)"
    expected = ["123", "789", "345"]
    test_expression(
        data=test_data,
        expected=expected,
        name="extract",
        namespace="str",
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
        namespace="str",
        sql_name="substr",
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
        name="replace",
        namespace="str",
        sql_name="regexp_replace",
        args=[regex, replace],
        kwargs={"regex": True},
    )
