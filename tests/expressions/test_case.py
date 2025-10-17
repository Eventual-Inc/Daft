from __future__ import annotations

import daft
from daft import col
from daft.functions import case


def test_case_simple_form_with_otherwise():
    """Test simple form with otherwise clause."""
    df = daft.from_pydict({"value": [1, 2, 3, 4]})
    result = df.select(case(col("value"), [(1, "a"), (2, "b")], otherwise="c").alias("result")).to_pydict()
    assert result["result"] == ["a", "b", "c", "c"]


def test_case_simple_form_without_otherwise():
    """Test simple form without otherwise clause - should return NULL for non-matches."""
    df = daft.from_pydict({"value": [1, 2, 3]})
    result = df.select(case(col("value"), [(1, "a"), (2, "b")]).alias("result")).to_pydict()
    assert result["result"] == ["a", "b", None]


def test_case_simple_form_single_match():
    """Test simple form with single match."""
    df = daft.from_pydict({"value": [1, 2, 1, 3]})
    result = df.select(case(col("value"), [(1, "one")], otherwise="other").alias("result")).to_pydict()
    assert result["result"] == ["one", "other", "one", "other"]


def test_case_simple_form_no_matches():
    """Test simple form where nothing matches."""
    df = daft.from_pydict({"value": [5, 6, 7]})
    result = df.select(case(col("value"), [(1, "a"), (2, "b")], otherwise="default").alias("result")).to_pydict()
    assert result["result"] == ["default", "default", "default"]


def test_case_simple_form_numeric_results():
    """Test simple form with numeric results."""
    df = daft.from_pydict({"value": [1, 2, 3]})
    result = df.select(case(col("value"), [(1, 100), (2, 200)], otherwise=0).alias("result")).to_pydict()
    assert result["result"] == [100, 200, 0]


def test_case_searched_form_with_otherwise():
    """Test searched form with otherwise clause."""
    df = daft.from_pydict({"foo": [5, 10, 15], "bar": [20, 25, 30]})
    result = df.select(
        case([(col("foo") == 10, "a"), (col("bar") == 20, "b")], otherwise="c").alias("result")
    ).to_pydict()
    assert result["result"] == ["b", "a", "c"]


def test_case_searched_form_without_otherwise():
    """Test searched form without otherwise clause."""
    df = daft.from_pydict({"x": [1, 2, 3]})
    result = df.select(case([(col("x") > 1, "big")]).alias("result")).to_pydict()
    assert result["result"] == [None, "big", "big"]


def test_case_searched_form_multiple_conditions():
    """Test searched form with multiple conditions like grade ranges."""
    df = daft.from_pydict({"score": [85, 92, 78, 65, 88]})
    result = df.select(
        case(
            [
                (col("score") >= 90, "A"),
                (col("score") >= 80, "B"),
                (col("score") >= 70, "C"),
            ],
            otherwise="F",
        ).alias("grade")
    ).to_pydict()
    assert result["grade"] == ["B", "A", "C", "F", "B"]


def test_case_searched_form_first_match_wins():
    """Test that first matching condition wins in searched form."""
    df = daft.from_pydict({"x": [10, 20, 30]})
    result = df.select(
        case(
            [
                (col("x") >= 10, "first"),
                (col("x") >= 20, "second"),
            ]
        ).alias("result")
    ).to_pydict()
    # All values match first condition, so all should be "first"
    assert result["result"] == ["first", "first", "first"]


def test_case_searched_form_complex_conditions():
    """Test searched form with complex boolean expressions."""
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [4, 3, 2, 1]})
    result = df.select(
        case(
            [
                ((col("a") > 2) & (col("b") < 3), "both"),
                (col("a") > 2, "a_only"),
                (col("b") < 3, "b_only"),
            ],
            otherwise="neither",
        ).alias("result")
    ).to_pydict()
    # Row 0: a=1, b=4 -> a>2=F, b<3=F -> neither
    # Row 1: a=2, b=3 -> a>2=F, b<3=F -> neither
    # Row 2: a=3, b=2 -> a>2=T & b<3=T -> both
    # Row 3: a=4, b=1 -> a>2=T & b<3=T -> both
    assert result["result"] == ["neither", "neither", "both", "both"]


def test_case_with_null_values():
    """Test case handling of null values."""
    df = daft.from_pydict({"value": [1, None, 2, None]})
    result = df.select(case(col("value"), [(1, "one"), (2, "two")], otherwise="other").alias("result")).to_pydict()
    # NULL comparisons result in NULL being propagated
    # This matches SQL behavior where NULL == anything is NULL (not false)
    assert result["result"] == ["one", None, "two", None]


def test_case_null_in_match_list():
    """Test case with None in the match list for simple form."""
    df = daft.from_pydict({"value": [1, None, 2]})
    result = df.select(case(col("value"), [(1, "one"), (None, "null")], otherwise="other").alias("result")).to_pydict()
    # In SQL, NULL == NULL is NULL (not true or false), so comparisons with NULL propagate NULL
    # Row 0: 1 == 1 -> "one"
    # Row 1: NULL == 1 -> NULL, NULL == NULL -> NULL, so result is NULL
    # Row 2: 2 == 1 -> false, 2 == NULL -> NULL, so result is NULL
    assert result["result"] == ["one", None, None]


def test_case_searched_form_null_condition():
    """Test searched form with is_null condition."""
    df = daft.from_pydict({"value": [1, None, 2]})
    result = df.select(
        case([(col("value").is_null(), "null"), (col("value") == 1, "one")], otherwise="other").alias("result")
    ).to_pydict()
    assert result["result"] == ["one", "null", "other"]


def test_case_expression_results():
    """Test case with expression results instead of literals."""
    df = daft.from_pydict({"value": [1, 2, 3]})
    result = df.select(
        case(col("value"), [(1, col("value") * 10), (2, col("value") * 20)], otherwise=0).alias("result")
    ).to_pydict()
    assert result["result"] == [10, 40, 0]


def test_case_empty_conditions():
    """Test case with empty conditions list."""
    df = daft.from_pydict({"value": [1, 2, 3]})
    result = df.select(case([], otherwise="default").alias("result")).to_pydict()
    # No conditions, should always return otherwise
    assert result["result"] == ["default", "default", "default"]


def test_case_empty_matches():
    """Test simple form with empty matches list."""
    df = daft.from_pydict({"value": [1, 2, 3]})
    result = df.select(case(col("value"), [], otherwise="default").alias("result")).to_pydict()
    # No matches, should always return otherwise
    assert result["result"] == ["default", "default", "default"]


def test_case_mixed_types():
    """Test case with mixed result types."""
    df = daft.from_pydict({"value": [1, 2, 3]})
    # String and int results should be unified to a common type
    result = df.select(case(col("value"), [(1, "100"), (2, "200")], otherwise="0").alias("result")).to_pydict()
    assert result["result"] == ["100", "200", "0"]


def test_case_chaining_multiple_dataframe_operations():
    """Test case in a chain of dataframe operations."""
    df = daft.from_pydict({"value": [1, 2, 3, 4, 5]})
    result = (
        df.where(col("value") <= 4)
        .select(case(col("value"), [(1, "a"), (2, "b")], otherwise="c").alias("result"))
        .to_pydict()
    )
    assert result["result"] == ["a", "b", "c", "c"]


def test_case_with_literal_condition():
    """Test simple form where the expression to match is a literal."""
    df = daft.from_pydict({"x": [1, 2, 3]})
    result = df.select(case(daft.lit(2), [(1, "one"), (2, "two")], otherwise="other").alias("result")).to_pydict()
    # lit(2) always evaluates to 2, so all rows should get "two"
    assert result["result"] == ["two", "two", "two"]
