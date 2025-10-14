"""Tests for the case function - making sure it works like SQL CASE statements."""

from __future__ import annotations

import pytest

import daft
from daft.functions import case


def test_simple_case_grade_conversion():
    """Convert letter grades to GPA - the classic use case for simple CASE."""
    # Set up some student grades
    df = daft.from_pydict({"grade": ["A", "B", "C", "D", "F"]})

    # Convert to GPA using simple case (matching against specific values)
    result = df.select(
        case(
            df["grade"],  # Match against this column
            [
                ("A", 4.0),  # A = 4.0 GPA
                ("B", 3.0),  # B = 3.0 GPA
                ("C", 2.0),  # C = 2.0 GPA
                ("D", 1.0),  # D = 1.0 GPA
            ],
            else_=0.0,  # F or anything else = 0.0
        ).alias("gpa")
    ).collect()

    # Should get the right GPA for each grade
    expected_gpa = [4.0, 3.0, 2.0, 1.0, 0.0]
    assert result.to_pydict()["gpa"] == expected_gpa


def test_simple_case_no_default():
    """What happens when we don't provide an else clause? Should get None for unmatched values."""
    df = daft.from_pydict({"letter": ["A", "B", "X"]})

    # Only handle A and B, no else clause
    result = df.select(
        case(
            df["letter"],
            [
                ("A", "first"),
                ("B", "second"),
            ],
            # No else_ means unmatched values become None
        ).alias("position")
    ).collect()

    # X doesn't match anything, so it should be None
    expected = ["first", "second", None]
    assert result.to_pydict()["position"] == expected


def test_searched_case_score_to_grade():
    """Convert numeric scores to letter grades using searched case (boolean conditions)."""
    # Some student test scores
    df = daft.from_pydict({"score": [85, 92, 78, 65, 88]})

    # Use searched case - no expr parameter, just boolean conditions
    result = df.select(
        case(
            branches=[
                (df["score"] >= 90, "A"),  # 90+ is an A
                (df["score"] >= 80, "B"),  # 80-89 is a B
                (df["score"] >= 70, "C"),  # 70-79 is a C
                (df["score"] >= 60, "D"),  # 60-69 is a D
            ],
            else_="F",  # Below 60 is an F
        ).alias("grade")
    ).collect()

    # Check each score gets the right grade
    # 85->B, 92->A, 78->C, 65->D, 88->B
    expected = ["B", "A", "C", "D", "B"]
    assert result.to_pydict()["grade"] == expected


def test_searched_case_without_else():
    """Test searched case without else clause."""
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    result = df.select(
        case(
            branches=[
                (df["x"] > 3, "high"),
                (df["x"] == 2, "medium"),
            ]
        ).alias("category")
    ).collect()

    expected = [None, "medium", None, "high", "high"]
    assert result.to_pydict()["category"] == expected


def test_complex_business_logic():
    """Real-world example: categorize people based on age and income."""
    # Some people with different ages and incomes
    df = daft.from_pydict({"age": [15, 25, 35, 70], "income": [0, 30000, 60000, 40000]})

    # Complex business rules using multiple conditions
    result = df.select(
        case(
            branches=[
                # Working age adult with decent income
                ((df["age"] >= 18) & (df["age"] < 65) & (df["income"] > 25000), "working_adult"),
                # Anyone under 18 is a minor regardless of income
                (df["age"] < 18, "minor"),
                # Anyone 65+ is a senior regardless of income
                (df["age"] >= 65, "senior"),
            ],
            else_="other",  # Catch-all for edge cases
        ).alias("category")
    ).collect()

    # 15 y/o -> minor, 25 y/o with 30k -> working_adult,
    # 35 y/o with 60k -> working_adult, 70 y/o -> senior
    expected = ["minor", "working_adult", "working_adult", "senior"]
    assert result.to_pydict()["category"] == expected


def test_handling_missing_data():
    """Case statements should handle null/missing values gracefully."""
    # Some data with missing values (None)
    df = daft.from_pydict({"value": [1, None, 3, None, 5]})

    result = df.select(
        case(
            branches=[
                (df["value"].is_null(), "missing"),  # Check for nulls first
                (df["value"] > 3, "high"),  # Then check if high
                (df["value"] <= 3, "low"),  # Then check if low
            ]
        ).alias("status")
    ).collect()

    # 1->low, None->missing, 3->low, None->missing, 5->high
    expected = ["low", "missing", "low", "missing", "high"]
    assert result.to_pydict()["status"] == expected


def test_case_with_expression_values():
    """Test case with expression values in branches."""
    df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})
    result = df.select(
        case(
            branches=[
                (df["x"] > 3, df["x"] * 2),
                (df["x"] <= 3, df["x"] + 10),
            ]
        ).alias("result")
    ).collect()

    expected = [11, 12, 13, 8, 10]
    assert result.to_pydict()["result"] == expected


def test_simple_case_with_expression_match():
    """Test simple case with expression as match value."""
    df = daft.from_pydict({"x": [1, 2, 3], "y": [1, 1, 2]})
    result = df.select(
        case(
            df["x"],
            [
                (df["y"], "match"),
                (2, "two"),
            ],
            else_="no_match",
        ).alias("result")
    ).collect()

    # Row 0: x=1, y=1 → x==y (1==1) → "match"
    # Row 1: x=2, y=1 → x==y (2==1) → false, x==2 (2==2) → "two"
    # Row 2: x=3, y=2 → x==y (3==2) → false, x==2 (3==2) → false → "no_match"
    expected = ["match", "two", "no_match"]
    assert result.to_pydict()["result"] == expected


def test_empty_branches_should_fail():
    """You can't have a case statement with no branches - that doesn't make sense."""
    df = daft.from_pydict({"x": [1, 2, 3]})

    # This should blow up because what would it even do?
    with pytest.raises(ValueError, match="At least one branch must be provided"):
        case(df["x"], [])


def test_building_case_from_dictionary():
    """The real power: build case branches programmatically from data structures."""
    df = daft.from_pydict({"category": ["A", "B", "C", "D", "E"]})

    # Maybe you have a mapping stored somewhere - dictionary, config file, database, etc.
    grade_to_points = {"A": 90, "B": 80, "C": 70, "D": 60}

    # Build the branches dynamically - this is much cleaner than chaining when() calls
    branches = [(category, points) for category, points in grade_to_points.items()]

    result = df.select(case(df["category"], branches, else_=0).alias("points")).collect()

    # A->90, B->80, C->70, D->60, E->0 (else clause)
    expected = [90, 80, 70, 60, 0]
    assert result.to_pydict()["points"] == expected


def test_case_type_consistency():
    """Test that case maintains type consistency."""
    df = daft.from_pydict({"x": [1, 2, 3]})

    # All branches return integers
    result = df.select(
        case(
            branches=[
                (df["x"] == 1, 10),
                (df["x"] == 2, 20),
            ],
            else_=0,
        ).alias("result")
    ).collect()

    expected = [10, 20, 0]
    assert result.to_pydict()["result"] == expected


def test_case_with_mixed_types():
    """Test case with mixed return types (should work with proper type coercion)."""
    df = daft.from_pydict({"x": [1, 2, 3]})

    result = df.select(
        case(
            branches=[
                (df["x"] == 1, "one"),
                (df["x"] == 2, "two"),
            ],
            else_="other",
        ).alias("result")
    ).collect()

    expected = ["one", "two", "other"]
    assert result.to_pydict()["result"] == expected


def test_case_nested_conditions():
    """Test case with nested logical conditions."""
    df = daft.from_pydict({"a": [1, 2, 3, 4], "b": [10, 20, 30, 40]})

    result = df.select(
        case(
            branches=[
                ((df["a"] > 2) & (df["b"] > 25), "both_high"),
                (df["a"] > 2, "a_high"),
                (df["b"] > 25, "b_high"),
            ],
            else_="both_low",
        ).alias("category")
    ).collect()

    expected = ["both_low", "both_low", "both_high", "both_high"]
    assert result.to_pydict()["category"] == expected


def test_order_matters_first_match_wins():
    """Case statements check conditions in order - first match wins, just like SQL."""
    df = daft.from_pydict({"score": [85, 95]})

    # Deliberately put the broader condition first
    result = df.select(
        case(
            branches=[
                (df["score"] >= 80, "good"),  # This matches 95 first
                (df["score"] >= 90, "excellent"),  # So this never gets checked for 95
            ],
            else_="poor",
        ).alias("grade")
    ).collect()

    # Both 85 and 95 should be "good" because >= 80 matches first
    # This is the same behavior as SQL CASE statements
    expected = ["good", "good"]
    assert result.to_pydict()["grade"] == expected
