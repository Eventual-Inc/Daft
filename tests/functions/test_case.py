"""Tests for the case function."""

import pytest
import daft
from daft.functions import case


def test_simple_case_basic():
    """Test basic simple case functionality."""
    df = daft.from_pydict({"grade": ["A", "B", "C", "D", "F"]})
    result = df.select(
        case(
            df["grade"],
            [
                ("A", 4.0),
                ("B", 3.0),
                ("C", 2.0),
                ("D", 1.0),
            ],
            else_=0.0
        ).alias("gpa")
    ).collect()
    
    expected_gpa = [4.0, 3.0, 2.0, 1.0, 0.0]
    assert result.to_pydict()["gpa"] == expected_gpa


def test_simple_case_without_else():
    """Test simple case without else clause (should return None for unmatched)."""
    df = daft.from_pydict({"letter": ["A", "B", "X"]})
    result = df.select(
        case(
            df["letter"],
            [
                ("A", "first"),
                ("B", "second"),
            ]
        ).alias("position")
    ).collect()
    
    expected = ["first", "second", None]
    assert result.to_pydict()["position"] == expected


def test_searched_case_basic():
    """Test basic searched case functionality."""
    df = daft.from_pydict({"score": [85, 92, 78, 65, 88]})
    result = df.select(
        case(
            branches=[
                (df["score"] >= 90, "A"),
                (df["score"] >= 80, "B"),
                (df["score"] >= 70, "C"),
                (df["score"] >= 60, "D"),
            ],
            else_="F"
        ).alias("grade")
    ).collect()
    
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


def test_case_with_complex_conditions():
    """Test case with complex boolean conditions."""
    df = daft.from_pydict({"age": [15, 25, 35, 70], "income": [0, 30000, 60000, 40000]})
    result = df.select(
        case(
            branches=[
                ((df["age"] >= 18) & (df["age"] < 65) & (df["income"] > 25000), "working_adult"),
                (df["age"] < 18, "minor"),
                (df["age"] >= 65, "senior"),
            ],
            else_="other"
        ).alias("category")
    ).collect()
    
    expected = ["minor", "working_adult", "working_adult", "senior"]
    assert result.to_pydict()["category"] == expected


def test_case_with_null_values():
    """Test case handling null values."""
    df = daft.from_pydict({"value": [1, None, 3, None, 5]})
    result = df.select(
        case(
            branches=[
                (df["value"].is_null(), "missing"),
                (df["value"] > 3, "high"),
                (df["value"] <= 3, "low"),
            ]
        ).alias("status")
    ).collect()
    
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
            else_="no_match"
        ).alias("result")
    ).collect()
    
    # Row 0: x=1, y=1 → x==y (1==1) → "match"
    # Row 1: x=2, y=1 → x==y (2==1) → false, x==2 (2==2) → "two"  
    # Row 2: x=3, y=2 → x==y (3==2) → false, x==2 (3==2) → false → "no_match"
    expected = ["match", "two", "no_match"]
    assert result.to_pydict()["result"] == expected


def test_case_empty_branches_error():
    """Test that case raises error with empty branches."""
    df = daft.from_pydict({"x": [1, 2, 3]})
    
    with pytest.raises(ValueError, match="At least one branch must be provided"):
        case(df["x"], [])


def test_case_programmatic_construction():
    """Test programmatic construction of case branches."""
    df = daft.from_pydict({"category": ["A", "B", "C", "D", "E"]})
    
    # Programmatically build branches
    grade_mapping = {"A": 90, "B": 80, "C": 70, "D": 60}
    branches = [(cat, score) for cat, score in grade_mapping.items()]
    
    result = df.select(
        case(df["category"], branches, else_=0).alias("score")
    ).collect()
    
    expected = [90, 80, 70, 60, 0]
    assert result.to_pydict()["score"] == expected


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
            else_=0
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
            else_="other"
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
            else_="both_low"
        ).alias("category")
    ).collect()
    
    expected = ["both_low", "both_low", "both_high", "both_high"]
    assert result.to_pydict()["category"] == expected


def test_case_order_matters():
    """Test that case evaluates branches in order (first match wins)."""
    df = daft.from_pydict({"score": [85, 95]})
    
    result = df.select(
        case(
            branches=[
                (df["score"] >= 80, "good"),  # This should match first for 95
                (df["score"] >= 90, "excellent"),  # This won't be reached for 95
            ],
            else_="poor"
        ).alias("grade")
    ).collect()
    
    # Both should be "good" because the first condition matches
    expected = ["good", "good"]
    assert result.to_pydict()["grade"] == expected