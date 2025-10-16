from __future__ import annotations

import pytest

import daft
from daft.expressions import col, lit
from daft.functions import match
from daft.recordbatch import MicroPartition


class TestMatchBasic:
    """Basic match function tests."""

    def test_match_single_values(self):
        """Test matching single values."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3, 2, 1]})

        result = table.eval_expression_list([match(col("x")).case(1, "a").case(2, "b").otherwise("c").alias("result")])

        assert result.get_column_by_name("result").to_pylist() == ["a", "b", "c", "b", "a"]

    def test_match_tuple_multiple_values(self):
        """Test matching multiple values using tuples."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3, 4, 5, 6]})

        result = table.eval_expression_list(
            [match(col("x")).case((1, 2, 3), "low").case((4, 5), "medium").otherwise("high").alias("result")]
        )

        assert result.get_column_by_name("result").to_pylist() == ["low", "low", "low", "medium", "medium", "high"]

    def test_match_strings(self):
        """Test matching string values."""
        table = MicroPartition.from_pydict({"status": ["active", "inactive", "pending", "active"]})

        result = table.eval_expression_list(
            [match(col("status")).case("active", 1).case("inactive", 0).otherwise(-1).alias("code")]
        )

        assert result.get_column_by_name("code").to_pylist() == [1, 0, -1, 1]

    def test_match_strings_with_tuple(self):
        """Test matching string values with tuples for multiple options."""
        table = MicroPartition.from_pydict({"status": ["active", "inactive", "pending", "unknown", "active"]})

        result = table.eval_expression_list(
            [
                match(col("status"))
                .case("active", 1)
                .case("inactive", 0)
                .case(("pending", "unknown"), -1)
                .otherwise(-99)
                .alias("code")
            ]
        )

        assert result.get_column_by_name("code").to_pylist() == [1, 0, -1, -1, 1]

    def test_match_without_otherwise(self):
        """Test match without otherwise clause returns None for non-matches."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3]})

        # Create the match expression without otherwise - should work as an Expression directly
        match_expr = match(col("x")).case(1, "a").case(2, "b")
        result = table.eval_expression_list([match_expr.alias("result")])

        assert result.get_column_by_name("result").to_pylist() == ["a", "b", None]

    def test_match_without_otherwise_dataframe(self):
        """Test match can be used in DataFrame without otherwise clause."""
        df = daft.from_pydict({"x": [1, 2, 3, 4]})

        df = df.with_column("result", match(df["x"]).case(1, "one").case(2, "two"))

        result = df.to_pydict()
        assert result["result"] == ["one", "two", None, None]


class TestMatchDataTypes:
    """Test match with various data types."""

    @pytest.mark.parametrize(
        "values,match_value,result_value,expected",
        [
            ([1, 2, 3], 2, "matched", [False, True, False]),
            ([1.0, 2.5, 3.5], 2.5, "matched", [False, True, False]),
            ([True, False, True], True, "matched", [True, False, True]),
            (["a", "b", "c"], "b", "matched", [False, True, False]),
        ],
    )
    def test_match_different_types(self, values, match_value, result_value, expected):
        """Test matching with different data types."""
        table = MicroPartition.from_pydict({"x": values})

        result = table.eval_expression_list(
            [match(col("x")).case(match_value, result_value).otherwise("not_matched").alias("result")]
        )

        result_list = result.get_column_by_name("result").to_pylist()
        expected_result = [result_value if e else "not_matched" for e in expected]
        assert result_list == expected_result

    def test_match_with_nulls(self):
        """Test matching with null values."""
        table = MicroPartition.from_pydict({"x": [1, None, 3, None, 1]})

        result = table.eval_expression_list(
            [match(col("x")).case(1, "one").case(3, "three").otherwise("other").alias("result")]
        )

        # None values in the input result in None output (NULL propagation, consistent with SQL CASE)
        assert result.get_column_by_name("result").to_pylist() == ["one", None, "three", None, "one"]


class TestMatchChaining:
    """Test chaining multiple cases."""

    def test_match_many_cases(self):
        """Test matching with many chained cases."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})

        result = table.eval_expression_list(
            [
                match(col("x"))
                .case(1, "one")
                .case(2, "two")
                .case(3, "three")
                .case(4, "four")
                .case(5, "five")
                .otherwise("many")
                .alias("result")
            ]
        )

        assert result.get_column_by_name("result").to_pylist() == [
            "one",
            "two",
            "three",
            "four",
            "five",
            "many",
            "many",
            "many",
            "many",
            "many",
        ]

    def test_match_mixed_singles_and_tuples(self):
        """Test mixing single values and tuples in different cases."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3, 4, 5, 6, 7]})

        result = table.eval_expression_list(
            [
                match(col("x"))
                .case(1, "single")
                .case((2, 3, 4), "tuple")
                .case(5, "another_single")
                .otherwise("rest")
                .alias("result")
            ]
        )

        assert result.get_column_by_name("result").to_pylist() == [
            "single",
            "tuple",
            "tuple",
            "tuple",
            "another_single",
            "rest",
            "rest",
        ]


class TestMatchReturnTypes:
    """Test match with different return value types."""

    def test_match_return_integers(self):
        """Test match returning integer values."""
        table = MicroPartition.from_pydict({"x": ["a", "b", "c"]})

        result = table.eval_expression_list([match(col("x")).case("a", 1).case("b", 2).otherwise(0).alias("result")])

        assert result.get_column_by_name("result").to_pylist() == [1, 2, 0]

    def test_match_return_expressions(self):
        """Test match returning expressions (columns)."""
        table = MicroPartition.from_pydict(
            {"category": ["A", "B", "C"], "val_a": [10, 20, 30], "val_b": [100, 200, 300]}
        )

        result = table.eval_expression_list(
            [match(col("category")).case("A", col("val_a")).case("B", col("val_b")).otherwise(lit(0)).alias("result")]
        )

        assert result.get_column_by_name("result").to_pylist() == [10, 200, 0]

    def test_match_return_boolean(self):
        """Test match returning boolean values."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3, 4, 5]})

        result = table.eval_expression_list([match(col("x")).case((1, 2, 3), True).otherwise(False).alias("result")])

        assert result.get_column_by_name("result").to_pylist() == [True, True, True, False, False]


class TestMatchImmutability:
    """Test that MatchExpr is immutable."""

    def test_case_returns_new_instance(self):
        """Test that calling .case() returns a new instance and doesn't mutate the original."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3, 4]})

        match1 = match(col("x"))
        match2 = match1.case(1, "a")
        match3 = match2.case(2, "b")

        # Verify they are different objects
        assert id(match1) != id(match2)
        assert id(match2) != id(match3)
        assert id(match1) != id(match3)

        # Verify original instances are unchanged by testing their behavior
        # match1 has no cases, so everything should be None
        result1 = table.eval_expression_list([match1.alias("result")])
        assert result1.get_column_by_name("result").to_pylist() == [None, None, None, None]

        # match2 has one case (1 -> "a"), so only 1 matches
        result2 = table.eval_expression_list([match2.otherwise("other").alias("result")])
        assert result2.get_column_by_name("result").to_pylist() == ["a", "other", "other", "other"]

        # match3 has two cases (1 -> "a", 2 -> "b"), so 1 and 2 match
        result3 = table.eval_expression_list([match3.otherwise("other").alias("result")])
        assert result3.get_column_by_name("result").to_pylist() == ["a", "b", "other", "other"]

    def test_independent_usage_after_branching(self):
        """Test that branched match expressions can be used independently."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3, 4]})

        base_match = match(col("x"))
        branch1 = base_match.case(1, "one")
        branch2 = base_match.case(2, "two")

        # Both branches should work independently
        result1 = table.eval_expression_list([branch1.otherwise("other").alias("result")])
        result2 = table.eval_expression_list([branch2.otherwise("other").alias("result")])

        assert result1.get_column_by_name("result").to_pylist() == ["one", "other", "other", "other"]
        assert result2.get_column_by_name("result").to_pylist() == ["other", "two", "other", "other"]

    def test_reusing_intermediate_match(self):
        """Test that intermediate match expressions can be reused multiple times."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3, 4, 5]})

        # Create a base match with one case
        base = match(col("x")).case(1, "one")

        # Create two different branches from the same base
        branch_a = base.case(2, "two").otherwise("other")
        branch_b = base.case((3, 4), "three_or_four").otherwise("other")

        # Both should work correctly
        result_a = table.eval_expression_list([branch_a.alias("result")])
        result_b = table.eval_expression_list([branch_b.alias("result")])

        assert result_a.get_column_by_name("result").to_pylist() == ["one", "two", "other", "other", "other"]
        assert result_b.get_column_by_name("result").to_pylist() == [
            "one",
            "other",
            "three_or_four",
            "three_or_four",
            "other",
        ]


class TestMatchEdgeCases:
    """Test edge cases for match function."""

    def test_match_empty_tuple(self):
        """Test match with empty tuple."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3]})

        # Empty tuple should not match anything
        result = table.eval_expression_list(
            [match(col("x")).case((), "matched").otherwise("not_matched").alias("result")]
        )

        assert result.get_column_by_name("result").to_pylist() == ["not_matched", "not_matched", "not_matched"]

    def test_match_single_element_tuple(self):
        """Test match with single-element tuple."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3]})

        result = table.eval_expression_list(
            [match(col("x")).case((2,), "matched").otherwise("not_matched").alias("result")]
        )

        assert result.get_column_by_name("result").to_pylist() == ["not_matched", "matched", "not_matched"]

    def test_match_duplicate_cases(self):
        """Test match where a value appears in multiple cases (first match wins)."""
        table = MicroPartition.from_pydict({"x": [1, 2, 3]})

        result = table.eval_expression_list(
            [match(col("x")).case(1, "first").case((1, 2), "second").otherwise("other").alias("result")]
        )

        # Value 1 matches the first case, so it should return "first", not "second"
        assert result.get_column_by_name("result").to_pylist() == ["first", "second", "other"]

    def test_match_with_zero(self):
        """Test matching with zero value."""
        table = MicroPartition.from_pydict({"x": [0, 1, 2]})

        result = table.eval_expression_list(
            [match(col("x")).case(0, "zero").case(1, "one").otherwise("other").alias("result")]
        )

        assert result.get_column_by_name("result").to_pylist() == ["zero", "one", "other"]

    def test_match_with_negative_numbers(self):
        """Test matching with negative numbers."""
        table = MicroPartition.from_pydict({"x": [-1, 0, 1]})

        result = table.eval_expression_list(
            [
                match(col("x"))
                .case(-1, "negative")
                .case(0, "zero")
                .case(1, "positive")
                .otherwise("other")
                .alias("result")
            ]
        )

        assert result.get_column_by_name("result").to_pylist() == ["negative", "zero", "positive"]


class TestMatchDataFrame:
    """Test match function used with DataFrame operations."""

    def test_match_with_dataframe(self):
        """Test match function integrated with DataFrame."""
        df = daft.from_pydict({"foo": [1, 2, 3, 2, 1]})

        df = df.with_column("bar", match(df["foo"]).case(1, "a").case(2, "b").otherwise("c"))

        result = df.to_pydict()
        assert result["bar"] == ["a", "b", "c", "b", "a"]

    def test_match_in_select(self):
        """Test match function in select operation."""
        df = daft.from_pydict({"value": [1, 2, 3, 4, 5, 6]})

        df = df.select(
            match(df["value"]).case((1, 2, 3), "low").case((4, 5), "medium").otherwise("high").alias("category")
        )

        result = df.to_pydict()
        assert result["category"] == ["low", "low", "low", "medium", "medium", "high"]

    def test_match_in_filter(self):
        """Test using match result in filter."""
        df = daft.from_pydict({"x": [1, 2, 3, 4, 5]})

        df = df.with_column("category", match(df["x"]).case((1, 2), "small").otherwise("large"))
        df = df.filter(df["category"] == "small")

        result = df.to_pydict()
        assert result["x"] == [1, 2]
        assert result["category"] == ["small", "small"]

    def test_match_multiple_columns(self):
        """Test using match on different columns."""
        df = daft.from_pydict({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        df = df.with_column("result1", match(df["col1"]).case(1, "one").otherwise("other"))
        df = df.with_column("result2", match(df["col2"]).case("a", "alpha").otherwise("other"))

        result = df.to_pydict()
        assert result["result1"] == ["one", "other", "other"]
        assert result["result2"] == ["alpha", "other", "other"]
