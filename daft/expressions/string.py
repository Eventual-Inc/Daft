from __future__ import annotations

from daft import Expression
from daft.expressions.expressions import ExpressionNamespace


class ExpressionStringNamespace(ExpressionNamespace):
    def contains(self, substr: str | Expression) -> Expression:
        """Checks whether each string contains the given pattern in a string column

        Example:
            >>> col("x").str.contains(col("foo"))

        Args:
            pattern: pattern to search for as a literal string, or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value contains the provided pattern
        """
        substr_expr = Expression._to_expression(substr)
        return Expression._from_pyexpr(self._expr.utf8_contains(substr_expr._expr))

    def match(self, pattern: str | Expression) -> Expression:
        """Checks whether each string matches the given regular expression pattern in a string column

        Example:
            >>> df = daft.from_pydict({"x": ["foo", "bar", "baz"]})
            >>> df.with_column("match", df["x"].str.match("ba.")).collect()
            ╭─────────╮
            │ match   │
            │ ---     │
            │ Boolean │
            ╞═════════╡
            │ false   │
            ├╌╌╌╌╌╌╌╌╌┤
            │ true    │
            ├╌╌╌╌╌╌╌╌╌┤
            │ true    │
            ╰─────────╯

        Args:
            pattern: Regex pattern to search for as string or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value matches the provided pattern
        """
        pattern_expr = Expression._to_expression(pattern)
        return Expression._from_pyexpr(self._expr.utf8_match(pattern_expr._expr))

    def endswith(self, suffix: str | Expression) -> Expression:
        """Checks whether each string ends with the given pattern in a string column

        Example:
            >>> col("x").str.endswith(col("foo"))

        Args:
            pattern: pattern to search for as a literal string, or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value ends with the provided pattern
        """
        suffix_expr = Expression._to_expression(suffix)
        return Expression._from_pyexpr(self._expr.utf8_endswith(suffix_expr._expr))

    def startswith(self, prefix: str | Expression) -> Expression:
        """Checks whether each string starts with the given pattern in a string column

        Example:
            >>> col("x").str.startswith(col("foo"))

        Args:
            pattern: pattern to search for as a literal string, or as a column to pick values from

        Returns:
            Expression: a Boolean expression indicating whether each value starts with the provided pattern
        """
        prefix_expr = Expression._to_expression(prefix)
        return Expression._from_pyexpr(self._expr.utf8_startswith(prefix_expr._expr))

    def split(self, pattern: str | Expression, regex: bool = False) -> Expression:
        r"""Splits each string on the given literal or regex pattern, into a list of strings.

        Example:
            >>> df = daft.from_pydict({"data": ["foo.bar.baz", "a.b.c", "1.2.3"]})
            >>> df.with_column("split", df["data"].str.split(".")).collect()
            ╭─────────────┬─────────────────╮
            │ data        ┆ split           │
            │ ---         ┆ ---             │
            │ Utf8        ┆ List[Utf8]      │
            ╞═════════════╪═════════════════╡
            │ foo.bar.baz ┆ [foo, bar, baz] │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ a.b.c       ┆ [a, b, c]       │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1.2.3       ┆ [1, 2, 3]       │
            ╰─────────────┴─────────────────╯

            Split on a regex pattern

            >>> df = daft.from_pydict({"data": ["foo.bar...baz", "a.....b.c", "1.2...3.."]})
            >>> df.with_column("split", df["data"].str.split(r"\.+", regex=True)).collect()
            ╭───────────────┬─────────────────╮
            │ data          ┆ split           │
            │ ---           ┆ ---             │
            │ Utf8          ┆ List[Utf8]      │
            ╞═══════════════╪═════════════════╡
            │ foo.bar...baz ┆ [foo, bar, baz] │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ a.....b.c     ┆ [a, b, c]       │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 1.2...3..     ┆ [1, 2, 3, ]     │
            ╰───────────────┴─────────────────╯


        Args:
            pattern: The pattern on which each string should be split, or a column to pick such patterns from.
            regex: Whether the pattern is a regular expression. Defaults to False.

        Returns:
            Expression: A List[Utf8] expression containing the string splits for each string in the column.
        """
        pattern_expr = Expression._to_expression(pattern)
        return Expression._from_pyexpr(self._expr.utf8_split(pattern_expr._expr, regex))

    def concat(self, other: str) -> Expression:
        """Concatenates two string expressions together

        .. NOTE::
            Another (easier!) way to invoke this functionality is using the Python `+` operator which is
            aliased to using `.str.concat`. These are equivalent:

            >>> col("x").str.concat(col("y"))
            >>> col("x") + col("y")

        Args:
            other (Expression): a string expression to concatenate with

        Returns:
            Expression: a String expression which is `self` concatenated with `other`
        """
        # Delegate to + operator implementation.
        return Expression._from_pyexpr(self._expr) + other

    def extract(self, pattern: str | Expression, index: int = 0) -> Expression:
        r"""Extracts the specified match group from the first regex match in each string in a string column.

        Notes:
            If index is 0, the entire match is returned.
            If the pattern does not match or the group does not exist, a null value is returned.

        Example:
            >>> regex = r"(\d)(\d*)"
            >>> df = daft.from_pydict({"x": ["123-456", "789-012", "345-678"]})
            >>> df.with_column("match", df["x"].str.extract(regex))
            ╭─────────┬─────────╮
            │ x       ┆ match   │
            │ ---     ┆ ---     │
            │ Utf8    ┆ Utf8    │
            ╞═════════╪═════════╡
            │ 123-456 ┆ 123     │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 789-012 ┆ 789     │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 345-678 ┆ 345     │
            ╰─────────┴─────────╯

            Extract the first capture group

            >>> df.with_column("match", df["x"].str.extract(regex, 1)).collect()
            ╭─────────┬─────────╮
            │ x       ┆ match   │
            │ ---     ┆ ---     │
            │ Utf8    ┆ Utf8    │
            ╞═════════╪═════════╡
            │ 123-456 ┆ 1       │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 789-012 ┆ 7       │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ 345-678 ┆ 3       │
            ╰─────────┴─────────╯

        Args:
            pattern: The regex pattern to extract
            index: The index of the regex match group to extract

        Returns:
            Expression: a String expression with the extracted regex match

        See also:
            `extract_all`
        """
        pattern_expr = Expression._to_expression(pattern)
        return Expression._from_pyexpr(self._expr.utf8_extract(pattern_expr._expr, index))

    def extract_all(self, pattern: str | Expression, index: int = 0) -> Expression:
        r"""Extracts the specified match group from all regex matches in each string in a string column.

        Notes:
            This expression always returns a list of strings.
            If index is 0, the entire match is returned. If the pattern does not match or the group does not exist, an empty list is returned.

        Example:
            >>> regex = r"(\d)(\d*)"
            >>> df = daft.from_pydict({"x": ["123-456", "789-012", "345-678"]})
            >>> df.with_column("match", df["x"].str.extract_all(regex))
            ╭─────────┬────────────╮
            │ x       ┆ matches    │
            │ ---     ┆ ---        │
            │ Utf8    ┆ List[Utf8] │
            ╞═════════╪════════════╡
            │ 123-456 ┆ [123, 456] │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 789-012 ┆ [789, 012] │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 345-678 ┆ [345, 678] │
            ╰─────────┴────────────╯

            Extract the first capture group

            >>> df.with_column("match", df["x"].str.extract_all(regex, 1)).collect()
            ╭─────────┬────────────╮
            │ x       ┆ matches    │
            │ ---     ┆ ---        │
            │ Utf8    ┆ List[Utf8] │
            ╞═════════╪════════════╡
            │ 123-456 ┆ [1, 4]     │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 789-012 ┆ [7, 0]     │
            ├╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
            │ 345-678 ┆ [3, 6]     │
            ╰─────────┴────────────╯

        Args:
            pattern: The regex pattern to extract
            index: The index of the regex match group to extract

        Returns:
            Expression: a List[Utf8] expression with the extracted regex matches

        See also:
            `extract`
        """
        pattern_expr = Expression._to_expression(pattern)
        return Expression._from_pyexpr(self._expr.utf8_extract_all(pattern_expr._expr, index))

    def replace(self, pattern: str | Expression, replacement: str | Expression, regex: bool = False) -> Expression:
        """Replaces all occurrences of a pattern in a string column with a replacement string. The pattern can be a literal string or a regex pattern.

        Example:
            >>> df = daft.from_pydict({"data": ["foo", "bar", "baz"]})
            >>> df.with_column("replace", df["data"].str.replace("ba", "123")).collect()
            ╭──────┬─────────╮
            │ data ┆ replace │
            │ ---  ┆ ---     │
            │ Utf8 ┆ Utf8    │
            ╞══════╪═════════╡
            │ foo  ┆ foo     │
            ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ bar  ┆ 123r    │
            ├╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ baz  ┆ 123z    │
            ╰──────┴─────────╯

            Replace with a regex pattern

            >>> df = daft.from_pydict({"data": ["foo", "fooo", "foooo"]})
            >>> df.with_column("replace", df["data"].str.replace(r"o+", "a", regex=True)).collect()
            ╭───────┬─────────╮
            │ data  ┆ replace │
            │ ---   ┆ ---     │
            │ Utf8  ┆ Utf8    │
            ╞═══════╪═════════╡
            │ foo   ┆ fa      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ fooo  ┆ fa      │
            ├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
            │ foooo ┆ fa      │
            ╰───────┴─────────╯

        Args:
            pattern: The pattern to replace
            replacement: The replacement string
            regex: Whether the pattern is a regex pattern or an exact match. Defaults to False.

        Returns:
            Expression: a String expression with patterns replaced by the replacement string
        """
        pattern_expr = Expression._to_expression(pattern)
        replacement_expr = Expression._to_expression(replacement)
        return Expression._from_pyexpr(self._expr.utf8_replace(pattern_expr._expr, replacement_expr._expr, regex))

    def length(self) -> Expression:
        """Retrieves the length for a UTF-8 string column

        Example:
            >>> col("x").str.length()

        Returns:
            Expression: an UInt64 expression with the length of each string
        """
        return Expression._from_pyexpr(self._expr.utf8_length())

    def lower(self) -> Expression:
        """Convert UTF-8 string to all lowercase

        Example:
            >>> col("x").str.lower()

        Returns:
            Expression: a String expression which is `self` lowercased
        """
        return Expression._from_pyexpr(self._expr.utf8_lower())

    def upper(self) -> Expression:
        """Convert UTF-8 string to all upper

        Example:
            >>> col("x").str.upper()

        Returns:
            Expression: a String expression which is `self` uppercased
        """
        return Expression._from_pyexpr(self._expr.utf8_upper())

    def lstrip(self) -> Expression:
        """Strip whitespace from the left side of a UTF-8 string

        Example:
            >>> col("x").str.lstrip()

        Returns:
            Expression: a String expression which is `self` with leading whitespace stripped
        """
        return Expression._from_pyexpr(self._expr.utf8_lstrip())

    def rstrip(self) -> Expression:
        """Strip whitespace from the right side of a UTF-8 string

        Example:
            >>> col("x").str.rstrip()

        Returns:
            Expression: a String expression which is `self` with trailing whitespace stripped
        """
        return Expression._from_pyexpr(self._expr.utf8_rstrip())

    def reverse(self) -> Expression:
        """Reverse a UTF-8 string

        Example:
            >>> col("x").str.reverse()

        Returns:
            Expression: a String expression which is `self` reversed
        """
        return Expression._from_pyexpr(self._expr.utf8_reverse())

    def capitalize(self) -> Expression:
        """Capitalize a UTF-8 string

        Example:
            >>> col("x").str.capitalize()

        Returns:
            Expression: a String expression which is `self` uppercased with the first character and lowercased the rest
        """
        return Expression._from_pyexpr(self._expr.utf8_capitalize())

    def left(self, nchars: int | Expression) -> Expression:
        """Gets the n (from nchars) left-most characters of each string

        Example:
            >>> col("x").str.left(3)

        Returns:
            Expression: a String expression which is the `n` left-most characters of `self`
        """
        nchars_expr = Expression._to_expression(nchars)
        return Expression._from_pyexpr(self._expr.utf8_left(nchars_expr._expr))

    def right(self, nchars: int | Expression) -> Expression:
        """Gets the n (from nchars) right-most characters of each string

        Example:
            >>> col("x").str.right(3)

        Returns:
            Expression: a String expression which is the `n` right-most characters of `self`
        """
        nchars_expr = Expression._to_expression(nchars)
        return Expression._from_pyexpr(self._expr.utf8_right(nchars_expr._expr))

    def find(self, substr: str | Expression) -> Expression:
        """Returns the index of the first occurrence of the substring in each string

        .. NOTE::
            The returned index is 0-based.
            If the substring is not found, -1 is returned.

        Example:
            >>> col("x").str.find("foo")

        Returns:
            Expression: an Int64 expression with the index of the first occurrence of the substring in each string
        """
        substr_expr = Expression._to_expression(substr)
        return Expression._from_pyexpr(self._expr.utf8_find(substr_expr._expr))
