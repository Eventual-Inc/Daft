from __future__ import annotations

import pytest

from daft import Identifier


def assert_eq(parsed_id, expect_id):
    assert parsed_id == expect_id


def test_identifier_regular_single():
    input_sql = "a"
    parsed_id = Identifier.from_sql(input_sql)
    expect_id = Identifier("a")
    assert_eq(parsed_id, expect_id)


def test_identifier_regular_multi():
    input_sql = "a.b"
    parsed_id = Identifier.from_sql(input_sql)
    expect_id = Identifier("a", "b")
    assert_eq(parsed_id, expect_id)


def test_identifier_delimited_single():
    input_sql = """ "a" """
    parsed_id = Identifier.from_sql(input_sql)
    expect_id = Identifier("a")
    assert_eq(parsed_id, expect_id)


def test_identifier_delimited_multi():
    input_sql = """ "a"."b" """
    parsed_id = Identifier.from_sql(input_sql)
    expect_id = Identifier("a", "b")
    assert_eq(parsed_id, expect_id)


def test_identifier_mixed():
    input_sql = """ "a".b."c" """
    parsed_id = Identifier.from_sql(input_sql)
    expect_id = Identifier("a", "b", "c")
    assert_eq(parsed_id, expect_id)


def test_identifier_special():
    input_sql = """ "a.b"."-^-" """
    parsed_id = Identifier.from_sql(input_sql)
    expect_id = Identifier("a.b", "-^-")
    assert_eq(parsed_id, expect_id)


def test_identifier_sequence():
    parts = ["a", "b", "c"]
    ident = Identifier(*parts)
    # __len__
    assert len(parts) == len(ident)
    # __getitem__
    for i in range(len(parts)):
        assert parts[i] == ident[i]
        assert parts[-i] == ident[-i]
    # __iter__ (derived)
    for i, part in enumerate(ident):
        assert parts[i] == part


@pytest.mark.parametrize(
    "input",
    [
        "1",  # can't start with numbers
        "a.",  # invalid: .
        "a b",  # invalid: <space>
        "a-b",  # invalid: -
        "'a'",  # string literal, not an ident
        "`a`",  # backtick non-ANSI.
    ],
)
def test_invalid_identifier(input):
    with pytest.raises(Exception, match="Invalid identifier"):
        Identifier.from_sql(input)


# ensure we can support the customer ask: https://github.com/Eventual-Inc/Daft/issues/3758
def test_identifier_with_periods():
    assert 3 == len(Identifier.from_sql("a.b.c"))
    assert 1 == len(Identifier.from_sql('"a.b.c"'))


def test_identifier_drop():
    ident = Identifier("a", "b", "c")
    assert ident.drop() == Identifier("b", "c")
    assert ident.drop(2) == Identifier("c")
    with pytest.raises(ValueError):
        ident.drop(3)


def test_identifier_concat():
    id1 = Identifier("a")
    id2 = Identifier("b", "c")
    id3 = id1 + id2
    assert len(id3) == 3
    assert id3 == Identifier("a", "b", "c")
