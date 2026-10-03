from __future__ import annotations

import pytest

import daft
from daft import Session


def _df(name):
    """Create some table whose only column the case-preserved name so we know exactly which was found."""
    return daft.from_pydict({name: []})


def assert_ok(sess: Session, ident: str, find: str):
    """Assert that 'ident' is resolved to 'find'."""
    assert sess.sql(f"SELECT * FROM {ident.strip()}").to_pydict() == {find: []}


def assert_err(sess: Session, ident: str, match: str):
    """Assert that 'ident' fails to resolve with the given message match."""
    with pytest.raises(Exception, match=match):
        sess.sql(f"SELECT * FROM {ident}")


def test_case_sensitive_identifiers():
    sess = Session()

    # unambiguous uppercase name
    sess.create_temp_table("T", _df("T"))

    # ok since regular and delimited are both case-sensitive.
    assert_ok(sess, "  T  ", find="T")
    assert_ok(sess, ' "T" ', find="T")

    # fails since case-sensitive cannot find 'T'
    assert_err(sess, "  t  ", match="Table not found")
    assert_err(sess, ' "t" ', match="Table not found")
    # unambiguous lowercase name
    sess.create_temp_table("s", _df("s"))

    # ok since regular and delimited are both case-sensitive.
    assert_ok(sess, "  s  ", find="s")
    assert_ok(sess, ' "s" ', find="s")

    # fails since case-sensitive cannot find 's'
    assert_err(sess, "  S  ", match="Table not found")
    assert_err(sess, ' "S" ', match="Table not found")

    # possibly ambiguous names depending on mode
    sess.create_temp_table("abc", _df("abc"))
    sess.create_temp_table("ABC", _df("ABC"))
    sess.create_temp_table("aBc", _df("aBc"))

    # delimited identifiers are case-sensitive
    assert_ok(sess, ' "abc" ', find="abc")
    assert_ok(sess, ' "ABC" ', find="ABC")
    assert_ok(sess, ' "aBc" ', find="aBc")

    # regular identifiers are also case-sensitive
    assert_ok(sess, " abc ", find="abc")
    assert_ok(sess, " ABC ", find="ABC")
    assert_ok(sess, " aBc ", find="aBc")


def test_sql_set_identifier_mode_insensitive():
    sess = Session()
    sess.create_temp_table("T", _df("T"))
    assert sess.sql("SET identifier_mode = 'insensitive'") is None
    assert_ok(sess, "t", find="T")


def test_sql_set_identifier_mode_normalized_alias():
    sess = Session()
    sess.create_temp_table("t", _df("t"))
    assert sess.sql("SET identifier_mode = 'normalized'") is None
    # Unquoted identifiers are lowercased, so T resolves to the table stored as t.
    assert_ok(sess, "T", find="t")


@pytest.mark.parametrize(
    "sql",
    [
        "SET identifier_mode = 'insensitive'",
        'SET "identifier_mode" = \'insensitive\'',
        "SET daft.identifier_mode = 'insensitive'",
        'SET "daft"."identifier_mode" = \'insensitive\'',
    ],
)
def test_sql_set_identifier_mode_quoted_and_qualified(sql: str):
    sess = Session()
    sess.create_temp_table("T", _df("T"))
    assert sess.sql(sql) is None
    assert_ok(sess, "t", find="T")


def test_sql_set_quoted_option_with_internal_space_is_unknown():
    sess = Session()
    with pytest.raises(Exception, match=r"Unsupported SQL: 'SET identifier _mode'"):
        sess.sql('SET "identifier _mode" = \'insensitive\'')


def test_sql_set_invalid_identifier_mode():
    sess = Session()
    with pytest.raises(Exception, match=r"Invalid identifier identifier_mode 'x'"):
        sess.sql("SET identifier_mode = 'x'")
