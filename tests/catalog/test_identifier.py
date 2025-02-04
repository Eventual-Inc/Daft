from daft import Identifier


def assert_eq(parsed_id, expect_id):
    # TODO equality
    assert parsed_id.__repr__() == expect_id.__repr__()


def test_identifier_regular_single():
    input_sql = "a"
    parsed_id = Identifier.parse(input_sql)
    expect_id = Identifier("a")
    # TODO equality
    assert_eq(parsed_id, expect_id)


def test_identifier_regular_multi():
    input_sql = "a.b"
    parsed_id = Identifier.parse(input_sql)
    expect_id = Identifier("a", "b")
    # TODO equality
    assert_eq(parsed_id, expect_id)


def test_identifier_delimited_single():
    input_sql = """ "a" """
    parsed_id = Identifier.parse(input_sql)
    expect_id = Identifier("a")
    # TODO equality
    assert_eq(parsed_id, expect_id)


def test_identifier_delimited_multi():
    input_sql = """ "a"."b" """
    parsed_id = Identifier.parse(input_sql)
    expect_id = Identifier("a", "b")
    # TODO equality
    assert_eq(parsed_id, expect_id)


def test_identifier_mixed():
    input_sql = """ "a".b."c" """
    parsed_id = Identifier.parse(input_sql)
    expect_id = Identifier("a", "b", "c")
    # TODO equality
    assert_eq(parsed_id, expect_id)


def test_identifier_special():
    input_sql = """ "a.b"."-^-" """
    parsed_id = Identifier.parse(input_sql)
    expect_id = Identifier("a.b", "-^-")
    # TODO equality
    assert_eq(parsed_id, expect_id)
