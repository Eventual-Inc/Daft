from daft import Identifier


def assert_eq(parsed_id, expect_id):
    assert parsed_id == expect_id


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
