from __future__ import annotations

import pytest

import daft
from daft import col, lit


def test_utf8_exprs():
    df = daft.from_pydict(
        {
            "a": [
                "a",
                "df_daft",
                "foo",
                "bar",
                "baz",
                "lorém",
                "ipsum",
                "dolor",
                "sit",
                "amet",
                "😊",
                "🌟",
                "🎉",
                "This is a longer with some words",
                "THIS is ",
                "",
            ],
        }
    )

    sql = """
    SELECT
        ends_with(a, 'a') as ends_with_a,
        starts_with(a, 'a') as starts_with_a,
        contains(a, 'a') as contains_a,
        split(a, ' ') as split_a,
        regexp_match(a, 'ba.') as match_a,
        regexp_extract(a, 'ba.') as extract_a,
        regexp_extract_all(a, 'ba.') as extract_all_a,
        regexp_replace(a, 'ba.', 'foo') as replace_a,
        regexp_split(a, '\\s+') as regexp_split_a,
        length(a) as length_a,
        length_bytes(a) as length_bytes_a,
        lower(a) as lower_a,
        lstrip(a) as lstrip_a,
        rstrip(a) as rstrip_a,
        strip(a) as strip_a,
        reverse(a) as reverse_a,
        capitalize(a) as capitalize_a,
        left(a, 4) as left_a,
        right(a, 4) as right_a,
        find(a, 'a') as find_a,
        rpad(a, 10, '<') as rpad_a,
        lpad(a, 10, '>') as lpad_a,
        repeat(a, 2) as repeat_a,
        a like 'a%' as like_a,
        a ilike 'a%' as ilike_a,
        substring(a, 2, 3) as substring_a,
        count_matches(a, 'a') as count_matches_a_0,
        count_matches(a, 'a', case_sensitive := true) as count_matches_a_1,
        count_matches(a, 'a', case_sensitive := false, whole_words := false) as count_matches_a_2,
        count_matches(a, 'a', case_sensitive := true, whole_words := true) as count_matches_a_3,
        normalize(a) as normalize_a,
        normalize(a, remove_punct:=true) as normalize_remove_punct_a,
        normalize(a, remove_punct:=true, lowercase:=true) as normalize_remove_punct_lower_a,
        normalize(a, remove_punct:=true, lowercase:=true, white_space:=true) as normalize_remove_punct_lower_ws_a,
        tokenize_encode(a, 'r50k_base') as tokenize_encode_a,
        tokenize_decode(tokenize_encode(a, 'r50k_base'), 'r50k_base') as tokenize_decode_a,
        concat(a, '---') as concat_a,
        concat('--', a, a, a, '--') as concat_multi_a
    FROM df
    """
    actual = daft.sql(sql).collect()
    expected = (
        df.select(
            col("a").endswith("a").alias("ends_with_a"),
            col("a").startswith("a").alias("starts_with_a"),
            col("a").contains("a").alias("contains_a"),
            col("a").split(" ").alias("split_a"),
            col("a").regexp("ba.").alias("match_a"),
            col("a").regexp_extract("ba.").alias("extract_a"),
            col("a").regexp_extract_all("ba.").alias("extract_all_a"),
            col("a").regexp_split(r"\s+").alias("regexp_split_a"),
            col("a").regexp_replace("ba.", "foo").alias("replace_a"),
            col("a").length().alias("length_a"),
            col("a").length_bytes().alias("length_bytes_a"),
            col("a").lower().alias("lower_a"),
            col("a").lstrip().alias("lstrip_a"),
            col("a").rstrip().alias("rstrip_a"),
            col("a").strip().alias("strip_a"),
            col("a").reverse().alias("reverse_a"),
            col("a").capitalize().alias("capitalize_a"),
            col("a").left(4).alias("left_a"),
            col("a").right(4).alias("right_a"),
            col("a").find("a").alias("find_a"),
            col("a").rpad(10, "<").alias("rpad_a"),
            col("a").lpad(10, ">").alias("lpad_a"),
            col("a").repeat(2).alias("repeat_a"),
            col("a").like("a%").alias("like_a"),
            col("a").ilike("a%").alias("ilike_a"),
            col("a").substr(1, 3).alias("substring_a"),
            col("a").count_matches("a").alias("count_matches_a_0"),
            col("a").count_matches("a", case_sensitive=True).alias("count_matches_a_1"),
            col("a").count_matches("a", case_sensitive=False, whole_words=False).alias("count_matches_a_2"),
            col("a").count_matches("a", case_sensitive=True, whole_words=True).alias("count_matches_a_3"),
            col("a").normalize().alias("normalize_a"),
            col("a").normalize(remove_punct=True).alias("normalize_remove_punct_a"),
            col("a").normalize(remove_punct=True, lowercase=True).alias("normalize_remove_punct_lower_a"),
            col("a")
            .normalize(remove_punct=True, lowercase=True, white_space=True)
            .alias("normalize_remove_punct_lower_ws_a"),
            col("a").tokenize_encode("r50k_base").alias("tokenize_encode_a"),
            col("a").tokenize_encode("r50k_base").tokenize_decode("r50k_base").alias("tokenize_decode_a"),
            col("a").concat("---").alias("concat_a"),
            lit("--").concat(col("a")).concat(col("a")).concat(col("a")).concat("--").alias("concat_multi_a"),
        )
        .collect()
        .to_pydict()
    )
    actual = actual.to_pydict()
    assert actual == expected


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("substring(s FROM 2 FOR 3)", ["ell", "aft", "oré", None]),
        ("substring(s FROM 2)", ["ello", "aft", "orém", None]),
        ("substring(s FOR 4)", ["hell", "daft", "loré", None]),
        ("substring(s, 2, 3)", ["ell", "aft", "oré", None]),
        ("substring(s, 2)", ["ello", "aft", "orém", None]),
    ],
)
def test_substring_syntax(expr, expected):
    df = daft.from_pydict({"s": ["hello", "daft", "lorém", None]})
    actual = daft.sql(f"SELECT {expr} AS out FROM df", df=df).to_pydict()
    assert actual == {"out": expected}


def test_substring_from_for_columns():
    df = daft.from_pydict({"s": ["hello", "daft", "lorém"], "start": [1, 2, 3], "len": [2, 1, 3]})
    actual = daft.sql("SELECT substring(s FROM start FOR len) AS out FROM df", df=df).to_pydict()
    assert actual == {"out": ["he", "a", "rém"]}


def test_substring_ansi_matches_comma_syntax():
    df = daft.from_pydict({"s": ["hello", "daft", "lorém", None]})
    actual = daft.sql(
        "SELECT substring(s FROM 2 FOR 2) AS ansi, substring(s, 2, 2) AS comma FROM df",
        df=df,
    ).to_pydict()
    assert actual["ansi"] == actual["comma"]


def test_substring_without_start_or_length():
    df = daft.from_pydict({"s": ["hello"]})
    with pytest.raises(Exception, match="SUBSTRING requires a start position or a length"):
        daft.sql("SELECT substring(s) FROM df", df=df).collect()
