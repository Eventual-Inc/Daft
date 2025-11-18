from __future__ import annotations

import daft
from daft import col


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
            daft.functions.endswith(col("a"), "a").alias("ends_with_a"),
            daft.functions.startswith(col("a"), "a").alias("starts_with_a"),
            daft.functions.contains(col("a"), "a").alias("contains_a"),
            daft.functions.split(col("a"), " ").alias("split_a"),
            daft.functions.regexp(col("a"), "ba.").alias("match_a"),
            daft.functions.regexp_extract(col("a"), "ba.").alias("extract_a"),
            daft.functions.regexp_extract_all(col("a"), "ba.").alias("extract_all_a"),
            daft.functions.regexp_split(col("a"), r"\s+").alias("regexp_split_a"),
            daft.functions.regexp_replace(col("a"), "ba.", "foo").alias("replace_a"),
            daft.functions.length(col("a")).alias("length_a"),
            daft.functions.length_bytes(col("a")).alias("length_bytes_a"),
            daft.functions.lower(col("a")).alias("lower_a"),
            daft.functions.lstrip(col("a")).alias("lstrip_a"),
            daft.functions.rstrip(col("a")).alias("rstrip_a"),
            daft.functions.reverse(col("a")).alias("reverse_a"),
            daft.functions.capitalize(col("a")).alias("capitalize_a"),
            daft.functions.left(col("a"), 4).alias("left_a"),
            daft.functions.right(col("a"), 4).alias("right_a"),
            daft.functions.find(col("a"), "a").alias("find_a"),
            daft.functions.rpad(col("a"), 10, "<").alias("rpad_a"),
            daft.functions.lpad(col("a"), 10, ">").alias("lpad_a"),
            daft.functions.repeat(col("a"), 2).alias("repeat_a"),
            daft.functions.like(col("a"), "a%").alias("like_a"),
            daft.functions.ilike(col("a"), "a%").alias("ilike_a"),
            daft.functions.substr(col("a"), 1, 3).alias("substring_a"),
            daft.functions.count_matches(col("a"), "a").alias("count_matches_a_0"),
            daft.functions.count_matches(col("a"), "a", case_sensitive=True).alias("count_matches_a_1"),
            daft.functions.count_matches(col("a"), "a", case_sensitive=False, whole_words=False).alias(
                "count_matches_a_2"
            ),
            daft.functions.count_matches(col("a"), "a", case_sensitive=True, whole_words=True).alias(
                "count_matches_a_3"
            ),
            daft.functions.normalize(col("a")).alias("normalize_a"),
            daft.functions.normalize(col("a"), remove_punct=True).alias("normalize_remove_punct_a"),
            daft.functions.normalize(col("a"), remove_punct=True, lowercase=True).alias(
                "normalize_remove_punct_lower_a"
            ),
            daft.functions.normalize(col("a"), remove_punct=True, lowercase=True, white_space=True).alias(
                "normalize_remove_punct_lower_ws_a"
            ),
            daft.functions.tokenize_encode(col("a"), "r50k_base").alias("tokenize_encode_a"),
            daft.functions.tokenize_decode(daft.functions.tokenize_encode(col("a"), "r50k_base"), "r50k_base").alias(
                "tokenize_decode_a"
            ),
            daft.functions.concat(col("a"), "---").alias("concat_a"),
            daft.functions.concat(
                daft.functions.concat(
                    daft.functions.concat(daft.functions.concat(daft.lit("--"), col("a")), col("a")), col("a")
                ),
                "--",
            ).alias("concat_multi_a"),
        )
        .collect()
        .to_pydict()
    )
    actual = actual.to_pydict()
    assert actual == expected
