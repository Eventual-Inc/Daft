from __future__ import annotations

import pytest

import daft
from daft.io.mcap._pushdown import UINT64_MAX, explicit_mcap_filter, extract_mcap_filter


@pytest.mark.parametrize(
    ("expression", "start", "end"),
    [
        (daft.col("log_time") >= 10, 10, None),
        (daft.col("log_time") > 10, 11, None),
        (daft.col("log_time") < 10, None, 10),
        (daft.col("log_time") <= 10, None, 11),
        (daft.col("log_time") == 10, 10, 11),
        (10 <= daft.col("log_time"), 10, None),
        (10 > daft.col("log_time"), None, 10),
        (daft.col("log_time").between(10, 20), 10, 21),
        (daft.col("log_time") == UINT64_MAX, UINT64_MAX, None),
    ],
)
def test_extract_log_time_bounds(expression, start, end):
    filters = extract_mcap_filter(expression)
    assert filters.start_time == start
    assert filters.end_time == end
    assert filters.exact is True


def test_extract_topic_constraints():
    equal = extract_mcap_filter(daft.col("topic") == "/camera")
    reverse_equal = extract_mcap_filter(daft.lit("/camera") == daft.col("topic"))
    in_list = extract_mcap_filter(daft.col("topic").is_in(["/camera", "/imu"]))

    assert equal.topics == frozenset({"/camera"})
    assert reverse_equal == equal
    assert in_list.topics == frozenset({"/camera", "/imu"})


def test_and_keeps_safe_constraints_but_marks_unsupported_residual():
    filters = extract_mcap_filter((daft.col("log_time") >= 10) & (daft.col("sequence") > 2))

    assert filters.start_time == 10
    assert filters.exact is False


def test_or_and_not_are_not_pushed():
    disjunction = extract_mcap_filter((daft.col("topic") == "/camera") | (daft.col("log_time") > 10))
    negation = extract_mcap_filter(~(daft.col("topic") == "/camera"))

    assert disjunction.topics is None and disjunction.start_time is None and disjunction.exact is False
    assert negation.topics is None and negation.exact is False


def test_contradictions_short_circuit():
    time = extract_mcap_filter((daft.col("log_time") >= 10) & (daft.col("log_time") < 10))
    topics = extract_mcap_filter((daft.col("topic") == "/camera") & (daft.col("topic") == "/imu"))

    assert time.empty is True
    assert topics.empty is True


@pytest.mark.parametrize(
    "kwargs",
    [
        {"topics": None, "start_time": -1, "end_time": None},
        {"topics": None, "start_time": True, "end_time": None},
        {"topics": ["/camera", 1], "start_time": None, "end_time": None},
    ],
)
def test_explicit_filter_validation(kwargs):
    with pytest.raises(ValueError):
        explicit_mcap_filter(**kwargs)
