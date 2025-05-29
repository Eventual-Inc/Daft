from __future__ import annotations

from datetime import timedelta


def test_total_seconds(test_expression):
    test_data = [
        timedelta(seconds=1),
        timedelta(milliseconds=1),
        timedelta(microseconds=1),
        timedelta(days=1),
        timedelta(hours=1),
        timedelta(minutes=1),
    ]
    expected = [1, 0, 0, 86400, 3600, 60]
    test_expression(data=test_data, expected=expected, name="total_seconds", sql_name="total_seconds", namespace="dt")


def test_total_milliseconds(test_expression):
    test_data = [
        timedelta(seconds=1),
        timedelta(milliseconds=1),
        timedelta(microseconds=1),
        timedelta(days=1),
        timedelta(hours=1),
        timedelta(minutes=1),
    ]
    expected = [1000, 1, 0, 86400000, 3600000, 60000]
    test_expression(
        data=test_data, expected=expected, name="total_milliseconds", sql_name="total_milliseconds", namespace="dt"
    )


def test_total_microseconds(test_expression):
    test_data = [
        timedelta(seconds=1),
        timedelta(milliseconds=1),
        timedelta(microseconds=1),
        timedelta(days=1),
        timedelta(hours=1),
        timedelta(minutes=1),
    ]
    expected = [1000000, 1000, 1, 86400000000, 3600000000, 60000000]
    test_expression(
        data=test_data, expected=expected, name="total_microseconds", sql_name="total_microseconds", namespace="dt"
    )


def test_total_nanoseconds(test_expression):
    test_data = [
        timedelta(seconds=1),
        timedelta(milliseconds=1),
        timedelta(microseconds=1),
        timedelta(days=1),
        timedelta(hours=1),
        timedelta(minutes=1),
    ]
    expected = [1000000000, 1000000, 1000, 86400000000000, 3600000000000, 60000000000]
    test_expression(
        data=test_data, expected=expected, name="total_nanoseconds", sql_name="total_nanoseconds", namespace="dt"
    )


def test_total_days(test_expression):
    test_data = [
        timedelta(seconds=1),
        timedelta(milliseconds=1),
        timedelta(microseconds=1),
        timedelta(days=1),
        timedelta(hours=1),
        timedelta(minutes=1),
    ]
    expected = [0, 0, 0, 1, 0, 0]
    test_expression(data=test_data, expected=expected, name="total_days", sql_name="total_days", namespace="dt")


def test_total_hours(test_expression):
    test_data = [
        timedelta(seconds=1),
        timedelta(milliseconds=1),
        timedelta(microseconds=1),
        timedelta(days=1),
        timedelta(hours=1),
        timedelta(minutes=1),
    ]
    expected = [0, 0, 0, 24, 1, 0]
    test_expression(data=test_data, expected=expected, name="total_hours", sql_name="total_hours", namespace="dt")


def test_total_minutes(test_expression):
    test_data = [
        timedelta(seconds=1),
        timedelta(milliseconds=1),
        timedelta(microseconds=1),
        timedelta(days=1),
        timedelta(hours=1),
        timedelta(minutes=1),
    ]
    expected = [0, 0, 0, 1440, 60, 1]
    test_expression(data=test_data, expected=expected, name="total_minutes", sql_name="total_minutes", namespace="dt")
