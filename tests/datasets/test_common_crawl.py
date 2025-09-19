from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from daft.datasets import common_crawl


def test_basic():
    test_cases = [
        ("CC-MAIN-2023-01", "s3://commoncrawl/crawl-data/CC-MAIN-2023-01/segments/**/warc/*.warc.gz"),
        ("CC-MAIN-2024-50", "s3://commoncrawl/crawl-data/CC-MAIN-2024-50/segments/**/warc/*.warc.gz"),
        ("CC-MAIN-2025-33", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/warc/*.warc.gz"),
    ]

    for crawl_date, expected in test_cases:
        result = common_crawl(crawl_date)
        assert result == expected


def test_different_content_types():
    test_cases = [
        ("raw", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/warc/*.warc.gz"),
        ("text", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/wet/*.warc.wet.gz"),
        ("metadata", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/wat/*.warc.wat.gz"),
        ("warc", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/warc/*.warc.gz"),
        ("wet", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/wet/*.warc.wet.gz"),
        ("wat", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/wat/*.warc.wat.gz"),
    ]
    for content, expected in test_cases:
        result = common_crawl("CC-MAIN-2025-33", content=content)
        assert result == expected


def test_segment_specified():
    test_cases = [
        ("1234567890.1", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/warc/*.warc.gz"),
        ("2345678901.2", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/2345678901.2/warc/*.warc.gz"),
        ("3456789012.3", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/3456789012.3/warc/*.warc.gz"),
    ]

    for segment, expected in test_cases:
        result = common_crawl("CC-MAIN-2025-33", segment=segment)
        assert result == expected


def test_segment_with_different_content_type():
    test_cases = [
        ("raw", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/warc/*.warc.gz"),
        ("text", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wet/*.warc.wet.gz"),
        ("metadata", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wat/*.warc.wat.gz"),
        ("warc", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/warc/*.warc.gz"),
        ("wet", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wet/*.warc.wet.gz"),
        ("wat", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wat/*.warc.wat.gz"),
    ]

    for content, expected in test_cases:
        result = common_crawl("CC-MAIN-2025-33", segment="1234567890.1", content=content)
        assert result == expected


def test_invalid_content_type():
    with pytest.raises(ValueError):
        common_crawl("CC-MAIN-2025-33", content="invalid")


def test_num_files_zero_raises_error():
    with pytest.raises(ValueError, match="num_files must be a positive integer"):
        common_crawl("CC-MAIN-2025-33", num_files=0)


def test_num_files_negative_raises_error():
    with pytest.raises(ValueError, match="num_files must be a positive integer"):
        common_crawl("CC-MAIN-2025-33", num_files=-1)


@patch("daft.datasets.common_crawl.from_glob_path")
def test_num_files_limit(mock_from_glob_path):
    mock_df = Mock()
    mock_select = Mock()
    mock_limit = Mock()

    mock_from_glob_path.return_value = mock_df
    mock_df.select.return_value = mock_select
    mock_select.limit.return_value = mock_limit
    mock_limit.to_pydict.return_value = {"path": ["file1.warc.gz", "file2.warc.gz", "file3.warc.gz"]}

    result = common_crawl("CC-MAIN-2025-33", num_files=3)

    mock_from_glob_path.assert_called_once_with(
        "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/warc/*.warc.gz", io_config=None
    )

    assert result == ["file1.warc.gz", "file2.warc.gz", "file3.warc.gz"]
