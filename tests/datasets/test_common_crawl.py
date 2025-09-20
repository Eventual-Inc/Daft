from __future__ import annotations

import sys
from unittest.mock import Mock, patch

import pytest

import daft


@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_basic(mock_read_warc):
    mock_df = Mock()
    mock_read_warc.return_value = mock_df

    test_cases = [
        ("CC-MAIN-2023-01", "s3://commoncrawl/crawl-data/CC-MAIN-2023-01/segments/**/warc/*.warc.gz"),
        ("CC-MAIN-2024-50", "s3://commoncrawl/crawl-data/CC-MAIN-2024-50/segments/**/warc/*.warc.gz"),
        ("CC-MAIN-2025-33", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/warc/*.warc.gz"),
    ]

    for crawl_date, expected_glob in test_cases:
        result = daft.datasets.common_crawl(crawl_date)
        mock_read_warc.assert_called_with(expected_glob, io_config=None)
        assert result == mock_df


@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_different_content_types(mock_read_warc):
    mock_df = Mock()
    mock_read_warc.return_value = mock_df

    test_cases = [
        ("raw", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/warc/*.warc.gz"),
        ("text", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/wet/*.warc.wet.gz"),
        ("metadata", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/wat/*.warc.wat.gz"),
        ("warc", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/warc/*.warc.gz"),
        ("wet", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/wet/*.warc.wet.gz"),
        ("wat", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/wat/*.warc.wat.gz"),
    ]

    for content, expected_glob in test_cases:
        mock_read_warc.reset_mock()
        result = daft.datasets.common_crawl("CC-MAIN-2025-33", content=content)
        mock_read_warc.assert_called_with(expected_glob, io_config=None)
        assert result == mock_df


@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_segment_specified(mock_read_warc):
    mock_df = Mock()
    mock_read_warc.return_value = mock_df

    test_cases = [
        ("1234567890.1", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/warc/*.warc.gz"),
        ("2345678901.2", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/2345678901.2/warc/*.warc.gz"),
        ("3456789012.3", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/3456789012.3/warc/*.warc.gz"),
    ]

    for segment, expected_glob in test_cases:
        mock_read_warc.reset_mock()
        result = daft.datasets.common_crawl("CC-MAIN-2025-33", segment=segment)
        mock_read_warc.assert_called_with(expected_glob, io_config=None)
        assert result == mock_df


@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_segment_with_different_content_type(mock_read_warc):
    mock_df = Mock()
    mock_read_warc.return_value = mock_df

    test_cases = [
        ("raw", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/warc/*.warc.gz"),
        ("text", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wet/*.warc.wet.gz"),
        ("metadata", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wat/*.warc.wat.gz"),
        ("warc", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/warc/*.warc.gz"),
        ("wet", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wet/*.warc.wet.gz"),
        ("wat", "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wat/*.warc.wat.gz"),
    ]

    for content, expected_glob in test_cases:
        mock_read_warc.reset_mock()
        result = daft.datasets.common_crawl("CC-MAIN-2025-33", segment="1234567890.1", content=content)
        mock_read_warc.assert_called_with(expected_glob, io_config=None)
        assert result == mock_df


def test_invalid_content_type():
    with pytest.raises(ValueError):
        daft.datasets.common_crawl("CC-MAIN-2025-33", content="invalid")


def test_num_files_zero_raises_error():
    with pytest.raises(ValueError, match="num_files must be a positive integer"):
        daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=0)


def test_num_files_negative_raises_error():
    with pytest.raises(ValueError, match="num_files must be a positive integer"):
        daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=-1)


@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
@patch.object(sys.modules["daft.datasets.common_crawl"], "from_glob_path")
def test_num_files_limit(mock_from_glob_path, mock_read_warc):
    mock_df = Mock()
    mock_select = Mock()
    mock_limit = Mock()
    mock_result_df = Mock()

    mock_from_glob_path.return_value = mock_df
    mock_df.select.return_value = mock_select
    mock_select.limit.return_value = mock_limit
    mock_limit.to_pydict.return_value = {"path": ["file1.warc.gz", "file2.warc.gz", "file3.warc.gz"]}
    mock_read_warc.return_value = mock_result_df

    result = daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=3)

    mock_from_glob_path.assert_called_once_with(
        "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/warc/*.warc.gz", io_config=None
    )
    mock_read_warc.assert_called_once_with(["file1.warc.gz", "file2.warc.gz", "file3.warc.gz"], io_config=None)
    assert result == mock_result_df


@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_io_config_passed_through(mock_read_warc):
    mock_df = Mock()
    mock_read_warc.return_value = mock_df

    io_config = Mock()
    result = daft.datasets.common_crawl("CC-MAIN-2025-33", io_config=io_config)

    mock_read_warc.assert_called_with(
        "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/segments/**/warc/*.warc.gz", io_config=io_config
    )
    assert result == mock_df
