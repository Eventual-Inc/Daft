from __future__ import annotations

import gzip
import sys
from unittest.mock import Mock, patch

import pytest

import daft
from daft.datasets.common_crawl import _get_http_manifest_path, _get_s3_manifest_path

WARC_PATHS = [
    "crawl-data/CC-MAIN-2025-33/segments/1234567890.1/warc/CC-MAIN-20250801120000-20250801150000-00001.warc.gz",
    "crawl-data/CC-MAIN-2025-33/segments/1234567890.1/warc/CC-MAIN-20250801120000-20250801150000-00002.warc.gz",
    "crawl-data/CC-MAIN-2025-33/segments/1234567890.1/warc/CC-MAIN-20250801120000-20250801150000-00003.warc.gz",
    "crawl-data/CC-MAIN-2025-33/segments/2345678901.2/warc/CC-MAIN-20250801160000-20250801190000-00001.warc.gz",
    "crawl-data/CC-MAIN-2025-33/segments/2345678901.2/warc/CC-MAIN-20250801160000-20250801190000-00002.warc.gz",
    "crawl-data/CC-MAIN-2025-33/segments/3456789012.3/warc/CC-MAIN-20250801200000-20250801230000-00001.warc.gz",
]

WET_PATHS = [
    "crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wet/CC-MAIN-20250801120000-20250801150000-00001.warc.wet.gz",
    "crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wet/CC-MAIN-20250801120000-20250801150000-00002.warc.wet.gz",
    "crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wet/CC-MAIN-20250801120000-20250801150000-00003.warc.wet.gz",
    "crawl-data/CC-MAIN-2025-33/segments/2345678901.2/wet/CC-MAIN-20250801160000-20250801190000-00001.warc.wet.gz",
    "crawl-data/CC-MAIN-2025-33/segments/2345678901.2/wet/CC-MAIN-20250801160000-20250801190000-00002.warc.wet.gz",
    "crawl-data/CC-MAIN-2025-33/segments/3456789012.3/wet/CC-MAIN-20250801200000-20250801230000-00001.warc.wet.gz",
]


WAT_PATHS = [
    "crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wat/CC-MAIN-20250801120000-20250801150000-00001.warc.wat.gz",
    "crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wat/CC-MAIN-20250801120000-20250801150000-00002.warc.wat.gz",
    "crawl-data/CC-MAIN-2025-33/segments/1234567890.1/wat/CC-MAIN-20250801120000-20250801150000-00003.warc.wat.gz",
    "crawl-data/CC-MAIN-2025-33/segments/2345678901.2/wat/CC-MAIN-20250801160000-20250801190000-00001.warc.wat.gz",
    "crawl-data/CC-MAIN-2025-33/segments/2345678901.2/wat/CC-MAIN-20250801160000-20250801190000-00002.warc.wat.gz",
    "crawl-data/CC-MAIN-2025-33/segments/3456789012.3/wat/CC-MAIN-20250801200000-20250801230000-00001.warc.wat.gz",
]


@pytest.fixture
def fake_manifest_files(tmp_path):
    """Create fake .paths.gz manifest files programmatically."""
    # Common Crawl manifest files are gzipped.
    manifest_files = {}
    for file_type, paths in [("warc", WARC_PATHS), ("wet", WET_PATHS), ("wat", WAT_PATHS)]:
        manifest_path = tmp_path / f"{file_type}.paths.gz"
        with gzip.open(manifest_path, "wt") as f:
            f.write("\n".join(paths))
        manifest_files[file_type] = manifest_path

    return manifest_files


@pytest.fixture
def mock_manifest_path(fake_manifest_files) -> str:
    """Helper fixture to mock the manifest path function."""

    def _mock_manifest_path(crawl, file_type) -> str:
        return f"file://{fake_manifest_files[file_type]}"

    return _mock_manifest_path


@pytest.mark.parametrize("in_aws", [True, False])
@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_basic(mock_read_warc, mock_manifest_path, in_aws):
    mock_read_warc.return_value = Mock()

    manifest_func = "_get_s3_manifest_path" if in_aws else "_get_http_manifest_path"
    with patch.object(sys.modules["daft.datasets.common_crawl"], manifest_func) as mock_get_manifest_path:
        mock_get_manifest_path.side_effect = mock_manifest_path

        daft.datasets.common_crawl("CC-MAIN-2025-33", in_aws=in_aws)

    # Verify read_warc was called with the expected paths from our fixture.
    mock_read_warc.assert_called_once()
    args = mock_read_warc.call_args[0][0]

    assert len(args) == len(WARC_PATHS)
    expected_prefix = "s3://commoncrawl/" if in_aws else "https://data.commoncrawl.org/"
    for path in WARC_PATHS:
        assert f"{expected_prefix}{path}" in args


@pytest.mark.parametrize("in_aws", [True, False])
@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_different_content_types(mock_read_warc, mock_manifest_path, in_aws):
    test_cases = [
        ("raw", "warc"),
        ("text", "wet"),
        ("metadata", "wat"),
        ("warc", "warc"),
        ("wet", "wet"),
        ("wat", "wat"),
    ]

    manifest_func = "_get_s3_manifest_path" if in_aws else "_get_http_manifest_path"

    for content, expected_file_type in test_cases:
        mock_read_warc.return_value = Mock()

        with patch.object(sys.modules["daft.datasets.common_crawl"], manifest_func) as mock_get_manifest_path:
            mock_get_manifest_path.side_effect = mock_manifest_path

            daft.datasets.common_crawl("CC-MAIN-2025-33", content=content, in_aws=in_aws)

            mock_get_manifest_path.assert_called_with("CC-MAIN-2025-33", expected_file_type)

        args = mock_read_warc.call_args[0][0]

        for path in args:
            if expected_file_type == "warc":
                assert path.endswith(".warc.gz")
            elif expected_file_type == "wet":
                assert path.endswith(".warc.wet.gz")
            elif expected_file_type == "wat":
                assert path.endswith(".warc.wat.gz")


@pytest.mark.parametrize("in_aws", [True, False])
@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_segment_filtering_works(mock_read_warc, mock_manifest_path, in_aws):
    """Test that segment filtering actually works with real data processing."""
    mock_read_warc.return_value = Mock()

    manifest_func = "_get_s3_manifest_path" if in_aws else "_get_http_manifest_path"
    with patch.object(sys.modules["daft.datasets.common_crawl"], manifest_func) as mock_get_manifest_path:
        mock_get_manifest_path.side_effect = mock_manifest_path

        # Test with a segment that exists in our fixture data.
        daft.datasets.common_crawl("CC-MAIN-2025-33", segment="1234567890.1", in_aws=in_aws)

    # Verify only files from the specified segment were included.
    mock_read_warc.assert_called_once()
    args = mock_read_warc.call_args[0][0]

    # All returned paths should contain the specified segment.
    for path in args:
        assert "1234567890.1" in path

    # Should have exactly 3 files for this segment (based on our fixture).
    assert len(args) == 3


@pytest.mark.parametrize("in_aws", [True, False])
@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_num_files_limit_works(mock_read_warc, mock_manifest_path, in_aws):
    """Test that num_files limit actually works with real data processing."""
    mock_read_warc.return_value = Mock()

    manifest_func = "_get_s3_manifest_path" if in_aws else "_get_http_manifest_path"
    with patch.object(sys.modules["daft.datasets.common_crawl"], manifest_func) as mock_get_manifest_path:
        mock_get_manifest_path.side_effect = mock_manifest_path

        # Test limiting to 2 files.
        daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=2, in_aws=in_aws)

    mock_read_warc.assert_called_once()
    args = mock_read_warc.call_args[0][0]

    # Should have exactly 2 files.
    assert len(args) == 2


@pytest.mark.parametrize("in_aws", [True, False])
@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_segment_and_num_files_combined(mock_read_warc, mock_manifest_path, in_aws):
    """Test that segment filtering and num_files limit work together."""
    mock_read_warc.return_value = Mock()

    manifest_func = "_get_s3_manifest_path" if in_aws else "_get_http_manifest_path"
    with patch.object(sys.modules["daft.datasets.common_crawl"], manifest_func) as mock_get_manifest_path:
        mock_get_manifest_path.side_effect = mock_manifest_path

        # Test with segment filter and file limit.
        daft.datasets.common_crawl("CC-MAIN-2025-33", segment="1234567890.1", num_files=1, in_aws=in_aws)

        daft.datasets.common_crawl("CC-MAIN-2025-33", segment="1234567890.1", num_files=1, in_aws=in_aws)

    args = mock_read_warc.call_args[0][0]

    # Should have exactly 1 file, all from the specified segment.
    assert len(args) == 1
    for path in args:
        assert "1234567890.1" in path


def test_invalid_content_type():
    """Test that invalid content types raise appropriate errors."""
    with pytest.raises(ValueError, match="Invalid content type"):
        daft.datasets.common_crawl("CC-MAIN-2025-33", content="invalid", in_aws=True)


def test_num_files_zero_raises_error():
    """Test that num_files=0 raises an error."""
    with pytest.raises(ValueError, match="num_files must be a positive integer"):
        daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=0, in_aws=True)


def test_num_files_negative_raises_error():
    """Test that negative num_files raises an error."""
    with pytest.raises(ValueError, match="num_files must be a positive integer"):
        daft.datasets.common_crawl("CC-MAIN-2025-33", num_files=-1, in_aws=True)


@pytest.mark.parametrize("in_aws", [True, False])
@patch.object(sys.modules["daft.datasets.common_crawl"], "read_warc")
def test_io_config_passed_through(mock_read_warc, mock_manifest_path, in_aws):
    """Test that io_config is properly passed through to read_warc."""
    mock_read_warc.return_value = Mock()

    mock_io_config = daft.IOConfig()

    manifest_func = "_get_s3_manifest_path" if in_aws else "_get_http_manifest_path"
    with patch.object(sys.modules["daft.datasets.common_crawl"], manifest_func) as mock_get_manifest_path:
        mock_get_manifest_path.side_effect = mock_manifest_path

        daft.datasets.common_crawl("CC-MAIN-2025-33", io_config=mock_io_config, in_aws=in_aws)

    mock_read_warc.assert_called_once()
    assert mock_read_warc.call_args[1]["io_config"] == mock_io_config


def test_get_http_manifest_path():
    url = _get_http_manifest_path("CC-MAIN-2025-33", "warc")
    assert url == "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-33/warc.paths.gz"


def test_get_s3_manifest_path():
    url = _get_s3_manifest_path("CC-MAIN-2025-33", "warc")
    assert url == "s3://commoncrawl/crawl-data/CC-MAIN-2025-33/warc.paths.gz"
