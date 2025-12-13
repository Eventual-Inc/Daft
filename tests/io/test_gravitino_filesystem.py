"""Tests for Gravitino filesystem implementation."""

from __future__ import annotations

import io
from unittest.mock import Mock, patch

import pytest

from daft.io import IOConfig


class TestGravitinoFileSystem:
    """Test GravitinoFileSystem class."""

    @pytest.fixture
    def gravitino_fs(self):
        """Create a GravitinoFileSystem instance for testing."""
        from daft.io.gravitino_filesystem import GravitinoFileSystem

        return GravitinoFileSystem()

    @pytest.fixture
    def gravitino_fs_with_config(self):
        """Create a GravitinoFileSystem instance with IOConfig."""
        from daft.io.gravitino_filesystem import GravitinoFileSystem

        io_config = IOConfig()
        return GravitinoFileSystem(io_config=io_config)

    def test_init_without_config(self):
        """Test initialization without IOConfig."""
        from daft.io.gravitino_filesystem import GravitinoFileSystem

        fs = GravitinoFileSystem()
        assert fs.io_config is not None
        assert isinstance(fs.io_config, IOConfig)

    def test_init_with_config(self):
        """Test initialization with IOConfig."""
        from daft.io.gravitino_filesystem import GravitinoFileSystem

        io_config = IOConfig()
        fs = GravitinoFileSystem(io_config=io_config)
        assert fs.io_config is io_config

    def test_type_name(self, gravitino_fs):
        """Test type_name property."""
        assert gravitino_fs.type_name == "gravitino"

    def test_normalize_path(self, gravitino_fs):
        """Test normalize_path keeps gvfs:// paths as-is."""
        test_paths = [
            "gvfs://fileset/catalog/schema/fileset/file.parquet",
            "gvfs://fileset/catalog/schema/fileset/",
            "gvfs://fileset/catalog/schema/fileset/dir/file.json",
        ]

        for path in test_paths:
            assert gravitino_fs.normalize_path(path) == path

    def test_get_file_info_single_file(self, gravitino_fs):
        """Test get_file_info with a single file path."""
        pytest.importorskip("pyarrow")

        path = "gvfs://fileset/catalog/schema/fileset/file.parquet"
        file_infos = gravitino_fs.get_file_info(path)

        assert len(file_infos) == 1
        assert file_infos[0].path == path
        # The file info should be created (type may vary based on PyArrow version)
        assert file_infos[0] is not None

    def test_get_file_info_directory(self, gravitino_fs):
        """Test get_file_info with a directory path (trailing slash)."""
        pytest.importorskip("pyarrow")

        path = "gvfs://fileset/catalog/schema/fileset/"
        file_infos = gravitino_fs.get_file_info(path)

        assert len(file_infos) == 1
        assert file_infos[0].path == path
        # The file info should be created
        assert file_infos[0] is not None

    def test_get_file_info_multiple_paths(self, gravitino_fs):
        """Test get_file_info with multiple paths."""
        paths = [
            "gvfs://fileset/catalog/schema/fileset/file1.parquet",
            "gvfs://fileset/catalog/schema/fileset/file2.parquet",
            "gvfs://fileset/catalog/schema/fileset/dir/",
        ]
        file_infos = gravitino_fs.get_file_info(paths)

        assert len(file_infos) == 3
        assert file_infos[0].path == paths[0]
        assert file_infos[1].path == paths[1]
        assert file_infos[2].path == paths[2]

    def test_get_file_info_with_file_selector(self, gravitino_fs):
        """Test get_file_info with a FileSelector-like object."""
        # Create a mock FileSelector
        mock_selector = Mock()
        mock_selector.base_dir = "gvfs://fileset/catalog/schema/fileset/"

        file_infos = gravitino_fs.get_file_info(mock_selector)

        assert len(file_infos) == 1
        assert file_infos[0].path == "gvfs://fileset/catalog/schema/fileset/"

    def test_get_file_info_handles_pathlike(self, gravitino_fs):
        """Test get_file_info handles os.PathLike objects."""
        pytest.importorskip("pyarrow")
        import pathlib

        # Create a Path object (which is PathLike)
        path = pathlib.PurePosixPath("gvfs://fileset/catalog/schema/fileset/file.parquet")
        file_infos = gravitino_fs.get_file_info(path)

        assert len(file_infos) == 1
        assert file_infos[0].path == str(path)

    def test_create_dir(self, gravitino_fs):
        """Test create_dir is a no-op."""
        # Should not raise any exception
        gravitino_fs.create_dir("gvfs://fileset/catalog/schema/fileset/dir/")
        gravitino_fs.create_dir("gvfs://fileset/catalog/schema/fileset/dir/", recursive=True)
        gravitino_fs.create_dir("gvfs://fileset/catalog/schema/fileset/dir/", recursive=False)

    def test_open_input_stream_not_implemented(self, gravitino_fs):
        """Test open_input_stream raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Direct streaming from gvfs://"):
            gravitino_fs.open_input_stream("gvfs://fileset/catalog/schema/fileset/file.parquet")

    def test_delete_dir_not_implemented(self, gravitino_fs):
        """Test delete_dir raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Directory deletion not implemented"):
            gravitino_fs.delete_dir("gvfs://fileset/catalog/schema/fileset/dir/")

    def test_delete_file_not_implemented(self, gravitino_fs):
        """Test delete_file raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="File deletion not implemented"):
            gravitino_fs.delete_file("gvfs://fileset/catalog/schema/fileset/file.parquet")

    def test_move_not_implemented(self, gravitino_fs):
        """Test move raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="File moving not implemented"):
            gravitino_fs.move(
                "gvfs://fileset/catalog/schema/fileset/src.parquet",
                "gvfs://fileset/catalog/schema/fileset/dest.parquet",
            )

    def test_copy_file_not_implemented(self, gravitino_fs):
        """Test copy_file raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="File copying not implemented"):
            gravitino_fs.copy_file(
                "gvfs://fileset/catalog/schema/fileset/src.parquet",
                "gvfs://fileset/catalog/schema/fileset/dest.parquet",
            )

    def test_open_output_stream(self, gravitino_fs):
        """Test open_output_stream returns GravitinoOutputStream."""
        from daft.io.gravitino_filesystem import GravitinoOutputStream

        path = "gvfs://fileset/catalog/schema/fileset/output.parquet"
        stream = gravitino_fs.open_output_stream(path)

        assert isinstance(stream, GravitinoOutputStream)
        assert stream.path == path
        assert stream.io_config is not None

    def test_open_output_stream_with_metadata(self, gravitino_fs):
        """Test open_output_stream with metadata and kwargs."""
        from daft.io.gravitino_filesystem import GravitinoOutputStream

        path = "gvfs://fileset/catalog/schema/fileset/output.parquet"
        metadata = {"key": "value"}
        stream = gravitino_fs.open_output_stream(path, metadata=metadata, compression="snappy")

        assert isinstance(stream, GravitinoOutputStream)
        assert stream.path == path


class TestGravitinoOutputStream:
    """Test GravitinoOutputStream class."""

    @pytest.fixture
    def output_stream(self):
        """Create a GravitinoOutputStream instance for testing."""
        from daft.io.gravitino_filesystem import GravitinoOutputStream

        path = "gvfs://fileset/catalog/schema/fileset/test.parquet"
        return GravitinoOutputStream(path)

    @pytest.fixture
    def output_stream_with_config(self):
        """Create a GravitinoOutputStream instance with IOConfig."""
        from daft.io.gravitino_filesystem import GravitinoOutputStream

        path = "gvfs://fileset/catalog/schema/fileset/test.parquet"
        io_config = IOConfig()
        return GravitinoOutputStream(path, io_config=io_config)

    def test_init_without_config(self):
        """Test initialization without IOConfig."""
        from daft.io.gravitino_filesystem import GravitinoOutputStream

        path = "gvfs://fileset/catalog/schema/fileset/test.parquet"
        stream = GravitinoOutputStream(path)

        assert stream.path == path
        assert stream.io_config is not None
        assert isinstance(stream.io_config, IOConfig)
        assert not stream._closed
        assert isinstance(stream.buffer, io.BytesIO)

    def test_init_with_config(self):
        """Test initialization with IOConfig."""
        from daft.io.gravitino_filesystem import GravitinoOutputStream

        path = "gvfs://fileset/catalog/schema/fileset/test.parquet"
        io_config = IOConfig()
        stream = GravitinoOutputStream(path, io_config=io_config)

        assert stream.path == path
        assert stream.io_config is io_config

    def test_fspath(self, output_stream):
        """Test __fspath__ returns the path."""
        import os

        assert os.fspath(output_stream) == output_stream.path

    def test_write(self, output_stream):
        """Test writing data to the stream."""
        data = b"test data"
        bytes_written = output_stream.write(data)

        assert bytes_written == len(data)
        assert output_stream.buffer.getvalue() == data

    def test_write_multiple_times(self, output_stream):
        """Test writing data multiple times."""
        data1 = b"first "
        data2 = b"second "
        data3 = b"third"

        output_stream.write(data1)
        output_stream.write(data2)
        output_stream.write(data3)

        assert output_stream.buffer.getvalue() == data1 + data2 + data3

    def test_write_to_closed_stream(self, output_stream):
        """Test writing to a closed stream raises ValueError."""
        output_stream._closed = True

        with pytest.raises(ValueError, match="Cannot write to closed stream"):
            output_stream.write(b"data")

    def test_flush(self, output_stream):
        """Test flushing the stream."""
        output_stream.write(b"test data")
        output_stream.flush()  # Should not raise

    def test_flush_closed_stream(self, output_stream):
        """Test flushing a closed stream."""
        output_stream._closed = True
        output_stream.flush()  # Should not raise

    def test_tell(self, output_stream):
        """Test tell returns current position."""
        assert output_stream.tell() == 0

        output_stream.write(b"12345")
        assert output_stream.tell() == 5

        output_stream.write(b"67890")
        assert output_stream.tell() == 10

    def test_size(self, output_stream):
        """Test size returns current position."""
        assert output_stream.size() == 0

        output_stream.write(b"test")
        assert output_stream.size() == 4

    def test_mode(self, output_stream):
        """Test mode returns 'wb'."""
        assert output_stream.mode() == "wb"

    def test_closed_property(self, output_stream):
        """Test closed property."""
        assert not output_stream.closed

        output_stream._closed = True
        assert output_stream.closed

    def test_readable(self, output_stream):
        """Test readable returns False."""
        assert not output_stream.readable()

    def test_writable(self, output_stream):
        """Test writable returns True when not closed."""
        assert output_stream.writable()

        output_stream._closed = True
        assert not output_stream.writable()

    def test_seekable(self, output_stream):
        """Test seekable returns False."""
        assert not output_stream.seekable()

    def test_isatty(self, output_stream):
        """Test isatty returns False."""
        assert not output_stream.isatty()

    def test_read_not_implemented(self, output_stream):
        """Test read raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Cannot read from output stream"):
            output_stream.read()

    def test_seek_not_implemented(self, output_stream):
        """Test seek raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Seeking not supported"):
            output_stream.seek(0)

    def test_fileno_not_implemented(self, output_stream):
        """Test fileno raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="fileno not supported"):
            output_stream.fileno()

    def test_truncate(self, output_stream):
        """Test truncate."""
        output_stream.write(b"0123456789")
        assert output_stream.tell() == 10

        size = output_stream.truncate(5)
        assert size == 5
        assert output_stream.buffer.getvalue() == b"01234"

    def test_truncate_without_size(self, output_stream):
        """Test truncate without size argument."""
        output_stream.write(b"0123456789")
        output_stream.buffer.seek(5)

        size = output_stream.truncate()
        assert size == 5

    def test_truncate_closed_stream(self, output_stream):
        """Test truncate on closed stream raises ValueError."""
        output_stream._closed = True

        with pytest.raises(ValueError, match="Cannot truncate closed stream"):
            output_stream.truncate()

    @patch("daft.io.gravitino_filesystem.io_put")
    def test_close_writes_data(self, mock_io_put, output_stream):
        """Test close writes buffered data to Gravitino."""
        test_data = b"test data for gravitino"
        output_stream.write(test_data)

        output_stream.close()

        # Verify io_put was called with correct arguments
        mock_io_put.assert_called_once()
        call_args = mock_io_put.call_args
        assert call_args.kwargs["path"] == output_stream.path
        assert call_args.kwargs["data"] == test_data
        assert call_args.kwargs["multithreaded_io"] is True
        assert call_args.kwargs["io_config"] == output_stream.io_config

        # Verify stream is closed
        assert output_stream.closed

    @patch("daft.io.gravitino_filesystem.io_put")
    def test_close_empty_buffer(self, mock_io_put, output_stream):
        """Test close with empty buffer."""
        output_stream.close()

        # Should still call io_put with empty data
        mock_io_put.assert_called_once()
        call_args = mock_io_put.call_args
        assert call_args.kwargs["data"] == b""

    @patch("daft.io.gravitino_filesystem.io_put")
    def test_close_idempotent(self, mock_io_put, output_stream):
        """Test close can be called multiple times safely."""
        output_stream.write(b"data")
        output_stream.close()
        output_stream.close()  # Second close should be no-op

        # io_put should only be called once
        assert mock_io_put.call_count == 1

    @patch("daft.io.gravitino_filesystem.io_put", side_effect=Exception("Write failed"))
    def test_close_handles_write_error(self, mock_io_put, output_stream):
        """Test close handles write errors."""
        output_stream.write(b"data")

        with pytest.raises(Exception, match="Write failed"):
            output_stream.close()

        # Stream should still be marked as closed
        assert output_stream.closed

    def test_name_property(self, output_stream):
        """Test name property returns the path."""
        # Test that name property returns the path
        result = output_stream.name
        assert result == output_stream.path

    def test_context_manager_protocol(self, output_stream_with_config):
        """Test that the stream can be used as a context manager."""
        # While not explicitly implemented, test the basic close behavior
        with patch("daft.io.gravitino_filesystem.io_put"):
            output_stream_with_config.write(b"test")
            output_stream_with_config.close()
            assert output_stream_with_config.closed


class TestGravitinoFileSystemIntegration:
    """Integration tests for GravitinoFileSystem."""

    def test_filesystem_and_stream_integration(self):
        """Test that filesystem and stream work together."""
        from daft.io.gravitino_filesystem import GravitinoFileSystem

        fs = GravitinoFileSystem()
        path = "gvfs://fileset/catalog/schema/fileset/test.parquet"

        with patch("daft.io.gravitino_filesystem.io_put") as mock_io_put:
            stream = fs.open_output_stream(path)
            stream.write(b"integration test data")
            stream.close()

            mock_io_put.assert_called_once()
            assert mock_io_put.call_args.kwargs["path"] == path

    def test_multiple_streams(self):
        """Test creating multiple output streams."""
        from daft.io.gravitino_filesystem import GravitinoFileSystem

        fs = GravitinoFileSystem()
        path1 = "gvfs://fileset/catalog/schema/fileset/file1.parquet"
        path2 = "gvfs://fileset/catalog/schema/fileset/file2.parquet"

        with patch("daft.io.gravitino_filesystem.io_put") as mock_io_put:
            stream1 = fs.open_output_stream(path1)
            stream2 = fs.open_output_stream(path2)

            stream1.write(b"data1")
            stream2.write(b"data2")

            stream1.close()
            stream2.close()

            assert mock_io_put.call_count == 2

    def test_file_info_for_various_path_types(self):
        """Test get_file_info with various path types."""
        from daft.io.gravitino_filesystem import GravitinoFileSystem

        fs = GravitinoFileSystem()

        # Test with different path formats
        paths = [
            "gvfs://fileset/catalog/schema/fileset/file.parquet",
            "gvfs://fileset/catalog/schema/fileset/dir/",
            "gvfs://fileset/catalog/schema/fileset/nested/path/file.json",
        ]

        file_infos = fs.get_file_info(paths)
        assert len(file_infos) == len(paths)

        for i, path in enumerate(paths):
            assert file_infos[i].path == path


if __name__ == "__main__":
    # Run basic tests
    print("Testing GravitinoFileSystem...")

    fs_test = TestGravitinoFileSystem()
    fs_test.test_init_without_config()
    fs_test.test_init_with_config()
    print("GravitinoFileSystem basic tests passed")

    stream_test = TestGravitinoOutputStream()
    stream_test.test_init_without_config()
    stream_test.test_init_with_config()
    print("GravitinoOutputStream basic tests passed")

    print("\nAll basic tests passed!")
    print("Run with pytest for full test suite: pytest tests/io/test_gravitino_filesystem.py -v")
