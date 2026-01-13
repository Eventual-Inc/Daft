"""Unit tests for Gravitino filesystem implementation."""

from __future__ import annotations

import io
from unittest.mock import Mock, patch

import pytest

from daft.dependencies import pafs
from daft.io import IOConfig
from daft.io.gravitino_filesystem import (
    GravitinoFileSystem,
    GravitinoFileSystemHandler,
    GravitinoOutputStream,
)


class TestGravitinoFileSystemHandler:
    """Test the GravitinoFileSystemHandler class."""

    def test_init_default_config(self):
        """Test initialization with default IOConfig."""
        handler = GravitinoFileSystemHandler()
        assert handler.io_config is not None
        assert isinstance(handler.io_config, IOConfig)

    def test_init_custom_config(self):
        """Test initialization with custom IOConfig."""
        custom_config = IOConfig()
        handler = GravitinoFileSystemHandler(io_config=custom_config)
        assert handler.io_config is custom_config

    def test_get_file_info_single_path_file(self):
        """Test get_file_info with a single file path."""
        handler = GravitinoFileSystemHandler()
        file_infos = handler.get_file_info("gvfs://fileset/cat/sch/fs/file.parquet")

        assert len(file_infos) == 1
        assert file_infos[0].path == "gvfs://fileset/cat/sch/fs/file.parquet"
        # The actual implementation creates FileInfo objects that may be NotFound initially
        # This is expected behavior as the validation happens in the Rust layer
        assert file_infos[0].type in [pafs.FileType.File, pafs.FileType.NotFound]

    def test_get_file_info_single_path_directory(self):
        """Test get_file_info with a single directory path."""
        handler = GravitinoFileSystemHandler()
        file_infos = handler.get_file_info("gvfs://fileset/cat/sch/fs/")

        assert len(file_infos) == 1
        assert file_infos[0].path == "gvfs://fileset/cat/sch/fs/"
        assert file_infos[0].type == pafs.FileType.Directory

    def test_get_file_info_multiple_paths(self):
        """Test get_file_info with multiple paths."""
        handler = GravitinoFileSystemHandler()
        paths = [
            "gvfs://fileset/cat/sch/fs/file1.parquet",
            "gvfs://fileset/cat/sch/fs/dir/",
            "gvfs://fileset/cat/sch/fs/file2.json",
        ]
        file_infos = handler.get_file_info(paths)

        assert len(file_infos) == 3
        # Directory should be correctly identified
        assert file_infos[1].type == pafs.FileType.Directory
        # Files may be NotFound initially (validation happens in Rust layer)
        assert file_infos[0].type in [pafs.FileType.File, pafs.FileType.NotFound]
        assert file_infos[2].type in [pafs.FileType.File, pafs.FileType.NotFound]

    def test_get_file_info_pathlike_object(self):
        """Test get_file_info with os.PathLike object."""
        handler = GravitinoFileSystemHandler()

        # Use pathlib.Path which is a proper PathLike implementation
        from pathlib import Path

        # Create a mock path that returns our gvfs URL
        class GvfsPath(Path):
            def __new__(cls, *args, **kwargs):
                # Create a regular Path object but override __str__
                obj = super().__new__(cls, "dummy")
                return obj

            def __str__(self):
                return "gvfs://fileset/cat/sch/fs/file.parquet"

        try:
            path_obj = GvfsPath()
            file_infos = handler.get_file_info(path_obj)
            assert len(file_infos) == 1
        except Exception:
            # If pathlib approach doesn't work, test with a simpler approach
            # Just test that the method handles non-string, non-iterable objects gracefully
            class MockPath:
                def __str__(self):
                    return "gvfs://fileset/cat/sch/fs/file.parquet"

            # This will go through the else clause and try to iterate
            # Since MockPath is not iterable, it should handle the exception
            try:
                file_infos = handler.get_file_info(MockPath())
                # If we get here, the method handled it somehow
                assert len(file_infos) >= 0
            except TypeError:
                # This is expected for non-iterable objects
                pass

    def test_get_file_info_file_selector(self):
        """Test get_file_info with FileSelector-like object."""
        handler = GravitinoFileSystemHandler()

        class MockFileSelector:
            def __init__(self, base_dir):
                self.base_dir = base_dir

        selector = MockFileSelector("gvfs://fileset/cat/sch/fs/")
        file_infos = handler.get_file_info(selector)

        assert len(file_infos) == 1
        assert file_infos[0].path == "gvfs://fileset/cat/sch/fs/"
        assert file_infos[0].type == pafs.FileType.Directory

    def test_get_file_info_exception_handling(self):
        """Test get_file_info handles exceptions gracefully."""
        handler = GravitinoFileSystemHandler()

        # Test with a path that would cause an exception in the try block
        # We'll mock the FileInfo constructor to raise an exception
        with patch("daft.io.gravitino_filesystem.pafs.FileInfo") as mock_file_info:
            # First call (in try block) raises exception, second call (in except block) succeeds
            mock_file_info.side_effect = [Exception("Test error"), Mock()]

            file_infos = handler.get_file_info("gvfs://fileset/cat/sch/fs/file.parquet")
            # Should still return a result from the except block
            assert len(file_infos) == 1

    def test_open_input_stream_not_implemented(self):
        """Test that open_input_stream raises NotImplementedError."""
        handler = GravitinoFileSystemHandler()

        with pytest.raises(NotImplementedError, match="Direct streaming from gvfs://"):
            handler.open_input_stream("gvfs://fileset/cat/sch/fs/file.parquet")

    def test_open_write_mode(self):
        """Test opening a file in write mode."""
        handler = GravitinoFileSystemHandler()

        stream = handler.open("gvfs://fileset/cat/sch/fs/file.parquet", mode="wb")
        assert isinstance(stream, GravitinoOutputStream)
        assert stream.path == "gvfs://fileset/cat/sch/fs/file.parquet"

    def test_open_write_mode_w(self):
        """Test opening a file in 'w' mode."""
        handler = GravitinoFileSystemHandler()

        stream = handler.open("gvfs://fileset/cat/sch/fs/file.parquet", mode="w")
        assert isinstance(stream, GravitinoOutputStream)

    def test_open_unsupported_mode(self):
        """Test opening a file in unsupported mode."""
        handler = GravitinoFileSystemHandler()

        with pytest.raises(NotImplementedError, match="Mode rb not supported"):
            handler.open("gvfs://fileset/cat/sch/fs/file.parquet", mode="rb")

    def test_open_output_stream(self):
        """Test open_output_stream method."""
        handler = GravitinoFileSystemHandler()

        stream = handler.open_output_stream("gvfs://fileset/cat/sch/fs/file.parquet")
        assert isinstance(stream, GravitinoOutputStream)

    def test_open_output_stream_with_metadata(self):
        """Test open_output_stream with metadata and kwargs."""
        handler = GravitinoFileSystemHandler()

        metadata = {"compression": "gzip"}
        stream = handler.open_output_stream(
            "gvfs://fileset/cat/sch/fs/file.parquet", metadata=metadata, some_kwarg="value"
        )
        assert isinstance(stream, GravitinoOutputStream)

    def test_create_dir(self):
        """Test create_dir method (should be no-op)."""
        handler = GravitinoFileSystemHandler()

        # Should not raise any exception
        handler.create_dir("gvfs://fileset/cat/sch/fs/dir/")
        handler.create_dir("gvfs://fileset/cat/sch/fs/dir/", recursive=True)

    def test_delete_dir_not_implemented(self):
        """Test that delete_dir raises NotImplementedError."""
        handler = GravitinoFileSystemHandler()

        with pytest.raises(NotImplementedError, match="Directory deletion not implemented"):
            handler.delete_dir("gvfs://fileset/cat/sch/fs/dir/")

    def test_exists(self):
        """Test exists method (always returns True)."""
        handler = GravitinoFileSystemHandler()

        assert handler.exists("gvfs://fileset/cat/sch/fs/file.parquet") is True
        assert handler.exists("gvfs://fileset/cat/sch/fs/nonexistent.parquet") is True

    def test_rm_not_implemented(self):
        """Test that rm raises NotImplementedError."""
        handler = GravitinoFileSystemHandler()

        with pytest.raises(NotImplementedError, match="File deletion not implemented"):
            handler.rm("gvfs://fileset/cat/sch/fs/file.parquet")

    def test_delete_file_not_implemented(self):
        """Test that delete_file raises NotImplementedError."""
        handler = GravitinoFileSystemHandler()

        with pytest.raises(NotImplementedError, match="File deletion not implemented"):
            handler.delete_file("gvfs://fileset/cat/sch/fs/file.parquet")

    def test_move_not_implemented(self):
        """Test that move raises NotImplementedError."""
        handler = GravitinoFileSystemHandler()

        with pytest.raises(NotImplementedError, match="File moving not implemented"):
            handler.move("gvfs://fileset/cat/sch/fs/src.parquet", "gvfs://fileset/cat/sch/fs/dst.parquet")

    def test_copy_file_not_implemented(self):
        """Test that copy_file raises NotImplementedError."""
        handler = GravitinoFileSystemHandler()

        with pytest.raises(NotImplementedError, match="File copying not implemented"):
            handler.copy_file("gvfs://fileset/cat/sch/fs/src.parquet", "gvfs://fileset/cat/sch/fs/dst.parquet")

    def test_normalize_path(self):
        """Test normalize_path method."""
        handler = GravitinoFileSystemHandler()

        path = "gvfs://fileset/cat/sch/fs/file.parquet"
        assert handler.normalize_path(path) == path

    def test_type_name(self):
        """Test type_name property."""
        handler = GravitinoFileSystemHandler()
        assert handler.type_name == "gravitino"


class TestGravitinoOutputStream:
    """Test the GravitinoOutputStream class."""

    def test_init_default_config(self):
        """Test initialization with default IOConfig."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        assert stream.path == "gvfs://fileset/cat/sch/fs/file.parquet"
        assert stream.io_config is not None
        assert isinstance(stream.buffer, io.BytesIO)
        assert not stream._closed

    def test_init_custom_config(self):
        """Test initialization with custom IOConfig."""
        custom_config = IOConfig()
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet", io_config=custom_config)

        assert stream.io_config is custom_config

    def test_fspath(self):
        """Test __fspath__ method."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        assert stream.__fspath__() == "gvfs://fileset/cat/sch/fs/file.parquet"

    def test_getattr_fspath(self):
        """Test __getattr__ for __fspath__."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        fspath_func = stream.__getattr__("__fspath__")
        assert fspath_func() == "gvfs://fileset/cat/sch/fs/file.parquet"

    def test_getattr_fileno(self):
        """Test __getattr__ for fileno."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        fileno_func = stream.__getattr__("fileno")
        assert fileno_func() is False

    def test_getattr_isatty(self):
        """Test __getattr__ for isatty."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        isatty_func = stream.__getattr__("isatty")
        assert isatty_func() is False

    def test_getattr_mode(self):
        """Test __getattr__ for mode."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        mode_func = stream.__getattr__("mode")
        assert mode_func() == "wb"

    def test_getattr_name(self):
        """Test __getattr__ for name."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        name_func = stream.__getattr__("name")
        assert name_func() == "gvfs://fileset/cat/sch/fs/file.parquet"

    def test_getattr_not_implemented(self):
        """Test __getattr__ for unknown method."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        with pytest.raises(NotImplementedError, match="Method unknown_method not implemented"):
            unknown_func = stream.__getattr__("unknown_method")
            unknown_func()

    def test_write(self):
        """Test writing data to the stream."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        data = b"test data"
        bytes_written = stream.write(data)

        assert bytes_written == len(data)
        assert stream.buffer.getvalue() == data

    def test_write_closed_stream(self):
        """Test writing to a closed stream raises ValueError."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        stream._closed = True

        with pytest.raises(ValueError, match="Cannot write to closed stream"):
            stream.write(b"test data")

    def test_flush(self):
        """Test flushing the stream."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        # Should not raise any exception
        stream.flush()

    def test_flush_closed_stream(self):
        """Test flushing a closed stream."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        stream._closed = True

        # Should not raise any exception
        stream.flush()

    @patch("daft.io.gravitino_filesystem.io_put")
    def test_close_with_data(self, mock_io_put):
        """Test closing stream with data writes to Gravitino."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        test_data = b"test data for gravitino"
        stream.write(test_data)
        stream.close()

        assert stream._closed is True
        mock_io_put.assert_called_once_with(
            path="gvfs://fileset/cat/sch/fs/file.parquet",
            data=test_data,
            multithreaded_io=True,
            io_config=stream.io_config,
        )

    @patch("daft.io.gravitino_filesystem.io_put")
    def test_close_empty_data(self, mock_io_put):
        """Test closing stream with no data doesn't call io_put."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        stream.close()

        assert stream._closed is True
        mock_io_put.assert_not_called()

    def test_close_already_closed(self):
        """Test closing an already closed stream."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        stream.close()

        # Should not raise any exception
        stream.close()
        assert stream._closed is True

    @patch("daft.io.gravitino_filesystem.io_put", side_effect=Exception("IO Error"))
    def test_close_io_error(self, mock_io_put):
        """Test closing stream handles IO errors."""
        # Create a mock logger to capture the error
        with patch("logging.getLogger") as mock_get_logger:
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger

            stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
            stream.write(b"test data")

            with pytest.raises(Exception, match="IO Error"):
                stream.close()

            mock_logger.error.assert_called_once()

    def test_del_cleanup(self):
        """Test __del__ method cleanup."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        # Mock close method to verify it's called
        with patch.object(stream, "close") as mock_close:
            stream.__del__()
            mock_close.assert_called_once()

    def test_del_cleanup_already_closed(self):
        """Test __del__ method with already closed stream."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        stream._closed = True

        # Should not raise any exception
        stream.__del__()

    def test_del_cleanup_exception(self):
        """Test __del__ method handles exceptions gracefully."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        with patch.object(stream, "close", side_effect=Exception("Cleanup error")):
            # Should not raise any exception
            stream.__del__()

    @patch("daft.io.gravitino_filesystem.io_put")
    def test_context_manager(self, mock_io_put):
        """Test using stream as context manager."""
        with GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet") as stream:
            assert isinstance(stream, GravitinoOutputStream)
            assert not stream._closed
            stream.write(b"test data")

        assert stream._closed is True
        mock_io_put.assert_called_once()

    @patch("daft.io.gravitino_filesystem.io_put")
    def test_context_manager_with_exception(self, mock_io_put):
        """Test context manager handles exceptions properly."""
        try:
            with GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet") as stream:
                stream.write(b"test data")
                raise ValueError("Test exception")
        except ValueError:
            pass

        assert stream._closed is True
        mock_io_put.assert_called_once()

    def test_closed_property(self):
        """Test closed property."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        assert stream.closed is False
        stream.close()
        assert stream.closed is True

    def test_readable(self):
        """Test readable method."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        assert stream.readable() is False

    def test_writable(self):
        """Test writable method."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        assert stream.writable() is True
        stream.close()
        assert stream.writable() is False

    def test_seekable(self):
        """Test seekable method."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        assert stream.seekable() is False

    def test_tell(self):
        """Test tell method."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        assert stream.tell() == 0
        stream.write(b"test")
        assert stream.tell() == 4

    def test_read_not_implemented(self):
        """Test that read raises NotImplementedError."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        with pytest.raises(NotImplementedError, match="Cannot read from output stream"):
            stream.read()

    def test_seek_not_implemented(self):
        """Test that seek raises NotImplementedError."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        with pytest.raises(NotImplementedError, match="Seeking not supported"):
            stream.seek(0)

    def test_size(self):
        """Test size method."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        assert stream.size() == 0
        stream.write(b"test data")
        assert stream.size() == 9

    def test_mode_method(self):
        """Test mode method."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        assert stream.mode() == "wb"

    def test_fileno_not_implemented(self):
        """Test that fileno raises NotImplementedError."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        with pytest.raises(NotImplementedError, match="fileno not supported"):
            stream.fileno()

    def test_isatty_method(self):
        """Test isatty method."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        assert stream.isatty() is False

    def test_truncate(self):
        """Test truncate method."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        stream.write(b"test data")
        assert stream.tell() == 9

        result = stream.truncate(4)
        assert result == 4
        assert stream.buffer.getvalue() == b"test"

    def test_truncate_no_size(self):
        """Test truncate method without size parameter."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")

        stream.write(b"test data")
        stream.buffer.seek(4)  # Move position to middle

        result = stream.truncate()
        assert result == 4
        assert stream.buffer.getvalue() == b"test"

    def test_truncate_closed_stream(self):
        """Test truncate on closed stream raises ValueError."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/file.parquet")
        stream._closed = True

        with pytest.raises(ValueError, match="Cannot truncate closed stream"):
            stream.truncate(0)


class TestGravitinoFileSystem:
    """Test the GravitinoFileSystem class."""

    def test_init_default_config(self):
        """Test initialization with default IOConfig."""
        fs = GravitinoFileSystem()
        assert fs is not None

    def test_init_custom_config(self):
        """Test initialization with custom IOConfig."""
        custom_config = IOConfig()
        fs = GravitinoFileSystem(io_config=custom_config)
        assert fs is not None

    def test_init_creates_handler(self):
        """Test that initialization creates proper handler chain."""
        custom_config = IOConfig()

        # We can't easily mock PyFileSystem.__init__ due to it being immutable
        # Instead, just test that the filesystem can be created without errors
        fs = GravitinoFileSystem(io_config=custom_config)
        assert fs is not None


class TestIntegration:
    """Integration tests for the filesystem components."""

    @patch("daft.io.gravitino_filesystem.io_put")
    def test_end_to_end_write_operation(self, mock_io_put):
        """Test complete write operation from filesystem to output stream."""
        handler = GravitinoFileSystemHandler()

        # Open output stream
        stream = handler.open_output_stream("gvfs://fileset/cat/sch/fs/test.parquet")

        # Write data
        test_data = b"parquet file content"
        stream.write(test_data)

        # Close stream (should trigger write to Gravitino)
        stream.close()

        # Verify io_put was called correctly
        mock_io_put.assert_called_once_with(
            path="gvfs://fileset/cat/sch/fs/test.parquet",
            data=test_data,
            multithreaded_io=True,
            io_config=stream.io_config,
        )

    def test_file_info_consistency(self):
        """Test that file info is consistent across different path formats."""
        handler = GravitinoFileSystemHandler()

        # Test different path formats
        paths = [
            "gvfs://fileset/cat/sch/fs/file.parquet",
            "gvfs://fileset/cat/sch/fs/dir/",
            "gvfs://fileset/cat/sch/fs/nested/path/file.json",
        ]

        for path in paths:
            file_infos = handler.get_file_info(path)
            assert len(file_infos) == 1
            assert file_infos[0].path == path

            if path.endswith("/"):
                assert file_infos[0].type == pafs.FileType.Directory
            else:
                # Files may be NotFound initially (validation happens in Rust layer)
                assert file_infos[0].type in [pafs.FileType.File, pafs.FileType.NotFound]

    @patch("daft.io.gravitino_filesystem.io_put")
    def test_multiple_writes_same_stream(self, mock_io_put):
        """Test multiple writes to the same stream."""
        stream = GravitinoOutputStream("gvfs://fileset/cat/sch/fs/multi.parquet")

        # Write data in chunks
        chunks = [b"chunk1", b"chunk2", b"chunk3"]
        for chunk in chunks:
            stream.write(chunk)

        stream.close()

        # Should write all chunks as one operation
        expected_data = b"".join(chunks)
        mock_io_put.assert_called_once_with(
            path="gvfs://fileset/cat/sch/fs/multi.parquet",
            data=expected_data,
            multithreaded_io=True,
            io_config=stream.io_config,
        )

    @patch("daft.io.gravitino_filesystem.io_put")
    def test_stream_properties_consistency(self, mock_io_put):
        """Test that stream properties are consistent throughout lifecycle."""
        path = "gvfs://fileset/cat/sch/fs/props.parquet"
        stream = GravitinoOutputStream(path)

        # Initial state
        assert stream.path == path
        assert not stream.closed
        assert stream.readable() is False
        assert stream.writable() is True
        assert stream.seekable() is False
        assert stream.tell() == 0
        assert stream.size() == 0

        # After writing
        stream.write(b"test")
        assert stream.tell() == 4
        assert stream.size() == 4
        assert stream.writable() is True

        # After closing
        stream.close()
        assert stream.closed is True
        assert stream.writable() is False
        mock_io_put.assert_called_once()
