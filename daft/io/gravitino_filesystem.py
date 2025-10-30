"""PyArrow filesystem implementation for Gravitino gvfs:// URLs."""

from __future__ import annotations

import io
import os
from typing import Any

from daft.dependencies import pa, pafs
from daft.io import IOConfig


class GravitinoFileSystem:
    """PyArrow FileSystem implementation for Gravitino gvfs:// URLs.

    This filesystem delegates operations to Daft's Rust-based Gravitino implementation,
    allowing PyArrow-based operations (like parquet writing) to work with gvfs:// URLs.
    """

    def __init__(self, io_config: IOConfig | None = None):
        """Initialize the Gravitino filesystem.

        Args:
            io_config: IOConfig containing Gravitino configuration
        """
        self.io_config = io_config or IOConfig()

    def get_file_info(self, paths_or_selector: Any) -> list[pafs.FileInfo]:
        """Get file info for the given paths or selector."""
        if isinstance(paths_or_selector, (str, os.PathLike)):
            paths = [str(paths_or_selector)]
        elif hasattr(paths_or_selector, "base_dir"):
            # It's a FileSelector
            base_path = paths_or_selector.base_dir
            paths = [base_path]
        else:
            paths = [str(p) for p in paths_or_selector]

        file_infos = []
        for path in paths:
            try:
                # For gvfs:// paths, we'll assume they exist and are files
                # The actual validation happens in the Rust layer
                if path.endswith("/"):
                    file_info = pafs.FileInfo(path, pafs.FileType.Directory)
                else:
                    file_info = pafs.FileInfo(path, pafs.FileType.File)
                    file_info.size = -1  # Unknown size
                file_infos.append(file_info)

            except Exception:
                # If anything fails, mark as not found
                file_info = pafs.FileInfo(path, pafs.FileType.NotFound)
                file_infos.append(file_info)

        return file_infos

    def open_input_stream(self, path: str) -> pa.NativeFile:
        """Open an input stream for reading from the given path."""
        raise NotImplementedError(
            "Direct streaming from gvfs:// not yet implemented. "
            "Use daft.read_parquet() instead for reading operations."
        )

    def open_output_stream(self, path: str, metadata: dict[str, str] | None = None, **kwargs: Any) -> pa.NativeFile:
        """Open an output stream for writing to the given path."""
        # Accept any additional kwargs that PyArrow might pass (like compression)
        print(
            f"DEBUG: GravitinoFileSystem.open_output_stream called with path={path}, metadata={metadata}, kwargs={kwargs}"
        )
        return GravitinoOutputStream(path, self.io_config)

    def create_dir(self, path: str, *, recursive: bool = True) -> None:
        """Create a directory. For gvfs://, this is typically a no-op."""
        # Gravitino filesets don't require explicit directory creation
        pass

    def delete_dir(self, path: str) -> None:
        """Delete a directory."""
        raise NotImplementedError("Directory deletion not implemented for gvfs://")

    def delete_file(self, path: str) -> None:
        """Delete a file."""
        raise NotImplementedError("File deletion not implemented for gvfs://")

    def move(self, src: str, dest: str) -> None:
        """Move/rename a file or directory."""
        raise NotImplementedError("File moving not implemented for gvfs://")

    def copy_file(self, src: str, dest: str) -> None:
        """Copy a file."""
        raise NotImplementedError("File copying not implemented for gvfs://")

    def normalize_path(self, path: str) -> str:
        """Normalize the path. For gvfs://, we keep it as-is."""
        return path

    @property
    def type_name(self) -> str:
        """Return the filesystem type name."""
        return "gravitino"


class GravitinoOutputStream:
    """Output stream for writing to Gravitino gvfs:// URLs."""

    def __init__(self, path: str, io_config: IOConfig | None = None):
        """Initialize the output stream.

        Args:
            path: The gvfs:// path to write to
            io_config: IOConfig containing Gravitino configuration
        """
        print(f"DEBUG: GravitinoOutputStream.__init__ called with path={path}")
        self.path = path
        self.io_config = io_config or IOConfig()
        self.buffer = io.BytesIO()
        self._closed = False

    def __fspath__(self) -> str:
        """Return the file system path representation."""
        return self.path

    def __getattr__(self, name: str) -> Any:
        """Handle missing attributes with debug output."""
        print(f"DEBUG: GravitinoOutputStream.__getattr__ called for missing method: {name}")
        if name in ["__fspath__"]:
            return lambda: self.path

        # Return a dummy function for any missing method to see what PyArrow is trying to call
        def dummy_method(*args: Any, **kwargs: Any) -> Any:
            print(f"DEBUG: Dummy method {name} called with args={args}, kwargs={kwargs}")
            if name in ["fileno", "isatty"]:
                return False
            elif name in ["mode"]:
                return "wb"
            elif name in ["name"]:
                return self.path
            else:
                raise NotImplementedError(f"Method {name} not implemented")

        return dummy_method

    def write(self, data: bytes) -> int:
        """Write data to the buffer."""
        print(f"DEBUG: GravitinoOutputStream.write called with {len(data)} bytes")
        if self._closed:
            raise ValueError("Cannot write to closed stream")
        return self.buffer.write(data)

    def flush(self) -> None:
        """Flush the buffer."""
        if not self._closed:
            self.buffer.flush()

    def close(self) -> None:
        """Close the stream and write the buffered data to Gravitino."""
        print("DEBUG: GravitinoOutputStream.close() called")
        if self._closed:
            return

        try:
            # Get the buffered data
            data = self.buffer.getvalue()
            print(f"DEBUG: About to write {len(data)} bytes to {self.path}")

            # Write the data using Daft's Rust layer via the low-level interface
            self._write_to_gravitino(data)
            print(f"DEBUG: Successfully wrote data to {self.path}")

        except Exception as e:
            print(f"DEBUG: Error in close(): {e}")
            raise
        finally:
            self.buffer.close()
            self._closed = True

    def _write_to_gravitino(self, data: bytes) -> None:
        """Write data to Gravitino using Daft's Rust layer."""
        print(f"DEBUG: _write_to_gravitino called with {len(data)} bytes")

        # Use the new io_put function directly to write the bytes to gvfs://
        from daft.daft import io_put

        try:
            print(f"DEBUG: Calling io_put with path={self.path}")
            io_put(
                path=self.path,
                data=data,  # Pass bytes directly to Rust
                multithreaded_io=True,
                io_config=self.io_config,
            )
            print("DEBUG: io_put completed successfully")

        except Exception as e:
            print(f"DEBUG: io_put failed with error: {e}")
            raise

    @property
    def closed(self) -> bool:
        """Check if the stream is closed."""
        return self._closed

    def readable(self) -> bool:
        """Check if the stream is readable."""
        return False

    def writable(self) -> bool:
        """Check if the stream is writable."""
        result = not self._closed
        print(f"DEBUG: GravitinoOutputStream.writable() called, returning {result}")
        return result

    def seekable(self) -> bool:
        """Check if the stream is seekable."""
        return False

    def tell(self) -> int:
        """Get the current position in the stream."""
        return self.buffer.tell()

    def read(self, size: int = -1) -> bytes:
        """Read from the stream (not supported for output streams)."""
        raise NotImplementedError("Cannot read from output stream")

    def seek(self, pos: int, whence: int = 0) -> int:
        """Seek in the stream (not supported)."""
        raise NotImplementedError("Seeking not supported in Gravitino output stream")

    def size(self) -> int:
        """Get the size of the stream."""
        return self.buffer.tell()

    def mode(self) -> str:
        """Get the mode of the stream."""
        print("DEBUG: GravitinoOutputStream.mode() called, returning 'wb'")
        return "wb"

    def fileno(self) -> int:
        """Get the file descriptor (not supported)."""
        raise NotImplementedError("fileno not supported for Gravitino output stream")

    def isatty(self) -> bool:
        """Check if the stream is a TTY."""
        return False

    def truncate(self, size: int | None = None) -> int:
        """Truncate the stream."""
        if self._closed:
            raise ValueError("Cannot truncate closed stream")
        if size is None:
            size = self.buffer.tell()
        self.buffer.truncate(size)
        return size
