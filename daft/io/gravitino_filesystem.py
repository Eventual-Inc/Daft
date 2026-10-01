"""PyArrow filesystem implementation for Gravitino gvfs:// URLs."""

from __future__ import annotations

import asyncio
import io
import logging
import os
import threading
from typing import Any, Literal, cast
from urllib.parse import urlsplit

from daft.daft import io_get, io_get_size, io_ls, io_put
from daft.dependencies import pa, pafs
from daft.io import GravitinoConfig, IOConfig
from daft.io.extensions import IOExtension, IOFileInfo, IOFileType, IOListing, IOReadRange


class GravitinoIOExtension(IOExtension):
    """Gravitino implementation of Daft's async Python IO extension interface."""

    def __init__(self, config: GravitinoConfig):
        self._initialize(config)

    def _initialize(self, config: GravitinoConfig) -> None:
        if config.endpoint is None:
            raise ValueError("GravitinoConfig.endpoint must be provided to create a Gravitino IO extension")
        if config.metalake_name is None:
            raise ValueError("GravitinoConfig.metalake_name must be provided to create a Gravitino IO extension")

        from daft.catalog.__gravitino._client import GravitinoClient

        self._config = config
        auth_type = config.auth_type or "simple"
        if auth_type not in ("simple", "oauth2"):
            raise ValueError(f"Unsupported Gravitino auth type: {auth_type}")
        self._client = GravitinoClient(
            config.endpoint,
            config.metalake_name,
            auth_type=cast("Literal['simple', 'oauth2']", auth_type),
            username=config.username,
            password=config.password,
            token=config.token,
        )
        self._filesets: dict[str, tuple[str, IOConfig]] = {}
        self._lock = threading.Lock()

    def __getstate__(self) -> dict[str, GravitinoConfig]:
        return {"config": self._config}

    def __setstate__(self, state: dict[str, GravitinoConfig]) -> None:
        self._initialize(state["config"])

    @staticmethod
    def _parse_path(path: str) -> tuple[str, str, str]:
        parsed = urlsplit(path)
        parts = parsed.path.strip("/").split("/")
        if parsed.scheme != "gvfs" or parsed.netloc != "fileset" or len(parts) < 3 or not all(parts[:3]):
            raise FileNotFoundError(
                "Expected Gravitino fileset path to be in the form "
                f"`gvfs://fileset/catalog/schema/fileset/path`, instead found: {path}"
            )
        fileset_name = ".".join(parts[:3])
        fileset_root = f"gvfs://fileset/{'/'.join(parts[:3])}"
        relative_path = "/".join(parts[3:])
        return fileset_name, fileset_root, relative_path

    def _resolve_sync(self, path: str) -> tuple[str, IOConfig, str, str]:
        fileset_name, fileset_root, relative_path = self._parse_path(path)
        with self._lock:
            resolved = self._filesets.get(fileset_name)
            if resolved is None:
                fileset = self._client.load_fileset(fileset_name)
                io_config = fileset.io_config or IOConfig()
                storage_root = fileset.fileset_info.storage_location.rstrip("/")
                resolved = (storage_root, io_config)
                self._filesets[fileset_name] = resolved

        storage_root, io_config = resolved
        source_path = storage_root if not relative_path else f"{storage_root}/{relative_path}"
        return source_path, io_config, fileset_root, storage_root

    async def resolve_url(self, path: str) -> tuple[str, IOConfig]:
        source_path, io_config, _, _ = await asyncio.to_thread(self._resolve_sync, path)
        return source_path, io_config

    async def supports_range(self, path: str) -> bool:
        return True

    async def get(self, path: str, byte_range: IOReadRange | None = None) -> bytes:
        source_path, io_config, _, _ = await asyncio.to_thread(self._resolve_sync, path)
        kwargs: dict[str, int] = {}
        if byte_range is not None:
            if byte_range.suffix is not None:
                kwargs["suffix"] = byte_range.suffix
            else:
                assert byte_range.start is not None
                kwargs["range_start"] = byte_range.start
                if byte_range.end is not None:
                    kwargs["range_end"] = byte_range.end
        return await asyncio.to_thread(
            io_get,
            path=source_path,
            multithreaded_io=True,
            io_config=io_config,
            **kwargs,
        )

    async def put(self, path: str, data: bytes) -> None:
        source_path, io_config, _, _ = await asyncio.to_thread(self._resolve_sync, path)
        await asyncio.to_thread(
            io_put,
            path=source_path,
            data=data,
            multithreaded_io=True,
            io_config=io_config,
        )

    async def get_size(self, path: str) -> int:
        source_path, io_config, _, _ = await asyncio.to_thread(self._resolve_sync, path)
        return await asyncio.to_thread(
            io_get_size,
            path=source_path,
            multithreaded_io=True,
            io_config=io_config,
        )

    async def ls(
        self,
        path: str,
        *,
        posix: bool,
        continuation_token: str | None = None,
        page_size: int | None = None,
    ) -> IOListing:
        source_path, io_config, fileset_root, storage_root = await asyncio.to_thread(self._resolve_sync, path)
        files, next_token, not_found_if_empty = await asyncio.to_thread(
            io_ls,
            path=source_path,
            posix=posix,
            continuation_token=continuation_token,
            page_size=page_size,
            multithreaded_io=True,
            io_config=io_config,
        )
        rewritten = []
        for file in files:
            file_path = file["path"]
            if file_path == storage_root:
                file_path = fileset_root
            elif file_path.startswith(f"{storage_root}/"):
                file_path = f"{fileset_root}{file_path[len(storage_root) :]}"
            file_type = IOFileType.FILE if file["type"] == "File" else IOFileType.DIRECTORY
            rewritten.append(IOFileInfo(path=file_path, size=file["size"], file_type=file_type))
        return IOListing(
            files=rewritten,
            continuation_token=next_token,
            not_found_if_empty=not_found_if_empty,
        )


class GravitinoFileSystemHandler:
    """FSSpec-like handler for Gravitino gvfs:// URLs.

    This handler delegates operations to Daft's Rust-based Gravitino implementation,
    allowing PyArrow-based operations (like parquet writing) to work with gvfs:// URLs.
    """

    def __init__(self, io_config: IOConfig | None = None):
        """Initialize the Gravitino filesystem handler.

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
            "Direct streaming from gvfs:// not yet implemented. Use daft.read_parquet() instead for reading operations."
        )

    def open(self, path: str, mode: str = "rb", **kwargs: Any) -> GravitinoOutputStream:
        """Open a file for reading or writing.

        This is the FSSpec-style interface used by PyArrow's FSSpecHandler.
        """
        if mode in ("wb", "w"):
            return GravitinoOutputStream(path, self.io_config)
        else:
            raise NotImplementedError(f"Mode {mode} not supported for gvfs:// paths")

    def open_output_stream(self, path: str, metadata: dict[str, str] | None = None, **kwargs: Any) -> pa.NativeFile:
        """Open an output stream for writing to the given path."""
        # Accept any additional kwargs that PyArrow might pass (like compression)
        return GravitinoOutputStream(path, self.io_config)

    def create_dir(self, path: str, *, recursive: bool = True) -> None:
        """Create a directory. For gvfs://, this is typically a no-op."""
        # Gravitino filesets don't require explicit directory creation

    def delete_dir(self, path: str) -> None:
        """Delete a directory."""
        raise NotImplementedError("Directory deletion not implemented for gvfs://")

    def exists(self, path: str) -> bool:
        """Check if a path exists."""
        # For now, assume paths exist - actual validation happens in Rust layer
        return True

    def rm(self, path: str, recursive: bool = False) -> None:
        """Remove a file or directory."""
        raise NotImplementedError("File deletion not implemented for gvfs://")

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
        self.path = path
        self.io_config = io_config or IOConfig()
        self.buffer = io.BytesIO()
        self._closed = False

    def __fspath__(self) -> str:
        """Return the file system path representation."""
        return self.path

    def __getattr__(self, name: str) -> Any:
        """Handle missing attributes."""
        if name in ["__fspath__"]:
            return lambda: self.path

        # Return a dummy function for any missing method
        def dummy_method(*args: Any, **kwargs: Any) -> Any:
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
        if self._closed:
            raise ValueError("Cannot write to closed stream")
        return self.buffer.write(data)

    def flush(self) -> None:
        """Flush the buffer."""
        if not self._closed:
            self.buffer.flush()

    def close(self) -> None:
        """Close the stream and write the buffered data to Gravitino."""
        if self._closed:
            return

        try:
            # Get the buffered data
            data = self.buffer.getvalue()

            # Write the data using Daft's Rust layer via the low-level interface
            if len(data) > 0:
                self._write_to_gravitino(data)

        finally:
            self.buffer.close()
            self._closed = True

    def __del__(self) -> None:
        """Ensure the stream is closed when the object is garbage collected."""
        if not self._closed:
            try:
                self.close()
            except Exception:
                pass  # Ignore errors during cleanup

    def __enter__(self) -> GravitinoOutputStream:
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager."""
        self.close()

    def _write_to_gravitino(self, data: bytes) -> None:
        """Write data to Gravitino using Daft's Rust layer."""
        # Use the new io_put function directly to write the bytes to gvfs://
        try:
            io_put(
                path=self.path,
                data=data,  # Pass bytes directly to Rust
                multithreaded_io=True,
                io_config=self.io_config,
            )
        except Exception as e:
            # Log the error but don't fail silently
            logger = logging.getLogger(__name__)
            logger.error("Failed to write to %s: %s", self.path, e)
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
        return not self._closed

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


# PyArrow FileSystem wrapper
class GravitinoFileSystem(pafs.PyFileSystem):  # type: ignore[misc]
    """PyArrow FileSystem implementation for Gravitino gvfs:// URLs.

    This wraps GravitinoFileSystemHandler to provide a PyArrow-compatible filesystem.
    """

    def __init__(self, io_config: IOConfig | None = None):
        """Initialize the Gravitino filesystem.

        Args:
            io_config: IOConfig containing Gravitino configuration
        """
        handler = GravitinoFileSystemHandler(io_config=io_config)
        super().__init__(pafs.FSSpecHandler(handler))
