from __future__ import annotations

from typing import TYPE_CHECKING

from daft.datatype import MediaType
from daft.file.file import File

if TYPE_CHECKING:
    from daft.daft import PyDaftFile, PyFileReference
    from daft.io import IOConfig


MCAP_BUFFER_SIZE = 1024 * 1024


class McapFile(File):
    """An MCAP container backed by Daft's range-readable file interface."""

    @staticmethod
    def _from_file_reference(reference: PyFileReference) -> McapFile:
        instance = McapFile.__new__(McapFile)
        instance._inner = reference
        return instance

    def __init__(self, url: str, io_config: IOConfig | None = None) -> None:
        super().__init__(url, io_config, MediaType.mcap())
        if not self.is_mcap():
            raise ValueError(f"File {self} is not an MCAP file")

    def open(self, buffer_size: int | None = MCAP_BUFFER_SIZE) -> PyDaftFile:
        return super().open(buffer_size=buffer_size)
