from __future__ import annotations

import json
from typing import TYPE_CHECKING, cast

from daft.datatype import MediaType
from daft.file.file import File
from daft.file.typing import (
    McapAttachmentMetadata,
    McapChannelMetadata,
    McapChunkMetadata,
    McapFileMetadata,
    McapHeader,
    McapMetadataRecord,
    McapSchemaMetadata,
    McapStatistics,
    McapTimeRange,
)

if TYPE_CHECKING:
    from daft.daft import PyDaftFile, PyFileReference
    from daft.io import IOConfig


MCAP_METADATA_BUFFER_SIZE = 1024 * 1024


class McapFile(File):
    """An MCAP container backed by Daft's range-readable file interface.

    Metadata access reads the leading header and the footer/summary ranges when
    the storage backend supports range requests. It does not materialize the
    complete file on range-capable remote object stores.
    """

    @staticmethod
    def _from_file_reference(reference: PyFileReference) -> McapFile:
        instance = McapFile.__new__(McapFile)
        instance._inner = reference
        return instance

    def __init__(self, url: str, io_config: IOConfig | None = None) -> None:
        super().__init__(url, io_config, MediaType.mcap())
        if not self.is_mcap():
            raise ValueError(f"File {self} is not an MCAP file")

    def open(self, buffer_size: int | None = MCAP_METADATA_BUFFER_SIZE) -> PyDaftFile:
        return super().open(buffer_size=buffer_size)

    def metadata_json(
        self,
        *,
        include_schema_data: bool = True,
        include_metadata_records: bool = True,
        include_chunk_indexes: bool = True,
    ) -> str:
        """Return the native MCAP metadata catalog as compact JSON."""
        with self.open() as file:
            return file._mcap_metadata_json(
                include_schema_data,
                include_metadata_records,
                include_chunk_indexes,
            )

    def _read_metadata(
        self,
        *,
        include_schema_data: bool = False,
        include_metadata_records: bool = False,
        include_chunk_indexes: bool = False,
    ) -> McapFileMetadata:
        return cast(
            "McapFileMetadata",
            json.loads(
                self.metadata_json(
                    include_schema_data=include_schema_data,
                    include_metadata_records=include_metadata_records,
                    include_chunk_indexes=include_chunk_indexes,
                )
            ),
        )

    def metadata(self) -> McapFileMetadata:
        """Read the header, summary indexes, and indexed metadata records.

        MCAP summary sections are optional. ``has_summary`` reports whether one
        exists. ``has_chunk_indexes`` and ``has_message_indexes`` report the
        two index levels used for read and count pushdown; ``indexed`` is a
        shorthand for ``has_chunk_indexes``. A summary can contain channels or
        statistics without either index. This method never falls back to
        scanning the complete data section.
        """
        return self._read_metadata(
            include_schema_data=True,
            include_metadata_records=True,
            include_chunk_indexes=True,
        )

    def summary(self) -> McapFileMetadata:
        """Read the MCAP summary without following metadata-record indexes."""
        return self._read_metadata(include_schema_data=True, include_chunk_indexes=True)

    def header(self) -> McapHeader:
        return self._read_metadata()["header"]

    def statistics(self) -> McapStatistics | None:
        return self._read_metadata()["statistics"]

    def schemas(self) -> list[McapSchemaMetadata]:
        return self._read_metadata(include_schema_data=True)["schemas"]

    def channels(self) -> list[McapChannelMetadata]:
        return self._read_metadata()["channels"]

    def chunks(self) -> list[McapChunkMetadata]:
        return self._read_metadata(include_chunk_indexes=True)["chunks"]

    def attachments(self) -> list[McapAttachmentMetadata]:
        return self._read_metadata()["attachments"]

    def metadata_records(self) -> list[McapMetadataRecord]:
        return self._read_metadata(include_metadata_records=True)["metadata"]

    def topics(self) -> list[str]:
        """Return unique topics in channel-id order."""
        return list(dict.fromkeys(channel["topic"] for channel in self.channels()))

    def message_count(self) -> int | None:
        statistics = self.statistics()
        return None if statistics is None else statistics["message_count"]

    def time_range(self) -> McapTimeRange:
        """Return the inclusive file time bounds reported by MCAP statistics."""
        statistics = self.statistics()
        if statistics is None:
            return McapTimeRange(start_time=None, end_time=None)
        return McapTimeRange(
            start_time=statistics["message_start_time"],
            end_time=statistics["message_end_time"],
        )
