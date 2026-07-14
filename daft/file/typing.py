from __future__ import annotations

from typing import TYPE_CHECKING, Literal, TypedDict

if TYPE_CHECKING:
    import PIL


class VideoMetadata(TypedDict):
    width: int | None
    height: int | None
    fps: float | None
    duration: float | None
    frame_count: int | None
    time_base: float | None


class VideoFrameData(TypedDict):
    frame_index: int
    frame_time: float | None
    frame_time_base: str | None
    frame_pts: int | None
    frame_dts: int | None
    frame_duration: int | None
    is_key_frame: bool
    data: PIL.Image.Image


class AudioMetadata(TypedDict):
    sample_rate: int
    channels: int
    frames: int
    format: str
    subtype: str | None


class ImageMetadata(TypedDict):
    width: int | None
    height: int | None
    format: str | None
    mode: str | None


class Hdf5ObjectMetadata(TypedDict):
    h5path: str
    kind: Literal["dataset", "group"]
    shape: list[int]
    dtype: str
    chunks: list[int]
    compression: str


class McapHeader(TypedDict):
    profile: str
    library: str


class McapStatistics(TypedDict):
    message_count: int
    schema_count: int
    channel_count: int
    attachment_count: int
    metadata_count: int
    chunk_count: int
    message_start_time: int
    message_end_time: int


class McapSchemaMetadata(TypedDict):
    id: int
    name: str
    encoding: str
    data_size: int
    data_base64: str | None


class McapChannelMetadata(TypedDict):
    id: int
    topic: str
    message_encoding: str
    schema_id: int | None
    schema_name: str | None
    message_count: int | None
    metadata: dict[str, str]


class McapMessageIndexMetadata(TypedDict):
    channel_id: int
    position: int


class McapChunkMetadata(TypedDict):
    message_start_time: int
    message_end_time: int
    position: int
    size: int
    compression: str
    compressed_size: int
    uncompressed_size: int
    message_index_length: int
    message_index_offsets: list[McapMessageIndexMetadata]
    channel_ids: list[int]
    topics: list[str]


class McapAttachmentMetadata(TypedDict):
    position: int
    size: int
    log_time: int
    create_time: int
    data_size: int
    name: str
    media_type: str


class McapMetadataRecord(TypedDict):
    name: str
    metadata: dict[str, str]
    position: int
    size: int


class McapFileMetadata(TypedDict):
    file_size: int
    has_summary: bool
    indexed: bool
    has_chunk_indexes: bool
    has_message_indexes: bool
    header: McapHeader
    statistics: McapStatistics | None
    schemas: list[McapSchemaMetadata]
    channels: list[McapChannelMetadata]
    chunks: list[McapChunkMetadata]
    attachments: list[McapAttachmentMetadata]
    metadata: list[McapMetadataRecord]


class McapTimeRange(TypedDict):
    start_time: int | None
    end_time: int | None
