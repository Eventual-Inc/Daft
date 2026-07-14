"""MCAP file metadata expressions."""

from __future__ import annotations

from typing import TYPE_CHECKING

import daft
from daft.file.mcap import McapFile
from daft.file.typing import McapTimeRange
from daft.udf.udf_v2 import Func

if TYPE_CHECKING:
    from daft import Expression


def mcap_metadata_impl(file: McapFile) -> str:
    return file.metadata_json()


mcap_metadata_fn = Func._from_func(
    mcap_metadata_impl,
    return_dtype=daft.DataType.string(),
    unnest=False,
    use_process=False,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="mcap_metadata",
)


def mcap_metadata(file_expr: Expression) -> Expression:
    """Read an MCAP header/summary catalog as JSON using range requests.

    The JSON shape matches :meth:`daft.McapFile.metadata`. Unindexed MCAPs
    report ``indexed=false`` without falling back to a full data-section scan.
    """
    return mcap_metadata_fn(file_expr)


def mcap_topics_impl(file: McapFile) -> list[str]:
    return file.topics()


mcap_topics_fn = Func._from_func(
    mcap_topics_impl,
    return_dtype=daft.DataType.list(daft.DataType.string()),
    unnest=False,
    use_process=False,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="mcap_topics",
)


def mcap_topics(file_expr: Expression) -> Expression:
    """Return the topics declared by an MCAP's indexed channels."""
    return mcap_topics_fn(file_expr)


def mcap_message_count_impl(file: McapFile) -> int | None:
    return file.message_count()


mcap_message_count_fn = Func._from_func(
    mcap_message_count_impl,
    return_dtype=daft.DataType.uint64(),
    unnest=False,
    use_process=False,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="mcap_message_count",
)


def mcap_message_count(file_expr: Expression) -> Expression:
    """Return the indexed MCAP message count, or null without statistics."""
    return mcap_message_count_fn(file_expr)


def mcap_time_range_impl(file: McapFile) -> McapTimeRange:
    return file.time_range()


mcap_time_range_fn = Func._from_func(
    mcap_time_range_impl,
    return_dtype=daft.DataType.struct(
        {
            "start_time": daft.DataType.uint64(),
            "end_time": daft.DataType.uint64(),
        }
    ),
    unnest=False,
    use_process=False,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
    name_override="mcap_time_range",
)


def mcap_time_range(file_expr: Expression) -> Expression:
    """Return the indexed inclusive start/end log-time bounds."""
    return mcap_time_range_fn(file_expr)
