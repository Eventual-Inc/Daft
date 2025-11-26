"""Audio Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import daft
from daft.udf.udf_v2 import Func

if TYPE_CHECKING:
    from daft import Expression
    from daft.file.typing import AudioMetadata


def get_metadata_impl(
    file: daft.AudioFile,
) -> AudioMetadata:
    return file.metadata()


audio_metadata_fn = Func._from_func(
    get_metadata_impl,
    return_dtype=daft.DataType.struct(
        {
            "sample_rate": daft.DataType.int64(),
            "channels": daft.DataType.int64(),
            "frames": daft.DataType.float64(),
            "format": daft.DataType.string(),
            "subtype": daft.DataType.string(),
        }
    ),
    unnest=False,
    use_process=None,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
)


def audio_metadata(
    file_expr: Expression,
) -> Expression:
    """Get metadata for a audio file.

    Args:
        file_expr (AudioFile Expression): The audio file to get metadata for.

    Returns:
        Expression (Struct Expression): A struct containing the metadata
    """
    return audio_metadata_fn(file_expr)


def resample_impl(
    file: daft.AudioFile,
    sample_rate: int,
) -> Any:
    return file.resample(sample_rate)


resample_fn = Func._from_func(
    resample_impl,
    return_dtype=daft.DataType.tensor(daft.DataType.float64()),
    unnest=False,
    use_process=None,
    is_batch=False,
    batch_size=None,
    max_retries=None,
    on_error=None,
)


def resample(
    file_expr: Expression,
    sample_rate: int,
) -> Expression:
    """Resample a audio file.

    Args:
        file_expr (AudioFile Expression): The audio file to resample.
        sample_rate (int): The sample rate to resample to.

    Returns:
        Expression (Tensor[Python] Expression): The resampled audio file.
    """
    return resample_fn(file_expr, sample_rate)
