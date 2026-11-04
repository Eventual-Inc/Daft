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
    """Get metadata for an audio file.

    Requires the ``soundfile`` package; install it with
    ``pip install "daft[audio]"``. Calling this function without it raises an
    ``ImportError``.

    Args:
        file_expr (AudioFile Expression): The audio file to get metadata for.

    Returns:
        Expression (Struct Expression): A struct containing the metadata of the
        audio file, with the following fields:

            - ``sample_rate`` (``Int64``): Sample rate in Hz.
            - ``channels`` (``Int64``): Number of channels.
            - ``frames`` (``Float64``): Number of audio frames. Note that
              ``daft.AudioFile.metadata()`` returns this as a Python ``int``.
            - ``format`` (``String``): Container format, for example ``"MP3"``.
            - ``subtype`` (``String``): Sample encoding; values may be null when
              the container does not report one.

    Examples:
        >>> import daft
        >>> from daft.functions import audio_file, audio_metadata, resample

        >>> # Discover the audio files (returns a dataframe with ['path', 'size'] columns)
        >>> df = daft.from_glob_path("hf://datasets/Eventual-Inc/sample-files/audio/*.mp3")

        >>> df = (
        ...     df.with_column("file", audio_file(daft.col("path")))
        ...     .with_column("metadata", audio_metadata(daft.col("file")))
        ...     .select("path", "size", "metadata")
        ... )
        >>> df.show(3)  # doctest: +SKIP
        ╭────────────────────────────────┬─────────┬──────────────────────────────────────────────────────────────────────╮
        │ path                           ┆ size    ┆ metadata                                                             │
        │ ---                            ┆ ---     ┆ ---                                                                  │
        │ String                         ┆ Int64   ┆ Struct[sample_rate: Int64, channels: Int64, frames: Float64, format: │
        │                                ┆         ┆ String, subtype: String]                                             │
        ╞════════════════════════════════╪═════════╪══════════════════════════════════════════════════════════════════════╡
        │ hf://datasets/Eventual-Inc/sa… ┆ 822924  ┆ {sample_rate: 16000,                                                 │
        │                                ┆         ┆ channels…                                                            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hf://datasets/Eventual-Inc/sa… ┆ 618408  ┆ {sample_rate: 16000,                                                 │
        │                                ┆         ┆ channels…                                                            │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hf://datasets/Eventual-Inc/sa… ┆ 1190736 ┆ {sample_rate: 16000,                                                 │
        │                                ┆         ┆ channels…                                                            │
        ╰────────────────────────────────┴─────────┴──────────────────────────────────────────────────────────────────────╯
        <BLANKLINE>
        (Showing first 3 rows)
    """
    # Check for required dependencies at expression definition time
    from daft.dependencies import sf

    if not sf.module_available():
        raise ImportError(
            "The 'soundfile' module is required to get audio metadata. "
            "Please install it with: pip install 'daft[audio]'"
        )

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
    """Resample an audio file.

    Requires the ``soundfile`` and ``librosa`` packages; install them with
    ``pip install "daft[audio]"``. Calling this function without either raises
    an ``ImportError``.

    Resampling is only well-defined for mono files. For multichannel input the
    underlying ``librosa`` call uses its default axis, so the result should not
    be relied on; resample each channel separately instead.

    Args:
        file_expr (AudioFile Expression): The audio file to resample.
        sample_rate (int): The target sample rate in Hz.

    Returns:
        Expression (Tensor[Float64] Expression): The resampled audio samples. A
        mono file yields a tensor with shape ``(frames,)``; multichannel input
        follows the ``librosa`` output shape.

    Example:
        >>> import daft
        >>> from daft.functions import audio_file, resample

        >>> # Discover the audio files (returns a dataframe with ['path', 'size'] columns)
        >>> df = daft.from_glob_path("hf://datasets/Eventual-Inc/sample-files/audio/*.mp3")

        >>> df = (
        ...     df.with_column("file", audio_file(daft.col("path")))
        ...     .with_column("resampled", resample(daft.col("file"), sample_rate=16000))
        ...     .select("path", "size", "resampled")
        ... )
        >>> df.show(3)  # doctest: +SKIP
        ╭────────────────────────────────┬─────────┬──────────────────────────╮
        │ path                           ┆ size    ┆ resampled                │
        │ ---                            ┆ ---     ┆ ---                      │
        │ String                         ┆ Int64   ┆ Tensor[Float64]          │
        ╞════════════════════════════════╪═════════╪══════════════════════════╡
        │ hf://datasets/Eventual-Inc/sa… ┆ 822924  ┆ <Tensor shape=(2490278)> │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hf://datasets/Eventual-Inc/sa… ┆ 618408  ┆ <Tensor shape=(2207923)> │
        ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
        │ hf://datasets/Eventual-Inc/sa… ┆ 1190736 ┆ <Tensor shape=(3341800)> │
        ╰────────────────────────────────┴─────────┴──────────────────────────╯
        <BLANKLINE>
        (Showing first 3 rows)
    """
    # Check for required dependencies at expression definition time
    # This gives users a helpful error immediately, rather than during execution
    from daft.dependencies import librosa, sf

    if not sf.module_available():
        raise ImportError(
            "The 'soundfile' module is required to resample audio files. "
            "Please install it with: pip install 'daft[audio]'"
        )
    if not librosa.module_available():
        raise ImportError(
            "The 'librosa' module is required to resample audio files. "
            "Please install it with: pip install 'daft[audio]'"
        )

    return resample_fn(file_expr, sample_rate)
