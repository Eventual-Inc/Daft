"""Audio Functions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import daft
from daft.file.audio import write_audio as write_audio_file
from daft.udf.udf_v2 import Func

if TYPE_CHECKING:
    from daft import Expression
    from daft.file.audio import WriteAudioMetadata
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
        of the audio file.

        The struct contains the following fields:
            - sample_rate: int - The sample rate of the audio file
            - channels: int - The number of channels in the audio file
            - frames: int - The number of frames in the audio file
            - format: str - The format of the audio file
            - subtype: str | None - The subtype of the audio file

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
    """Resample a audio file.

    Args:
        file_expr (AudioFile Expression): The audio file to resample.
        sample_rate (int): The sample rate to resample to.

    Returns:
        Expression (Tensor[Python] Expression): The resampled audio file.

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
        >>> df.show(3)
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


def write_audio_impl(
    audio: Any,
    destination: str,
    sample_rate: int | None,
    format: str | None,
    subtype: str | None,
) -> WriteAudioMetadata:
    return write_audio_file(audio, destination, sample_rate=sample_rate, format=format, subtype=subtype)


write_audio_fn = Func._from_func(
    write_audio_impl,
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


def write_audio(
    audio_expr: Expression,
    destination: Expression | str,
    sample_rate: int | None = None,
    format: str | None = None,
    subtype: str | None = None,
) -> Expression:
    """Write audio data to a file.

    Mirrors the soundfile write API. Accepts either an AudioFile expression
    (sample rate inferred from metadata) or a Tensor/array expression
    (sample rate required). Audio data is clipped to [-1.0, 1.0] and
    channel layout is normalized automatically.

    Supports local and cloud (S3, GCS, Azure) destinations.

    Args:
        audio_expr: An AudioFile or Tensor[Float64] expression containing audio data.
        destination: A String expression with destination file paths, or a literal string.
        sample_rate: The sample rate in Hz. Required for tensor/array input.
            Inferred from AudioFile metadata if not specified.
        format: Audio format (e.g., "WAV", "MP3", "FLAC", "OGG").
            Inferred from destination file extension if not specified.
        subtype: Audio subtype (e.g., "PCM_16", "MPEG_LAYER_III").
            If not specified, soundfile chooses a default for the format.

    Returns:
        Expression: A struct expression containing AudioMetadata:
            - sample_rate: int
            - channels: int
            - frames: float
            - format: str
            - subtype: str | None

    Example:
        >>> import daft
        >>> from daft.functions import audio_file, write_audio

        >>> df = daft.from_glob_path("audio/*.mp3")
        >>> df = (
        ...     df.with_column("file", audio_file(daft.col("path")))
        ...     .with_column("out_path", daft.col("path").replace(".mp3", ".wav"))
        ...     .with_column("result", write_audio(daft.col("file"), daft.col("out_path")))
        ... )
    """
    from daft.dependencies import sf

    if not sf.module_available():
        raise ImportError(
            "The 'soundfile' module is required to write audio files. Please install it with: pip install 'daft[audio]'"
        )

    if isinstance(destination, str):
        destination = daft.lit(destination)

    return cast("Expression", write_audio_fn(audio_expr, destination, sample_rate, format, subtype))
