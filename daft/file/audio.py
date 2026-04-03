from __future__ import annotations

import io
import pathlib
from typing import TYPE_CHECKING, Any, TypedDict

from daft.daft import io_put
from daft.datatype import MediaType
from daft.dependencies import librosa, np, sf
from daft.file import File
from daft.file.typing import AudioMetadata

if TYPE_CHECKING:
    from daft.daft import PyFileReference
    from daft.io import IOConfig

_CLOUD_SCHEMES = ("s3://", "gs://", "gcs://", "az://", "abfs://", "hf://", "http://", "https://")


class WriteAudioMetadata(TypedDict):
    sample_rate: int
    channels: int
    frames: float
    format: str
    subtype: str | None


class AudioFile(File):
    """An audio-specific file interface that provides audio operations."""

    @staticmethod
    def _from_file_reference(reference: PyFileReference) -> AudioFile:
        instance = AudioFile.__new__(AudioFile)
        instance._inner = reference
        return instance

    def __init__(self, url: str, io_config: IOConfig | None = None) -> None:
        if not sf.module_available():
            raise ImportError(
                "The 'soundfile' module is required to create audio files. "
                "Please add 'daft[audio]' to your dependencies or install it with: pip install 'daft[audio]'"
            )
        super().__init__(url, io_config, MediaType.audio())

    def __post_init__(self) -> None:
        if not self.is_audio():
            raise ValueError(f"File {self} is not an audio file")

    def metadata(self) -> AudioMetadata:
        """Extract basic audio metadata from container headers.

        Returns:
            AudioMetadata: Audio metadata object containing:
                - sample_rate: int - The sample rate of the audio file
                - channels: int - The number of channels in the audio file
                - frames: int - The number of frames in the audio file
                - format: str - The format of the audio file
                - subtype: str | None - The subtype of the audio file
        """
        with self.open() as f:
            with sf.SoundFile(f) as af:
                return AudioMetadata(
                    sample_rate=af.samplerate,
                    channels=af.channels,
                    frames=af.frames,
                    format=af.format,
                    subtype=af.subtype,
                )

    def to_numpy(self) -> np.ndarray[Any, np.dtype[np.float64]]:
        """Convert the audio file to a numpy array.

        Returns:
            np.ndarray[Any, Any]: The audio data as a numpy array.

        """
        with self.to_tempfile() as tmp:
            audio, _ = sf.read(tmp)
            return audio

    def resample(self, sample_rate: int) -> np.ndarray[Any, np.dtype[np.float64]]:
        """Resample the audio file to the given sample rate.

        Args:
            sample_rate (int): The new sample rate.

        Returns:
            AudioFile: The resampled audio file.

        """
        if not librosa.module_available():
            raise ImportError(
                "The 'librosa' module is required to resample audio files. "
                "Please install it with: pip install 'daft[audio]'"
            )
        if not sf.module_available():
            raise ImportError(
                "The 'soundfile' module is required to resample audio files. "
                "Please install it with: pip install 'daft[audio]'"
            )
        with self.to_tempfile() as f:
            data, samplerate = sf.read(f)
            if samplerate != sample_rate:
                resampled_data = librosa.resample(data, orig_sr=samplerate, target_sr=sample_rate)
                return resampled_data
            else:
                return data

    def write(
        self,
        destination: str,
        sample_rate: int | None = None,
        format: str | None = None,
        subtype: str | None = None,
    ) -> WriteAudioMetadata:
        """Write this audio file to a destination path."""
        return write_audio(self, destination, sample_rate=sample_rate, format=format, subtype=subtype)


def write_audio(
    audio: AudioFile | Any,
    destination: str,
    sample_rate: int | None = None,
    format: str | None = None,
    subtype: str | None = None,
) -> WriteAudioMetadata:
    """Write audio data from an AudioFile or array-like object to a destination path."""
    if isinstance(audio, AudioFile):
        meta = audio.metadata()
        data = audio.to_numpy()
        if sample_rate is None:
            sample_rate = meta["sample_rate"]
    else:
        data = np.asarray(audio)
        if sample_rate is None:
            raise ValueError(
                "sample_rate is required when writing tensor/array data. "
                "Pass sample_rate to write_audio() or use an AudioFile input."
            )

    # Normalize channel layout: channel-first -> channel-last for soundfile.
    if data.ndim == 2 and data.shape[0] < data.shape[1]:
        data = data.T

    data = np.clip(data.astype(np.float32), -1.0, 1.0)

    ext = pathlib.PurePosixPath(destination).suffix.lstrip(".")
    inferred_format = ext.upper() if ext else None
    write_format = format or inferred_format

    is_cloud = any(destination.startswith(scheme) for scheme in _CLOUD_SCHEMES)
    if is_cloud:
        if write_format is None:
            raise ValueError("Cannot infer audio format from destination path. Please specify the 'format' parameter.")
        buf = io.BytesIO()
        sf.write(buf, data, samplerate=sample_rate, format=write_format, subtype=subtype)
        io_put(destination, buf.getvalue())
    else:
        pathlib.Path(destination).parent.mkdir(parents=True, exist_ok=True)
        sf.write(destination, data, samplerate=sample_rate, format=write_format, subtype=subtype)

    channels = data.shape[1] if data.ndim == 2 else 1
    frames = float(data.shape[0])
    return WriteAudioMetadata(
        sample_rate=sample_rate,
        channels=channels,
        frames=frames,
        format=write_format or "",
        subtype=subtype,
    )
