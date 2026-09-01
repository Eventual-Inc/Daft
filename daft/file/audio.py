from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.datatype import MediaType
from daft.dependencies import librosa, np, sf
from daft.file import File
from daft.file.file import BUFFER_COPY, BUFFER_METADATA
from daft.file.typing import AudioMetadata

if TYPE_CHECKING:
    from daft.daft import PyFileReference
    from daft.io import IOConfig


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
        with self.open(buffer_size=BUFFER_METADATA) as f, sf.SoundFile(f) as af:
            return AudioMetadata(
                sample_rate=af.samplerate,
                channels=af.channels,
                frames=af.frames,
                format=af.format,
                subtype=af.subtype,
            )

    def to_numpy(self, buffer_size: int = BUFFER_COPY) -> np.ndarray[Any, np.dtype[np.float64]]:
        """Convert the audio file to a numpy array.

        Args:
            buffer_size (int): The buffer size to use for the temporary file.

        Returns:
            np.ndarray[Any, Any]: The audio data as a numpy array.

        """
        with self.to_tempfile(buffer_size) as tmp:
            audio, _ = sf.read(tmp)
            return audio

    def resample(self, sample_rate: int, buffer_size: int = BUFFER_COPY) -> np.ndarray[Any, np.dtype[np.float64]]:
        """Resample the audio file to the given sample rate.

        Args:
            sample_rate (int): The new sample rate.
            buffer_size (int): The buffer size to use for the temporary file.

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
        with self.to_tempfile(buffer_size) as f:
            data, samplerate = sf.read(f)
            if samplerate != sample_rate:
                resampled_data = librosa.resample(data, orig_sr=samplerate, target_sr=sample_rate)
                return resampled_data
            else:
                return data
