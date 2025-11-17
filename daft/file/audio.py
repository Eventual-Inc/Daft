from __future__ import annotations

from typing import TYPE_CHECKING, Any

from daft.datatype import MediaType
from daft.dependencies import librosa, np, sf
from daft.file import File
from daft.file.typing import AudioMetadata

if TYPE_CHECKING:
    from daft.daft import PyFileReference
    from daft.io import IOConfig


class AudioFile(File):
    """A audio-specific file interface that provides audio operations."""

    @staticmethod
    def _from_file_reference(reference: PyFileReference) -> AudioFile:
        instance = AudioFile.__new__(AudioFile)
        instance._inner = reference
        return instance

    def __init__(self, url: str, io_config: IOConfig | None = None) -> None:
        if not sf.module_available():
            raise ImportError("The 'soundfile' module is required to create audio files.")
        super().__init__(url, io_config, MediaType.audio())

    def __post_init__(self) -> None:
        if not self.is_audio():
            raise ValueError(f"File {self} is not a audio file")

    def metadata(self) -> AudioMetadata:
        """Extract basic audio metadata from container headers.

        Returns:
            AudioMetadata: Audio metadata object containing width, height, fps, frame_count, time_base, keyframe_pts, keyframe_indices

        """
        with self._open_soundfile() as af:
            return AudioMetadata(
                sample_rate=af.samplerate,
                channels=af.channels,
                frames=af.frames,
                format=af.format,
                subtype=af.subtype,
            )

    def _open_soundfile(self) -> sf.SoundFile:
        with self.open() as f:
            return sf.SoundFile(f)

    def resample(self, sample_rate: int) -> np.ndarray[Any]:
        """Resample the audio file to the given sample rate.

        Args:
            sample_rate (int): The new sample rate.

        Returns:
            AudioFile: The resampled audio file.

        """
        if not librosa.module_available():
            raise ImportError("The 'librosa' module is required to resample audio files.")
        with self.open() as f:
            with sf.SoundFile(f) as af:
                data = af.read()
                if af.samplerate != sample_rate:
                    resampled_data = librosa.resample(data, orig_sr=af.samplerate, target_sr=sample_rate)
                    return resampled_data
                else:
                    return data
