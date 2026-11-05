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
    """An audio-specific file interface that provides audio operations.

    Instances are usually created with :func:`daft.functions.audio_file`, which
    only creates a file reference, or directly with ``AudioFile(url)``. These
    optional dependencies are required:

    - ``soundfile`` for direct construction with ``AudioFile(url)``,
      :meth:`metadata`, and :meth:`to_numpy`.
    - ``librosa`` for :meth:`resample`.

    Install both with ``pip install "daft[audio]"``. Using an operation whose
    dependency is missing raises an ``ImportError``.
    """

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

        Requires the ``soundfile`` package (``pip install "daft[audio]"``).

        Returns:
            AudioMetadata: A dictionary with the following fields:

                - ``sample_rate`` (``int``): Sample rate in Hz.
                - ``channels`` (``int``): Number of channels.
                - ``frames`` (``int``): Number of audio frames, that is, samples per channel.
                - ``format`` (``str``): Container format, for example ``"MP3"`` or ``"WAV"``.
                - ``subtype`` (``str | None``): Sample encoding, or ``None`` when the
                  container does not report one.

        Note:
            The expression-level :func:`daft.functions.audio_metadata` exposes
            ``frames`` as ``Float64``, while this method returns an ``int``.
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
        """Convert the audio file to a NumPy array.

        Requires the ``soundfile`` package (``pip install "daft[audio]"``).

        Args:
            buffer_size (int): The buffer size to use for the temporary file.

        Returns:
            A ``float64`` NumPy array with shape ``(frames,)`` for mono files or
            ``(frames, channels)`` for files with more than one channel.
        """
        with self.to_tempfile(buffer_size) as tmp:
            audio, _ = sf.read(tmp)
            return audio

    def resample(self, sample_rate: int, buffer_size: int = BUFFER_COPY) -> np.ndarray[Any, np.dtype[np.float64]]:
        """Resample the audio file to the given sample rate.

        Requires the ``soundfile`` and ``librosa`` packages
        (``pip install "daft[audio]"``).

        Resampling is only well-defined for mono files. For multichannel input
        the underlying ``librosa`` call uses its default axis, so the result
        should not be relied on; resample each channel separately instead. When
        the file already has the requested sample rate, the decoded samples are
        returned unchanged.

        Args:
            sample_rate (int): The target sample rate in Hz.
            buffer_size (int): The buffer size to use for the temporary file.

        Returns:
            A ``float64`` NumPy array of resampled samples, with shape
            ``(frames,)`` for mono input and the ``librosa`` output shape
            otherwise.
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
