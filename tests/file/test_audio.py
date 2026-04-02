from __future__ import annotations

from unittest.mock import patch

import pytest

import daft


@pytest.fixture(scope="module")
def sample_audio_path():
    return "tests/assets/sample_audio.mp3"


def test_audio_file_standalone(sample_audio_path):
    import numpy as np
    import soundfile as sf

    file = daft.AudioFile(sample_audio_path)
    arr = file.to_numpy()

    expected, _ = sf.read(sample_audio_path)
    assert np.allclose(arr, expected)


def test_audio_file_dtype(sample_audio_path):
    df = daft.from_pydict({"path": [sample_audio_path]})
    df = df.select(daft.functions.audio_file(df["path"]).alias("audio"))

    field = next(df.schema().__iter__())

    assert field.dtype == daft.DataType.file(daft.MediaType.audio())


def test_audio_file_verify():
    df = daft.from_pydict({"path": ["tests/assets/sampled-tpch.jsonl"]})

    with pytest.raises(ValueError):
        df = df.select(daft.functions.audio_file(df["path"], verify=True).alias("audio"))
        df.collect()


def test_audio_file_verify_ok(sample_audio_path):
    df = daft.from_pydict({"path": [sample_audio_path]})

    df = df.select(daft.functions.audio_file(df["path"], verify=True).alias("audio"))
    df.collect()


def test_get_metadata(sample_audio_path):
    df = daft.from_pydict({"path": [sample_audio_path]})
    df = df.select(daft.functions.audio_file(df["path"], verify=True).alias("audio"))
    df = df.select(daft.functions.audio_metadata(df["audio"]))

    expected = {
        "audio": [
            {"sample_rate": 44100, "channels": 2, "frames": 434176.0, "format": "MP3", "subtype": "MPEG_LAYER_III"}
        ]
    }

    assert df.to_pydict() == expected


# resampling is pretty slow so it's integration test only
@pytest.mark.integration()
def test_resample(sample_audio_path):
    import librosa
    import numpy as np
    import soundfile as sf

    def manual_resample():
        data, samplerate = sf.read(sample_audio_path)
        return librosa.resample(data, orig_sr=samplerate, target_sr=48000)

    df = daft.from_pydict({"path": [sample_audio_path]})
    df = df.select(daft.functions.audio_file(df["path"], verify=True).alias("audio"))
    df = df.select(daft.functions.resample(df["audio"], 48000))

    actual = df.to_pydict()["audio"][0]

    assert np.allclose(actual, manual_resample())


def test_audiofile_init_without_soundfile():
    """Test that AudioFile.__init__ raises ImportError with helpful message when soundfile is missing."""
    with patch("daft.file.audio.sf.module_available", return_value=False):
        with pytest.raises(ImportError) as exc_info:
            daft.AudioFile("some/path.mp3")

        error_message = str(exc_info.value)
        assert "soundfile" in error_message
        assert "daft[audio]" in error_message


def test_resample_without_librosa(tmp_path):
    """Test that resample raises ImportError with helpful message when librosa is missing.

    This specifically tests the case where soundfile is installed but librosa is not,
    which previously caused a confusing 'Need at least 1 series to perform concat' error.
    """
    import numpy as np
    import soundfile as sf

    sample_rate = 44100
    duration = 0.1  # 100ms
    samples = int(sample_rate * duration)
    audio_data = np.sin(2 * np.pi * 440 * np.linspace(0, duration, samples))

    audio_path = tmp_path / "test.wav"
    sf.write(str(audio_path), audio_data, sample_rate)

    # Create AudioFile (should work since soundfile is available)
    audio_file = daft.AudioFile(str(audio_path))

    # Mock librosa as unavailable and verify helpful error
    with patch("daft.file.audio.librosa.module_available", return_value=False):
        with pytest.raises(ImportError) as exc_info:
            audio_file.resample(48000)

        error_message = str(exc_info.value)
        assert "librosa" in error_message
        assert "daft[audio]" in error_message


def test_resample_without_soundfile(tmp_path):
    """Test that resample raises ImportError with helpful message when soundfile is missing."""
    import numpy as np
    import soundfile as sf

    sample_rate = 44100
    duration = 0.1
    samples = int(sample_rate * duration)
    audio_data = np.sin(2 * np.pi * 440 * np.linspace(0, duration, samples))

    audio_path = tmp_path / "test.wav"
    sf.write(str(audio_path), audio_data, sample_rate)

    audio_file = daft.AudioFile(str(audio_path))

    # Mock soundfile as unavailable for the resample call
    with patch("daft.file.audio.sf.module_available", return_value=False):
        with pytest.raises(ImportError) as exc_info:
            audio_file.resample(48000)

        error_message = str(exc_info.value)
        assert "soundfile" in error_message
        assert "daft[audio]" in error_message


def test_resample_expression_without_librosa(tmp_path):
    """Test that creating a resample expression raises ImportError when librosa is missing.

    This tests that the error is raised at expression definition time,
    not during execution. Previously, users saw the confusing
    'DaftError::ValueError Need at least 1 series to perform concat' error.
    """
    import numpy as np
    import soundfile as sf

    sample_rate = 44100
    duration = 0.1
    samples = int(sample_rate * duration)
    audio_data = np.sin(2 * np.pi * 440 * np.linspace(0, duration, samples))

    audio_path = tmp_path / "test.wav"
    sf.write(str(audio_path), audio_data, sample_rate)

    df = daft.from_pydict({"path": [str(audio_path)]})
    df = df.select(daft.functions.audio_file(df["path"]).alias("audio"))

    # Mock librosa as unavailable - error should be raised when creating the expression
    with patch("daft.dependencies.librosa.module_available", return_value=False):
        with pytest.raises(ImportError) as exc_info:
            # Error is raised here, at expression definition time, not at collect()
            df.select(daft.functions.resample(df["audio"], 48000))

        error_message = str(exc_info.value)
        assert "librosa" in error_message
        assert "daft[audio]" in error_message


def test_audio_metadata_expression_without_soundfile():
    """Test that creating an audio_metadata expression raises ImportError when soundfile is missing."""
    df = daft.from_pydict({"path": ["some/path.mp3"]})

    # Mock soundfile as unavailable - error should be raised when creating the expression
    with patch("daft.dependencies.sf.module_available", return_value=False):
        with pytest.raises(ImportError) as exc_info:
            df.select(daft.functions.audio_metadata(df["path"]))

        error_message = str(exc_info.value)
        assert "soundfile" in error_message
        assert "daft[audio]" in error_message


def test_file_as_audio_without_soundfile(tmp_path):
    """Test that File.as_audio() raises ImportError with helpful message when soundfile is missing."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")

    file = daft.File(str(test_file))

    with patch("daft.file.file.sf.module_available", return_value=False):
        with pytest.raises(ImportError) as exc_info:
            file.as_audio()

        error_message = str(exc_info.value)
        assert "soundfile" in error_message
        assert "daft[audio]" in error_message


def test_write_audio_from_audiofile(sample_audio_path, tmp_path):
    """Test writing an AudioFile to a new WAV file."""
    import numpy as np
    import soundfile as sf

    output_path = str(tmp_path / "output.wav")

    df = daft.from_pydict({"path": [sample_audio_path], "output": [output_path]})
    df = df.with_column("audio", daft.functions.audio_file(df["path"]))
    df = df.with_column("result", daft.functions.write_audio(df["audio"], df["output"]))
    result = df.to_pydict()

    # Verify the file was written
    written_data, written_sr = sf.read(output_path)
    original_data, original_sr = sf.read(sample_audio_path)

    assert written_sr == original_sr
    assert written_data.shape == original_data.shape
    # WAV uses PCM encoding by default, so there's quantization error from float32 → PCM → float64
    assert np.allclose(written_data, np.clip(original_data.astype(np.float32), -1.0, 1.0), atol=1e-3)

    # Verify returned metadata
    meta = result["result"][0]
    assert meta["sample_rate"] == original_sr
    assert meta["format"] == "WAV"


def test_write_audio_from_tensor(tmp_path):
    """Test writing a tensor/numpy array to an audio file with explicit sample_rate."""
    import numpy as np
    import soundfile as sf

    sample_rate = 16000
    duration = 0.1
    samples = int(sample_rate * duration)
    audio_data = np.sin(2 * np.pi * 440 * np.linspace(0, duration, samples))

    output_path = str(tmp_path / "output.wav")

    # Create a dataframe with tensor data via a UDF
    df = daft.from_pydict({"audio": [audio_data], "output": [output_path]})
    df = df.with_column("result", daft.functions.write_audio(df["audio"], df["output"], sample_rate=sample_rate))
    result = df.to_pydict()

    # Verify the file was written correctly
    written_data, written_sr = sf.read(output_path)
    assert written_sr == sample_rate
    assert written_data.shape[0] == samples

    # Verify metadata
    meta = result["result"][0]
    assert meta["sample_rate"] == sample_rate
    assert meta["channels"] == 1
    assert meta["format"] == "WAV"


def test_file_write_audio_channel_first_multichannel_tensor(tmp_path):
    """Test that channel-first tensor input is written with the expected channel-last layout."""
    import numpy as np
    import soundfile as sf

    from daft.file.audio import write_audio

    sample_rate = 16000
    audio_data = np.linspace(-1.0, 1.0, 9 * 32, dtype=np.float64).reshape(9, 32)
    output_path = str(tmp_path / "channel_first.wav")

    meta = write_audio(audio_data, output_path, sample_rate=sample_rate, format=None, subtype=None)
    written_data, written_sr = sf.read(output_path)

    assert written_sr == sample_rate
    assert written_data.shape == (32, 9)
    assert meta["channels"] == 9
    assert meta["frames"] == 32.0


def test_file_write_audio_clips_audio_and_creates_parent_dirs(tmp_path):
    """Test clipping behavior and parent directory creation in the file audio implementation."""
    import numpy as np
    import soundfile as sf

    from daft.file.audio import write_audio

    audio_data = np.array([1.5, -1.5, 0.25], dtype=np.float64)
    output_path = str(tmp_path / "nested" / "clipped.wav")

    meta = write_audio(audio_data, output_path, sample_rate=16000, format=None, subtype=None)
    written_data, written_sr = sf.read(output_path)

    assert written_sr == 16000
    assert np.allclose(written_data, np.array([1.0, -1.0, 0.25]), atol=1e-3)
    assert meta["frames"] == 3.0


def test_file_write_audio_cloud_requires_format_without_extension():
    """Test that cloud destinations without an extension require an explicit format."""
    import numpy as np

    from daft.file.audio import write_audio

    audio_data = np.zeros(16, dtype=np.float64)

    with pytest.raises(ValueError, match="Cannot infer audio format"):
        write_audio(audio_data, "s3://bucket/output", sample_rate=16000, format=None, subtype=None)


def test_file_write_audio_cloud_writes_with_io_put():
    """Test cloud writes go through io_put with encoded audio bytes."""
    import numpy as np

    from daft.file.audio import write_audio

    audio_data = np.zeros(16, dtype=np.float64)

    with patch("daft.file.audio.io_put") as mock_io_put:
        meta = write_audio(audio_data, "s3://bucket/output.wav", sample_rate=16000, format=None, subtype=None)

    mock_io_put.assert_called_once()
    destination, payload = mock_io_put.call_args.args
    assert destination == "s3://bucket/output.wav"
    assert isinstance(payload, bytes)
    assert len(payload) > 0
    assert meta["format"] == "WAV"


def test_audiofile_write_method(sample_audio_path, tmp_path):
    """Test the AudioFile.write() method delegates correctly."""
    import soundfile as sf

    audio_file = daft.AudioFile(sample_audio_path)
    output_path = str(tmp_path / "method.wav")

    meta = audio_file.write(output_path)
    info = sf.info(output_path)

    assert info.format == "WAV"
    assert meta["sample_rate"] == 44100


def test_write_audio_format_override(sample_audio_path, tmp_path):
    """Test that explicit format parameter overrides extension inference."""
    import soundfile as sf

    output_path = str(tmp_path / "output.dat")

    df = daft.from_pydict({"path": [sample_audio_path], "output": [output_path]})
    df = df.with_column("audio", daft.functions.audio_file(df["path"]))
    df = df.with_column("result", daft.functions.write_audio(df["audio"], df["output"], format="WAV"))
    result = df.to_pydict()

    assert result["result"][0]["format"] == "WAV"

    # Verify the file on disk is actually a valid WAV file
    info = sf.info(output_path)
    assert info.format == "WAV"


def test_write_audio_returns_metadata(sample_audio_path, tmp_path):
    """Test that write_audio returns AudioMetadata struct with correct fields."""
    output_path = str(tmp_path / "output.wav")

    df = daft.from_pydict({"path": [sample_audio_path], "output": [output_path]})
    df = df.with_column("audio", daft.functions.audio_file(df["path"]))
    df = df.with_column("result", daft.functions.write_audio(df["audio"], df["output"]))
    result = df.to_pydict()

    meta = result["result"][0]
    assert "sample_rate" in meta
    assert "channels" in meta
    assert "frames" in meta
    assert "format" in meta
    assert "subtype" in meta
    assert isinstance(meta["sample_rate"], int)
    assert isinstance(meta["channels"], int)
    assert isinstance(meta["frames"], float)


def test_write_audio_expression_without_soundfile():
    """Test that creating a write_audio expression raises ImportError when soundfile is missing."""
    df = daft.from_pydict({"path": ["some/path.mp3"], "output": ["out.wav"]})

    with patch("daft.dependencies.sf.module_available", return_value=False):
        with pytest.raises(ImportError) as exc_info:
            daft.functions.write_audio(df["path"], df["output"], sample_rate=16000)

        error_message = str(exc_info.value)
        assert "soundfile" in error_message
        assert "daft[audio]" in error_message


def test_write_audio_tensor_requires_sample_rate(tmp_path):
    """Test that writing tensor data without sample_rate raises ValueError."""
    import numpy as np

    audio_data = np.zeros(1600, dtype=np.float64)
    output_path = str(tmp_path / "output.wav")

    df = daft.from_pydict({"audio": [audio_data], "output": [output_path]})
    df = df.with_column("result", daft.functions.write_audio(df["audio"], df["output"]))

    with pytest.raises(Exception, match="sample_rate"):
        df.collect()
