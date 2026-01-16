from __future__ import annotations

from unittest.mock import patch

import pytest

import daft


@pytest.fixture(scope="module")
def sample_audio_path():
    return "tests/assets/sample_audio.mp3"


def test_audio_file_standalone(sample_audio_path):
    import numpy as np

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
    import numpy as np
    import librosa
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
