from __future__ import annotations

import pytest

pytest.importorskip("soundfile")
pytest.importorskip("librosa")

import librosa
import numpy as np
import soundfile as sf

import daft


@pytest.fixture(scope="module")
def sample_audio_path():
    return "tests/assets/sample_audio.mp3"


def test_audio_file_standalone(sample_audio_path):
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
    def manual_resample():
        data, samplerate = sf.read(sample_audio_path)
        return librosa.resample(data, orig_sr=samplerate, target_sr=48000)

    df = daft.from_pydict({"path": [sample_audio_path]})
    df = df.select(daft.functions.audio_file(df["path"], verify=True).alias("audio"))
    df = df.select(daft.functions.resample(df["audio"], 48000))

    actual = df.to_pydict()["audio"][0]

    assert np.allclose(actual, manual_resample())
