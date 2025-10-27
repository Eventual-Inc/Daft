# Working with Audio

Audio isn't just a collection of bytes or waveforms—it's speech, music, ambient sound with meaning you can extract, transcribe, and analyze. Daft is built to handle audio data at scale, making it easy to process recordings, transcribe speech, and transform audio in distributed pipelines.

This guide shows you how to accomplish common audio processing tasks with Daft:

- [Read and write audio files](#reading-and-writing-audio-files)
- [Transcribe audio with Voice Activity Detection](#transcription-with-voice-activity-detection-plus-segment-and-word-timestamps)
- [Extract segment and word-level timestamps](#transcription-with-voice-activity-detection-plus-segment-and-word-timestamps)

## Reading and Writing Audio Files

Audio files come in various formats and sample rates. With `daft.File`, you can read audio data into numpy arrays, resample to a target sampling rate, and write back to disk—all in parallel across your dataset.

```python
# /// script
# description = "Read audio, resample, and save as mp3"
# requires-python = ">=3.10, <3.13"
# dependencies = ["daft", "soundfile", "numpy", "scipy"]
# ///

import pathlib
from typing import Any

import daft
from daft import DataType

import soundfile as sf
import numpy as np

def resample_audio(audio: np.ndarray, orig_sr: int, target_sr: int) -> np.ndarray:
    from scipy import signal
    gcd = np.gcd(orig_sr, target_sr)
    up = target_sr // gcd
    down = orig_sr // gcd
    resampled = signal.resample_poly(audio, up, down, padtype="edge")
    return resampled


ReadAudioReturnType = DataType.struct({
    "audio_array": DataType.tensor(DataType.float32()),
    "sample_rate": DataType.float32(),
    "offset": DataType.float32(),
    "duration": DataType.float32(),
})

@daft.func(return_dtype=ReadAudioReturnType)
def read_audio(
    file: daft.File,
    frames: int = -1,
    start: float | None = None,
    stop: float | None = None,
    dtype: str = "float32",
    sample_rate: int = 16000,
    fill_value: Any | None = None,
    always_2d: bool = True,
):
    """
    Read audio file into a numpy array
    """

    with file.open() as f:
        audio, native_sample_rate = sf.read(
            file=f,
            frames=frames,
            start=start,
            stop=stop,
            dtype=dtype,
            fill_value=fill_value,
            always_2d=always_2d,
        )

    if native_sample_rate != sample_rate:
        audio = resample_audio(audio, native_sample_rate, sample_rate)

    # Calculate offset and duration in seconds
    offset_seconds = start / sample_rate if start is not None else 0.0
    duration_seconds = len(audio) / sample_rate

    return {
        "audio_array": audio,
        "sample_rate": sample_rate,
        "offset": offset_seconds,
        "duration": duration_seconds,
    }


@daft.func()
def write_audio_to_mp3(
    audio: np.ndarray,
    destination: str,
    sample_rate: int = 16000,
    *,
    subtype: str = "MPEG_LAYER_III",
) -> str:
    """Persist audio samples to an MP3 file via ``soundfile``.

    ``soundfile`` expects floating point audio in ``[-1.0, 1.0]`` with shape
    ``(n_samples,)`` for mono or ``(n_samples, n_channels)`` for multi-channel
    audio. The helper reshapes typical channel-first buffers.
    """

    arr = np.asarray(audio)
    if arr.ndim == 1:
        data = arr
    elif arr.ndim == 2:
        if arr.shape[0] <= 8 and arr.shape[0] < arr.shape[1]:
            data = arr.T
        else:
            data = arr
    else:
        raise ValueError("Audio array must be 1-D or 2-D")

    data = data.astype(np.float32, copy=False)
    data = np.clip(data, -1.0, 1.0)

    # Ensure Directory Exists
    pathlib.Path(destination).parent.mkdir(parents=True, exist_ok=True)

    sf.write(destination, data, samplerate=sample_rate, format="MP3", subtype=subtype)
    return destination


@daft.func()
def sanitize_filename(filename: str) -> str:
    import re
    # Replace problematic characters including Unicode variants
    return re.sub(r"[/\\|｜:<>\"?*\s⧸]+", "_", filename)


# Copy + Paste this script then `uv run script.py`
if __name__ == "__main__":
    from daft import col, lit
    from daft.functions import file, format

    SOURCE_URI = "hf://datasets/Eventual-Inc/sample-files/audio/*.mp3"
    DEST_URI = ".data/audio/"
    TARGET_SR = 16000

    df = (
        daft.from_glob_path(SOURCE_URI)
        .with_column("audio_file", file(col("path")))
        .with_column("audio", read_audio(col("audio_file")))
        .with_column(
            "filename_sanitized",
            sanitize_filename(
                col("path").split("/").list.get(-1).split(".").list.get(0)
            ),
        )
        .with_column(
            "resampled_path",
            write_audio_to_mp3(
                audio=col("audio").struct.get("audio_array"),
                destination=format(
                    "{}{}{}", lit(DEST_URI), col("filename_sanitized"), lit(".mp3")
                ),
                sample_rate=TARGET_SR,
            ),
        )

    )

    df.show()

```

This example demonstrates several key patterns:

- **User-defined Functions (UDFs)**: Use `@daft.func` to create custom processing functions for audio data
- **File handling**: Work with `daft.File` objects to read and write audio efficiently
- **Parallel processing**: Daft automatically distributes audio processing across your cluster
- **Format conversion**: Read from one format and write to another with full control over audio parameters

!!! tip "Working with Different Audio Formats"

    The `soundfile` library supports many audio formats including WAV, FLAC, OGG, and MP3. You can easily adapt the code above to work with your preferred format by changing the `format` and `subtype` parameters in `sf.write()`.

## Transcription with Voice Activity Detection plus Segment and Word Timestamps

Transcription is one of the most powerful use cases for audio processing in AI pipelines. Whether you're building voice assistants, generating subtitles, or analyzing customer calls, accurate transcription with timestamps is essential.

### How to transcribe audio files

This example shows how to transcribe audio files using [faster-whisper](https://github.com/SYSTRAN/faster-whisper) with Voice Activity Detection (VAD) to filter out silence, and extract both segment-level and word-level timestamps.

```python
# /// script
# description = "Transcribe + VAD with Faster Whisper"
# requires-python = ">=3.10, <3.13"
# dependencies = ["daft>=0.6.7", "faster-whisper"]
# ///
from dataclasses import asdict
import daft
from daft.functions import file
from faster_whisper import WhisperModel, BatchedInferencePipeline
from transcription_schema import TranscriptionResult

# Define Parameters & Constants
SOURCE_URI = "hf://datasets/Eventual-Inc/sample-files/audio/*.mp3"
SAMPLE_RATE = 16000
DTYPE = "float32"
BATCH_SIZE = 16


@daft.cls()
class FasterWhisperTranscriber:
    def __init__(self, model="distil-large-v3", compute_type="float32", device="auto"):
        self.model = WhisperModel(model, compute_type=compute_type, device=device)
        self.pipe = BatchedInferencePipeline(self.model)

    @daft.method(return_dtype=TranscriptionResult)
    def transcribe(self, audio_file: daft.File):
        """Transcribe Audio Files with Voice Activity Detection (VAD) using Faster Whisper"""
        with audio_file.to_tempfile() as tmp:
            segments_iter, info = self.pipe.transcribe(
                str(tmp.name),
                vad_filter=True,
                vad_parameters=dict(min_silence_duration_ms=500),
                word_timestamps=True,
                batch_size=BATCH_SIZE,
            )
            segments = [asdict(seg) for seg in segments_iter]
            text = " ".join([seg["text"] for seg in segments])

            return {"transcript": text, "segments": segments, "info": asdict(info)}

# Instantiate Transcription UDF
fwt = FasterWhisperTranscriber()

# Transcribe the audio files
df_transcript = (
    # Discover the audio files
    daft.from_glob_path(SOURCE_URI)

    # Wrap the path as a daft.File
    .with_column("audio_file", file(col("path")))

    # Transcribe the audio file with Voice Activity Detection (VAD) using Faster Whisper
    .with_column("result", fwt.transcribe(daft.col("audio_file")))

    # Unpack Results
    .select("path", daft.col("result").unnest())
    .explode("segments")
    .select("path", "info", "transcript", unnest(daft.col("segments")))
)

df_transcript.show(3, format="fancy", max_width=40)
```

```{title="Output"}
╭────────────────────────────────────────┬─────────────────────────┬────────────────────────────────────────┬────┬───────┬────────┬────────┬────────────────────────────────────────┬────────────────────────────────────────┬───────────────────────┬────────────────────┬──────────────────────┬──────────────────┬─────────────╮
│ path                                   ┆ info                    ┆ transcript                             ┆ id ┆ seek  ┆ start  ┆ end    ┆ text                                   ┆ tokens                                 ┆ avg_logprob           ┆ compression_ratio  ┆ no_speech_prob       ┆ words            ┆ temperature │
╞════════════════════════════════════════╪═════════════════════════╪════════════════════════════════════════╪════╪═══════╪════════╪════════╪════════════════════════════════════════╪════════════════════════════════════════╪═══════════════════════╪════════════════════╪══════════════════════╪══════════════════╪═════════════╡
│ file:///Users/everettkleven/Desktop/N… ┆ {language: en,          ┆  Okay, so I have a cluster running wi… ┆ 1  ┆ 0     ┆ 0      ┆ 29.46  ┆  Okay, so I have a cluster running wi… ┆ [1033, 11, 370, 286, 362, 257, 13630,… ┆ -0.06497006558853646  ┆ 1.619672131147541  ┆ 0.01067733857780695  ┆ [{start: 0,      ┆ 0           │
│                                        ┆ language_probability: … ┆                                        ┆    ┆       ┆        ┆        ┆                                        ┆                                        ┆                       ┆                    ┆                      ┆ end: 0.48,       ┆             │
│                                        ┆                         ┆                                        ┆    ┆       ┆        ┆        ┆                                        ┆                                        ┆                       ┆                    ┆                      ┆ word:  Okay,,    ┆             │
│                                        ┆                         ┆                                        ┆    ┆       ┆        ┆        ┆                                        ┆                                        ┆                       ┆                    ┆                      ┆ …                ┆             │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ file:///Users/everettkleven/Desktop/N… ┆ {language: en,          ┆  Okay, so I have a cluster running wi… ┆ 2  ┆ 2940  ┆ 30.04  ┆ 56.41  ┆  Usually they want to do this to put … ┆ [11419, 436, 528, 281, 360, 341, 281,… ┆ -0.03574741696222471  ┆ 1.6338028169014085 ┆ 0.009307598695158958 ┆ [{start: 30.04,  ┆ 0           │
│                                        ┆ language_probability: … ┆                                        ┆    ┆       ┆        ┆        ┆                                        ┆                                        ┆                       ┆                    ┆                      ┆ end: 30.48,      ┆             │
│                                        ┆                         ┆                                        ┆    ┆       ┆        ┆        ┆                                        ┆                                        ┆                       ┆                    ┆                      ┆ word:  Us…       ┆             │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ file:///Users/everettkleven/Desktop/N… ┆ {language: en,          ┆  Okay, so I have a cluster running wi… ┆ 3  ┆ 5622  ┆ 56.83  ┆ 79.56  ┆  And go number two, I want this script ┆ [400, 352, 1230, 732, 11, 286, 528, 3… ┆ -0.07241094582492397  ┆ 1.5330396475770924 ┆ 0.005606058984994888 ┆ [{start: 56.83,  ┆ 0           │
│                                        ┆ language_probability: … ┆                                        ┆    ┆       ┆        ┆        ┆                                        ┆                                        ┆                       ┆                    ┆                      ┆ end: 57.27,      ┆             │
│                                        ┆                         ┆                                        ┆    ┆       ┆        ┆        ┆                                        ┆                                        ┆                       ┆                    ┆                      ┆ word:  An…       ┆             │
╰────────────────────────────────────────┴─────────────────────────┴────────────────────────────────────────┴────┴───────┴────────┴────────┴────────────────────────────────────────┴────────────────────────────────────────┴───────────────────────┴────────────────────┴──────────────────────┴──────────────────┴─────────────╯
(Showing first 3 rows)
```

The output shows rich transcription data including:

- **Full transcript**: Complete text transcription of the audio
- **Segments**: Individual segments with timing information
- **Word-level timestamps**: Precise start and end times for each word
- **Confidence scores**: Log probability and no-speech probability for quality assessment
- **Language detection**: Automatically detected language and confidence

!!! tip "Voice Activity Detection (VAD)"

    VAD filtering automatically removes silent portions of audio before transcription, which improves accuracy and reduces processing time. Adjust the `min_silence_duration_ms` parameter to control how much silence is required before a segment break.

### Understanding the transcription schema

The transcription result is a structured object with nested data. Here's the schema definition using Daft's type system. You can save this as `transcription_schema.py` and import it in your scripts:

```python
from daft import DataType

WordStruct = DataType.struct(
    {
        "start": DataType.float64(),
        "end": DataType.float64(),
        "word": DataType.string(),
        "probability": DataType.float64(),
    }
)

SegmentStruct = DataType.struct(
    {
        "id": DataType.int64(),
        "seek": DataType.int64(),
        "start": DataType.float64(),
        "end": DataType.float64(),
        "text": DataType.string(),
        "tokens": DataType.list(DataType.int64()),
        "avg_logprob": DataType.float64(),
        "compression_ratio": DataType.float64(),
        "no_speech_prob": DataType.float64(),
        "words": DataType.list(WordStruct),
        "temperature": DataType.float64(),
    }
)

TranscriptionOptionsStruct = DataType.struct(
    {
        "beam_size": DataType.int64(),
        "best_of": DataType.int64(),
        "patience": DataType.float64(),
        "length_penalty": DataType.float64(),
        "repetition_penalty": DataType.float64(),
        "no_repeat_ngram_size": DataType.int64(),
        "log_prob_threshold": DataType.float64(),
        "no_speech_threshold": DataType.float64(),
        "compression_ratio_threshold": DataType.float64(),
        "condition_on_previous_text": DataType.bool(),
        "prompt_reset_on_temperature": DataType.float64(),
        "temperatures": DataType.list(DataType.float64()),
        "initial_prompt": DataType.python(),
        "prefix": DataType.string(),
        "suppress_blank": DataType.bool(),
        "suppress_tokens": DataType.list(DataType.int64()),
        "without_timestamps": DataType.bool(),
        "max_initial_timestamp": DataType.float64(),
        "word_timestamps": DataType.bool(),
        "prepend_punctuations": DataType.string(),
        "append_punctuations": DataType.string(),
        "multilingual": DataType.bool(),
        "max_new_tokens": DataType.float64(),
        "clip_timestamps": DataType.python(),
        "hallucination_silence_threshold": DataType.float64(),
        "hotwords": DataType.string(),
    }
)

VadOptionsStruct = DataType.struct(
    {
        "threshold": DataType.float64(),
        "neg_threshold": DataType.float64(),
        "min_speech_duration_ms": DataType.int64(),
        "max_speech_duration_s": DataType.float64(),
        "min_silence_duration_ms": DataType.int64(),
        "speech_pad_ms": DataType.int64(),
    }
)

LanguageProbStruct = DataType.struct(
    {
        "language": DataType.string(),
        "probability": DataType.float64(),
    }
)

InfoStruct = DataType.struct(
    {
        "language": DataType.string(),
        "language_probability": DataType.float64(),
        "duration": DataType.float64(),
        "duration_after_vad": DataType.float64(),
        "all_language_probs": DataType.list(LanguageProbStruct),
        "transcription_options": TranscriptionOptionsStruct,
        "vad_options": VadOptionsStruct,
    }
)

TranscriptionResult = DataType.struct(
    {
        "transcript": DataType.string(),
        "segments": DataType.list(SegmentStruct),
        "info": InfoStruct,
    }
)
```

Using strongly-typed schemas ensures that your data pipeline is robust and catches errors early. Daft's support for complex nested structures makes it easy to work with rich transcription data without flattening everything into primitive types.

## Working with transcription results

Once you have transcriptions with timestamps, you can:

- **Generate subtitles**: Use the word-level timestamps to create precise subtitle files (SRT, VTT)
- **Search and filter**: Query transcripts to find specific keywords or topics
- **Analyze speech patterns**: Calculate speaking rates, pause durations, or word frequencies
- **Join with metadata**: Combine transcription data with speaker information, recording metadata, or other datasets

## More examples

For more advanced audio processing workflows, check out:

- [Custom User-Defined Functions (UDFs)](../custom-code/udfs.md) for building your own audio processing functions
- [Working with Files and URLs](urls.md) for discovering and downloading audio from remote storage
- [Distributed Processing](../distributed/index.md) for scaling audio processing across multiple machines
