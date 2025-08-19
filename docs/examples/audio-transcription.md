# Transcribe Audio with Daft

This example demonstrates how to build audio transcription pipelines using Daft with Whisper. We'll use:

- **Daft**
- **Whisper** - OpenAI's speech recognition model
- **SoundFile** - Audio file reading library
- **uv** - Python package installer and environment manager

We'll cover two scenarios:

1. Transcribing audio bytes stored in a columnar dataset (parquet)
2. Processing local audio files from a directory

## Let's set up our environment & install some dependencies

If you don't already have `uv` installed, you can run:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

or see their [installation page](https://docs.astral.sh/uv/getting-started/installation/) for more installation methods.

```bash
$ mkdir transcribe_demo
$ cd transcribe_demo
# Initialize uv project
$ uv init
# Install dependencies
$ uv add daft soundfile openai-whisper
```

You can also install using `pip`

```bash
$ pip install daft soundfile openai-whisper
```

## Example 1: Transcribing Audio Bytes from a Dataset

Let's create a new file called `example_1.py`

```py
# example_1.py
import daft
from daft.functions import file
import soundfile as sf
import whisper

model = whisper.load_model("tiny")  # Use "tiny" for quick demos, "base" or "small" for better quality

# this dataset contains all of the audio inside a binary column within a parquet file. such as
# | ------------------------  | ----   |
# | audio {bytes, path}       | text   |
# | ------------------------  | ----   |
# | {bytes: b"...", path: ""} | <text> |
# | {bytes: b"...", path: ""} | <text> |
# | {bytes: b"...", path: ""} | <text> |
# | {bytes: b"...", path: ""} | <text> |
#
df = daft.read_parquet("hf://datasets/MrDragonFox/Elise")

@daft.func
def transcribe(file: daft.File) -> str:
    """
    Transcribes an audio file using openai whisper.
    """
    audio, _ = sf.read(file, dtype = 'float32')
    result = model.transcribe(audio, verbose=True)


    return result['text']

# Extract bytes from struct column
df = df.select(df["audio"].struct.get("bytes").alias("bytes"))
# | ---------- |
# | bytes      |
# | ---------- |
# | b"..."     |
# | b"..."     |


# Convert bytes to file-like interface
df = df.select(file(df["bytes"]).alias("file"))
# | ---------- |
# | file       |
# | ---------- |
# | <file>     |
# | <file>     |


# Transcribe audio
df = df.select(transcribe(df["file"]))
# | ---------- |
# | text       |
# | ---------- |
# | <text>     |
# | <text>     |

# write to csv
df.write_csv("transcriptions")
```

You can also combine these operations into a single pipeline:

```python
df = daft.read_parquet("hf://datasets/MrDragonFox/Elise")
df.select(
    transcribe(file(df["audio"].struct.get("bytes")))
).write_csv("transcriptions.csv")
```

Now let's run it!

```bash
uv run example_1.py
```

## Example 2: Transcribing Local Audio Files

The same `daft.File` datatype works just as well with file paths. Let's transcribe a directory of audio files:

```bash
# Download sample audio files
wget https://www.openslr.org/resources/12/dev-clean.tar.gz
tar -xzf dev-clean.tar.gz
# This will create a LibriSpeech directory with Flac files
```

Now we can easily transcribe an entire directory of audio!

Let's create a new file called `example_2.py`

```python
# example_2.py
import daft
from daft.functions import file
import soundfile as sf
import whisper

# Load model
model = whisper.load_model("tiny")

@daft.func
def transcribe(file: daft.File) -> str:
    """Transcribes an audio file using OpenAI Whisper"""
    audio, _ = sf.read(file, dtype='float32')
    result = model.transcribe(audio)
    return result['text']

# Create dataframe from all flac files in directory
df = daft.from_glob_paths("./LibriSpeech/dev-clean/**/*.flac")

# Process all files
df = df.select(
    df["path"],
    transcribe(file(df["path"])).alias("transcription")
)

# Write results
df.write_csv("transcriptions.csv")
```

Now let's run it!

```bash
uv run example_1.py
```

We should see an output that looks like this:

```csv
"path","transcription"
"file://./LibriSpeech/dev-clean/8297/275154/8297-275154-0023.flac"," Let me hear what it is first."
"file://./LibriSpeech/dev-clean/8297/275154/8297-275154-0019.flac"," I tried it yesterday. It set my brains on fire. I'm feeling that glass I took just now."
"file://./LibriSpeech/dev-clean/8297/275154/8297-275154-0015.flac"," I'm alone. Do you hear that? Alone."
```

That's it! You've successfully transcribed audio files using Daft!
