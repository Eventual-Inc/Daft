# Transcribe Audio with Daft

Let's install some dependencies.

```bash
pip install daft soundfile openai-whisper
```

```py
import daft
from daft.functions import file
import soundfile as sf
import whisper


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
model = whisper.load_model("turbo")

@daft.func
def transcribe(file: daft.File) -> str:
    """
    Transcribes an audio file using openai whisper.
    """
    audio, _ = sf.read(file, dtype = 'float32')
    result = model.transcribe(audio, verbose=True)


    return result['text']

# unnest the data into a single column
df = df.select(df["audio"].struct.get("bytes").alias("bytes"))
# | ---------- |
# | bytes      |
# | ---------- |
# | b"..."     |
# | b"..."     |


# convert the bytes into a file like interface
df = df.select(file(df["bytes"]).alias("file"))
# | ---------- |
# | file       |
# | ---------- |
# | <file>     |
# | <file>     |


# transcribe the audio
df = df.select(transcribe(df["file"]))
# | ---------- |
# | text       |
# | ---------- |
# | <text>     |
# | <text>     |

# materialize the dataframe
df.collect()
```

Note, these operations are broken out to make it easier to understand. One could just as easily combine them all in to a single operation.

```py
df.select(transcribe(file(df["audio"].struct.get("bytes").alias("bytes")))).collect()
```

The `daft.File` datatype can just as easily take in a file path instead of a `bytes` object.

Imagine you want to transcribe all files in a local directory:

```py
df = daft.from_glob_paths("path/to/audio_files/**/*.wav")

df = df.select(transcribe(file(df["file"])))

df.collect()
```
