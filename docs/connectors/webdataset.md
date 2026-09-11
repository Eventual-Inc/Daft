# WebDataset

[WebDataset](https://github.com/webdataset/webdataset) stores multimodal samples
as consecutive members of TAR shards. Daft reads these shards with
[`daft.read_webdataset()`][daft.read_webdataset].

## Reading shards

Pass a TAR file, directory, glob, or list of paths:

```python
import daft

df = daft.read_webdataset("/datasets/images/*.tar")
```

Each row represents one sample. Consecutive members with the same prefix are
grouped together, and the suffix becomes the column name. For example,
`000001.jpg`, `000001.json`, and `000001.txt` produce one row with `jpg`,
`json`, and `txt` columns. Daft also includes:

- `__key__`: the sample prefix
- `__url__`: the shard containing the sample

JSON, text, and class/index sidecars are decoded eagerly. Image, audio, video,
and other binary members are represented as lazy [`File`][daft.File]
references into their TAR shard. Selecting only metadata columns therefore
avoids reading media payloads:

```python
metadata = df.select("__key__", "json", "txt")
images = df.select("__key__", "jpg")
```

## Reading from Hugging Face

Use `format="webdataset"` with
[`daft.read_huggingface()`][daft.read_huggingface]:

```python
df = daft.read_huggingface(
    "laion/conceptual-captions-12m-webdataset",
    format="webdataset",
)
```

You can also read explicit Hugging Face paths:

```python
df = daft.read_webdataset(
    "hf://datasets/laion/conceptual-captions-12m-webdataset/data/*.tar"
)
```

## Format requirements

Daft currently requires:

- Uncompressed `.tar` shards, because lazy member reads use byte offsets.
- Consistent member suffixes and JSON shapes across shards.

Schema inference uses the first five samples in the first shard. If a later
sample introduces an incompatible suffix or JSON shape, Daft raises an error
instead of silently discarding data.
