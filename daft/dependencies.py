from __future__ import annotations

from typing import TYPE_CHECKING

from daft.lazy_import import LazyImport

if TYPE_CHECKING:
    import accelerate
    import av
    import bitsandbytes
    import fsspec
    import librosa
    import numpy as np
    import pandas as pd
    import PIL.Image as pil_image
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.csv as pacsv
    import pyarrow.dataset as pads
    import pyarrow.flight as flight
    import pyarrow.fs as pafs
    import pyarrow.json as pajson
    import pyarrow.parquet as pq
    import requests
    import soundfile as sf
    import tensorflow as tf
    import torch
    import torchvision
    import transformers
else:
    av = LazyImport("av")
    flight = LazyImport("pyarrow.flight")
    fsspec = LazyImport("fsspec")
    librosa = LazyImport("librosa")
    np = LazyImport("numpy")
    pa = LazyImport("pyarrow")
    pacsv = LazyImport("pyarrow.csv")
    pads = LazyImport("pyarrow.dataset")
    pafs = LazyImport("pyarrow.fs")
    pajson = LazyImport("pyarrow.json")
    pc = LazyImport("pyarrow.compute")
    pd = LazyImport("pandas")
    pil_image = LazyImport("PIL.Image")
    pq = LazyImport("pyarrow.parquet")
    sf = LazyImport("soundfile")
    requests = LazyImport("requests")
    tf = LazyImport("tensorflow")
    torch = LazyImport("torch")
    torchvision = LazyImport("torchvision")
    transformers = LazyImport("transformers")
    accelerate = LazyImport("accelerate")
    bitsandbytes = LazyImport("bitsandbytes")

unity_catalog = LazyImport("daft.unity_catalog")

__all__ = [
    "accelerate",
    "av",
    "bitsandbytes",
    "flight",
    "fsspec",
    "librosa",
    "np",
    "pa",
    "pacsv",
    "pads",
    "pafs",
    "pajson",
    "pc",
    "pd",
    "pil_image",
    "pq",
    "requests",
    "sf",
    "tf",
    "torch",
    "torchvision",
    "transformers",
    "unity_catalog",
]
