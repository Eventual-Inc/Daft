from __future__ import annotations

from typing import TYPE_CHECKING

from daft.lazy_import import LazyImport

if TYPE_CHECKING:
    import av
    import confluent_kafka
    import fsspec
    import h5py
    import librosa
    import mcap
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
else:
    av = LazyImport("av")
    confluent_kafka = LazyImport("confluent_kafka")
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
    h5py = LazyImport("h5py")
    mcap = LazyImport("mcap")
    requests = LazyImport("requests")
    tf = LazyImport("tensorflow")
    torch = LazyImport("torch")
    torchvision = LazyImport("torchvision")

unity_catalog = LazyImport("daft.catalog.__unity._client")

__all__ = [
    "av",
    "confluent_kafka",
    "flight",
    "fsspec",
    "h5py",
    "librosa",
    "mcap",
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
    "unity_catalog",
]
