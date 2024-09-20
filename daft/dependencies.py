from typing import TYPE_CHECKING

from daft.lazy_import import LazyImport

if TYPE_CHECKING:
    import xml.etree.ElementTree as ET

    import fsspec
    import numpy as np
    import pandas as pd
    import PIL.Image as pil_image
    import pyarrow as pa
    import pyarrow.csv as pacsv
    import pyarrow.dataset as pads
    import pyarrow.fs as pafs
    import pyarrow.json as pajson
    import pyarrow.parquet as pq
else:
    ET = LazyImport("xml.etree.ElementTree")

    fsspec = LazyImport("fsspec")
    np = LazyImport("numpy")
    pd = LazyImport("pandas")
    pil_image = LazyImport("PIL.Image")
    pa = LazyImport("pyarrow")
    pacsv = LazyImport("pyarrow.csv")
    pads = LazyImport("pyarrow.dataset")
    pafs = LazyImport("pyarrow.fs")
    pajson = LazyImport("pyarrow.json")
    pq = LazyImport("pyarrow.parquet")

unity_catalog = LazyImport("daft.unity_catalog")
