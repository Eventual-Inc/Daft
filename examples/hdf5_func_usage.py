import os 

import daft
from daft import col
from daft.functions import hdf5_file, hdf5_attrs, hdf5_keys, hdf5_metadata

import h5py
import numpy as np


def create_hdf5_sample_file(path: str):
    with h5py.File(path, "w") as f:
        f.attrs["source"] = "sample_data"
        grp = f.create_group("subgroup")
        
        dist = grp.create_dataset("distance_dataset", data=np.array([1.0, 2.0, 3.0]))
        dist.attrs["unit"] = "meters"

        ints = grp.create_dataset("int_dataset", data=np.array([1, 2, 3], dtype=np.int32))
        ints.attrs["unit"] = "count"


if __name__ == "__main__":
    BASE_URI = os.path.dirname(os.path.abspath(__file__))
    sample_path = os.path.join(BASE_URI, "sample.h5")

    # Create some sample data
    if not os.path.exists(sample_path):
        create_hdf5_sample_file(sample_path)
   
    # Discover the files
    df = daft.from_files(sample_path)

    # Process the data
    df = (
        df.with_column("h5_file", hdf5_file(col("file")))
        .with_column("attrs", hdf5_attrs(col("h5_file")))
        .with_column("keys", hdf5_keys(col("h5_file")))
        .with_column("metadata", hdf5_metadata(col("h5_file")))
        .select("h5_file", "attrs", "keys", col("metadata").explode().unnest())
    )
    df.show()
