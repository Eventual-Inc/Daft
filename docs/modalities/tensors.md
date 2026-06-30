## Tensors

Tensors are a multi-dimensional array of data. 

Daft natively supports converting tensors to and from Daft's native `daft.DataType.tensor(dtype)` from the following libraries. 
- NumPy
- PyTorch

## HDF5 Files

[HDF5](https://www.hdfgroup.org/solutions/hdf5/) is a high performance hierarchical file format for storing and organizing large amounts of data. It is widely used in scientific fields including physics, biology, and robotics. 

`daft.Hdf5File` mirrors the canonical `[h5py.File](https://docs.h5py.org/en/stable/high/file.html#the-file-object)` interface from the [h5py](https://docs.h5py.org/en/stable/) library. As a subclass of `daft.File`, the `daft.Hdf5File` class is a lazy file reference that can be used in a DataFrame expression/functions including `hdf5_attrs`, `hdf5_keys`, and `hdf5_metadata`. These expressions provide ample metadata for preprocessing and row-filtering. 

!!! note 
    The `read` and `visit` methods are not available as DataFrame expressions at this time. They must be called inside of a `daft.cls` or `daft.func` UDF. See the [Example Usage with a UDF](#example-usage-with-a-udf) section for more details.

## Functions Example Usage

```python
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

```

```
╭────────────────────────────────┬───────────────────────────┬──────────────┬───────────────────────────┬─────────┬─────────────┬─────────┬─────────────┬─────────────╮
│ h5_file                        ┆ attrs                     ┆ keys         ┆ h5path                    ┆ kind    ┆ shape       ┆ dtype   ┆ chunks      ┆ compression │
│ ---                            ┆ ---                       ┆ ---          ┆ ---                       ┆ ---     ┆ ---         ┆ ---     ┆ ---         ┆ ---         │
│ File[Hdf5]                     ┆ Python                    ┆ List[String] ┆ String                    ┆ String  ┆ List[Int64] ┆ String  ┆ List[Int64] ┆ String      │
╞════════════════════════════════╪═══════════════════════════╪══════════════╪═══════════════════════════╪═════════╪═════════════╪═════════╪═════════════╪═════════════╡
│ Hdf5(path: file:///Users/ever… ┆ {'source': 'sample_data'} ┆ [subgroup]   ┆ subgroup                  ┆ group   ┆ []          ┆         ┆ []          ┆             │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Hdf5(path: file:///Users/ever… ┆ {'source': 'sample_data'} ┆ [subgroup]   ┆ subgroup/distance_dataset ┆ dataset ┆ [3]         ┆ float64 ┆ []          ┆             │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Hdf5(path: file:///Users/ever… ┆ {'source': 'sample_data'} ┆ [subgroup]   ┆ subgroup/int_dataset      ┆ dataset ┆ [3]         ┆ int32   ┆ []          ┆             │
╰────────────────────────────────┴───────────────────────────┴──────────────┴───────────────────────────┴─────────┴─────────────┴─────────┴─────────────┴─────────────╯

(Showing first 3 of 3 rows)
```



## Example Usage with a UDF

While `attrs`, `keys`, and `metadata` functions simplify metadata scans, most workloads will leverage the `Hdf5File.read()` and `Hdf5File.visit()` inside of a `daft.cls` or `daft.func` UDF. 

It's generally recommended to leverage `daft.Hdf5File.to_tempfile()` if your data resides on remote object stores. Opening a `h5py.File` object directly incurs many seeks across the network and is not recommended for remote data.

To demonstrate the full materialization of the hdf5 datasets, we'll peak under the hood of the a UDF that powers the [DROID](../../datasets/droid.md) trajectory api.

```python
import daft 
from daft import col, DataType, Hdf5File
import h5py

# Build the UDF that will read the trajectory data and return a struct of the requested fields
@daft.func(
    return_dtype=DataType.struct({
        "action/gripper_position": DataType.tensor(DataType.float64()),
        "action/target_gripper_position": DataType.tensor(DataType.float64()),
        "observation/robot_state/gripper_position": DataType.tensor(DataType.float64()),
    }),
    use_process=False,
    unnest=True,
)
def read_droid_trajectory(file: Hdf5File):
    with file.to_tempfile() as tmp, h5py.File(tmp.name, "r") as h5:
        return {
            "action/gripper_position": h5["action/gripper_position"][()],
            "action/target_gripper_position": h5["action/target_gripper_position"][()],
            "observation/robot_state/gripper_position": h5["observation/robot_state/gripper_position"][()],
        }

if __name__ == "__main__":

    df = (
        daft.datasets.droid.raw()
        .where(col("success")) # filter out failed episodes
        .select(col("current_task"), read_droid_trajectory(col("trajectory")))
    )

    df.show(3) 

```

```
╭────────────────────────────────┬─────────────────────────┬────────────────────────────────┬──────────────────────────────────────────╮
│ current_task                   ┆ action/gripper_position ┆ action/target_gripper_position ┆ observation/robot_state/gripper_position │
│ ---                            ┆ ---                     ┆ ---                            ┆ ---                                      │
│ String                         ┆ Tensor[Float64]         ┆ Tensor[Float64]                ┆ Tensor[Float64]                          │
╞════════════════════════════════╪═════════════════════════╪════════════════════════════════╪══════════════════════════════════════════╡
│ Move object into or out of co… ┆ <Tensor shape=(477)>    ┆ <Tensor shape=(477)>           ┆ <Tensor shape=(477)>                     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Move object to a new position… ┆ <Tensor shape=(1097)>   ┆ <Tensor shape=(1097)>          ┆ <Tensor shape=(1097)>                    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Move object to a new position… ┆ <Tensor shape=(1186)>   ┆ <Tensor shape=(1186)>          ┆ <Tensor shape=(1186)>                    │
╰────────────────────────────────┴─────────────────────────┴────────────────────────────────┴──────────────────────────────────────────╯
```



## See also

- [DROID datasets](../datasets/droid.md)
- [daft.File](./files.md)
- [daft.VideoFile](./videos.md)

