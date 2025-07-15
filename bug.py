from __future__ import annotations

import shutil
import traceback
from pathlib import Path

import numpy as np

import daft
from daft import col

CLEANUP = True

D = 5

test_df = (
    daft.from_pydict({"e": [np.zeros(D, dtype=np.float32) for _ in range(10)]})
    .with_column("e", col("e").cast(daft.DataType.embedding(daft.DataType.float32(), D)))
    .collect()
)

print(test_df.schema())
# prints the following:
# ╭─────────────┬───────────────────────╮
# │ column_name ┆ type                  │
# ╞═════════════╪═══════════════════════╡
# │ e           ┆ Embedding[Float32; 5] │
# ╰─────────────┴───────────────────────╯

error: list[Exception] = []

o = "out.pq"
if Path(o).is_dir():
    shutil.rmtree(o)
parquet = test_df.write_parquet(o).collect()
print(f"PARQUET:\n{parquet}")

try:
    # this next line fails
    loaded_test_parquet_df = daft.read_parquet(o).collect()
except Exception as e:
    traceback.print_exc()
    error.append(e)

if CLEANUP:
    print("Removing parquet files")
    shutil.rmtree(o)

l = "./out.lance"  # noqa: E741
if Path(l).is_dir():
    shutil.rmtree(l)
lance = test_df.write_lance(l).collect()
print(f"LANCE:\n{lance}")

try:
    # this nex line also fails
    loaded_test_lance_df = daft.read_lance(l).collect()
except Exception as e:
    traceback.print_exc()
    error.append(e)

if CLEANUP:
    print("Removing lance files")
    shutil.rmtree(l)

# print("\n"*3)
# print("-"*80)
# print("\n")

print("-" * 80)

if len(error) > 0:
    print(f"Found {len(error)} errors!")
    # print(f"Found {len(error)} errors:\n{error}")
    # if len(error) == 2:
    #    raise error[1] from error[0]
    # else:
    #    raise error[0]
else:
    print("OK! No bugs!")
