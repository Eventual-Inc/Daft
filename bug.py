from __future__ import annotations

import shutil
import traceback
from pathlib import Path

import numpy as np

import daft
from daft import col

CLEANUP = True

D = 5

rng = np.random.default_rng()

test_df = (
    daft.from_pydict({"e": [rng.random(size=(D,), dtype=np.float32) for _ in range(10)]})
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
    print("OK! No bugs yet!")


test_rows = list(x["e"] for x in test_df.iter_rows())
for t in test_rows:
    assert isinstance(t, np.ndarray)
    assert t.dtype == np.float32


def check(loaded):
    l_rows = list(x["e"] for x in loaded.iter_rows())
    for i, (t, l) in enumerate(zip(test_rows, l_rows)):
        assert isinstance(l, np.ndarray), f"Expected a numpy array when loading, got a {type(l)}: {l}"
        assert (t == l).all(), f"Failed on row {i}: test_df={t} vs. loaded={l}"
        assert l.dtype == t.dtype


check(loaded_test_parquet_df)
print("Parquet loaded data matches 1-1")

check(loaded_test_lance_df)
print("Lance loaded data matches 1-1")

print("OK Loaded data is the same!")

import ipdb

ipdb.set_trace()
