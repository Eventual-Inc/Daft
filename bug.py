from __future__ import annotations

import numpy as np

import daft
from daft import col

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

o = "./out.pq"
test_df.write_parquet(o)

# this next line fails
loaded_test_df = daft.read_parquet(o).collect()
