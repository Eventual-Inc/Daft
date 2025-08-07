from __future__ import annotations

import daft

#df = daft.read_parquet("/Users/sammy/Downloads/yellow_tripdata_2024-01.parquet")
df = daft.from_pydict({"x": [1,2,3]}).where('x=2')

df.collect()
