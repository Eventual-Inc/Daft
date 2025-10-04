from __future__ import annotations

import daft

df = daft.read_parquet("../test/dummy_data/ds_500000000_rows.parquet")
df = df.groupby("rand_dt").agg(daft.col("rand_str").count_distinct()).collect()
# df = df.distinct("rand_dt", "rand_str").groupby("rand_dt").agg(daft.col("rand_str").count()).collect()

print(df)
