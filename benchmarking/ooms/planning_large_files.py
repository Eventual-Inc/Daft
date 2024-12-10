# /// script
# dependencies = []
# ///

import time

import daft

df = daft.read_parquet("s3://daft-public-datasets/tpch_iceberg_sf1000.db/lineitem/data/**")

start = time.time()
df.explain(True)
explain_time = time.time() - start

start = time.time()
df.limit(1).explain(True)
explain_limit_time = time.time() - start

start = time.time()
df.show()
show_time = time.time() - start

print("Explain", explain_time)
print("Explain limit", explain_limit_time)
print("Show", show_time)
