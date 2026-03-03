# from __future__ import annotations

# import daft

# daft.set_runner_ray()
# daft.set_execution_config(shuffle_algorithm="pre_shuffle_merge")
# REPARTITION_COUNT = 512

# df = (
#     daft.read_parquet("s3://daft-public-data/benchmarking/lineitem-parquet/")
#     .repartition(REPARTITION_COUNT, "L_ORDERKEY")
#     .write_parquet("out.pq")
# )
from __future__ import annotations

import daft

daft.set_runner_ray()
# daft.set_execution_config(
#     shuffle_algorithm="flight_shuffle_pre_shuffle_merge",
#     flight_shuffle_dirs=["/opt/ray/spill"],
#     pre_shuffle_merge_threshold=6 * 1024 * 1024 * 1024,  # 6GB
# )
daft.set_execution_config(
    shuffle_algorithm="pre_shuffle_merge",
    flight_shuffle_dirs=["/opt/ray/spill"],
    pre_shuffle_merge_threshold=60 * 1024 * 1024 * 1024,  # 500GB
)
df = daft.read_parquet(
    "s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/1000_0/512/parquet/lineitem/ff08ac7b-a0c3-4f15-b2d4-172a706eac95-0.parquet"
)
df = df.repartition(2, "L_ORDERKEY")
# df.write_parquet("s3://srinu-dump/lineitem-test.parquet")
# df.explain(show_all=True)

df = df.collect()
b = df._result_cache.size_bytes()
# print(b)
# print(b / 1024)
# print(b / 1024 / 1024)
# print(b / 1024 / 1024 / 1024)
