from __future__ import annotations

import daft


@daft.udf(return_dtype=daft.DataType.string(), label_selector={"label1": "group1"}, concurrency=2)
class Group1UDF:
    def __init__(self):
        pass

    def __call__(self, data):
        return [f"group1_processed_{item}" for item in data.to_pylist()]


@daft.udf(return_dtype=daft.DataType.string(), label_selector={"label2": "group2"}, concurrency=2)
class Group2UDF:
    def __init__(self):
        pass

    def __call__(self, data):
        return [f"group2_processed_{item}" for item in data.to_pylist()]


daft.context.set_runner_ray()
df = daft.from_pydict({"data": ["x", "y", "z"]})

df = df.with_column("p_group1", Group1UDF(df["data"]))
result_df = df.with_column("p_group2", Group2UDF(df["data"]))

result_df.show()
