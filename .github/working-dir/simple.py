import daft

daft.context.set_runner_ray("ray://localhost:10001")

df = daft.from_pydict({"nums": [1, 2, 3]})
df = df.with_column("result", daft.col("nums").cbrt()).collect()
df.show()
