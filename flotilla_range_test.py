import daft
import daft.context

daft.context.set_runner_ray()
daft.set_execution_config(flotilla=True)

df = daft.range(1, 1000, partitions=100)
df = df.filter(daft.col("id") % 2 == 0)
df = df.write_parquet("test_out/range_test/", write_mode="overwrite")

df.show()
