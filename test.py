import daft

df = daft.read_parquet("hdfs://127.0.0.1:9000/example.parquet")
df.write_parquet("hdfs://127.0.0.1:9000/another.parquet")
