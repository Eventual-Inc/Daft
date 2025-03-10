# PySpark.

The `daft.pyspark` module provides a way to create a PySpark session that can be run locally or backed by a ray cluster.

This serves as a way to run the Daft query engine with a Spark compatible API.

For the full PySpark SQL API documentation, see the [official PySpark documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html#spark-sql).

## Example

=== "🐍 Python"

```python
from daft.pyspark import SparkSession
from pyspark.sql.functions import col

# create a local spark session
spark = SparkSession.builder.local().getOrCreate()

# alternatively, connect to a ray cluster

spark = SparkSession.builder.remote("ray://<HEAD_IP>:6379").getOrCreate()
# you can use `ray get-head-ip <cluster_config.yaml>` to get the head ip!
# use spark as you would with the native spark library, but with a daft backend!

spark.createDataFrame([{"hello": "world"}]).select(col("hello")).show()

# stop the spark session
spark.stop()
```

## Notable Differences.

A few methods do have some notable differences compared to PySpark.

### explain

The `df.explain()` method will output non spark compatible `explain` and instead will be the same as calling `explain` on a Daft dataframe.

### show

Similarly, `df.show()` will output Daft's dataframe output instead of native spark's.
