# Engine Comparison

If you are familiar with DataFrame APIs like Pandas or Spark Dataframes, you will be right at home with Daft! Daft exposes a familiar DataFrame API which can be used for:

* Interactive Data Science: Performing interactive and ad-hoc exploration of data in a Notebook environment
* Extract/Transform/Load (ETL): Defining data pipelines that clean and process data for consumption by other users
* Data Analytics: Analyzing data by producing summaries and reports

Daft focuses on Machine Learning/Deep Learning workloads that often involve complex media data (images, video, audio, text documents and more).

Below we discuss some other data engines and compare them to Daft.

<!-- .. csv-table::
 :file: ../_static/dataframe-comp-table.csv
 :widths: 30, 30, 50, 30, 50, 30, 30
 :header-rows: 1 -->

| Engine                                         | Query Optimizer | Multimodal | Distributed | Arrow Backed | Vectorized Execution Engine | Out-of-Core |
| -----------------------------------------------| :--------------:| :--------: | :---------: | :----------: | :-------------------------: | :---------: |
| Daft                                           | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| [Pandas](https://github.com/pandas-dev/pandas) | ❌ | Python object | ❌ | optional >= 2.0 | some (Numpy) | ❌ |
| [Polars](https://github.com/pola-rs/polars)    | ✅ | Python object | ❌ | ✅ | ✅ | ✅ |
| [Modin](https://github.com/modin-project/modin)| Eagar | Python object | ✅ | ❌ | some (Pandas) | ✅ |
| [PySpark](https://github.com/apache/spark)     | ✅ | ❌ | ✅ | Pandas UDF/IO | Pandas UDF | ✅ |
| [Dask](https://github.com/dask/dask)           | ❌ | Python object| ✅ | ❌ | some (Pandas) | ✅ |

## Pandas/Modin

The main drawback of using Pandas is scalability. Pandas is single-threaded and not built for distributed computing. While this is not as much of a problem for purely tabular datasets, when dealing with data such as images/video your data can get very large and expensive to compute very quickly.

Modin is a project that provides "distributed Pandas". If the use-case is tabular, has code that is already written in Pandas but just needs to be scaled up to larger data, Modin may be a good choice. Modin aims to be 100% Pandas API compatible which means that certain operations that are important for performance in the world of multimodal data such as requesting for certain amount of resources (e.g. GPUs) is not yet possible.

## Spark Dataframes

Spark Dataframes are the modern enterprise de-facto solution for many ETL (Extract-Load-Transform) jobs. Originally written for Scala, a Python wrapper library called PySpark exists for Python compatibility which allows for some flexibility in leveraging the Python ecosystem for data processing.

Spark excels at large scale tabular analytics, with support for running Python code using [Pandas UDFs](https://www.databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html), but suffer from a few key issues.

* **Serialization overhead:** Spark itself is run on the JVM, and the PySpark wrapper runs a Python subprocess to execute Python code. This means means that running Python code always involves copying data back and forth between the Python and the Spark process. This is somewhat alleviated with [Arrow as the intermediate serialization format](https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html) but generally without the correct configurations and expert tuning this can be very slow.
* **Development experience:** Spark is not written for Python as a first-class citizen, which means that development velocity is often very slow when users need to run Python code for machine learning, image processing and more.
* **Typing:** Python is dynamically typed, but programming in Spark requires jumping through various hoops to be compatible with Spark's strongly typed data model. For example to [pass a 2D Numpy array between Spark functions](https://ai.plainenglish.io/large-scale-deep-learning-with-spark-an-opinionated-guide-1f2a7a948424), users have to:

    1. Store the shape and flatten the array
    2. Tell Spark exactly what type the array is
    3. Unravel the flattened array again on the other end

* **Debugging:** Key features such as exposing print statements or breakpoints from user-defined functions to the user are missing, which make PySpark extremely difficult to develop on.
* **Lack of granular execution control:** with heavy processing of multimodal data, users often need more control around the execution and scheduling of their work. For example, users may need to ensure that Spark runs a single executor per GPU, but Spark's programming model makes this very difficult.
* **Compatibility with downstream Machine Learning tasks:** Spark itself is not well suited for performing distributed ML training which is increasingly becoming the domain of frameworks such as Ray and Horovod. Integrating with such a solution is difficult and requires expert tuning of intermediate storage and data engineering solutions.

## Dask Dataframes

Dask and Daft are both DataFrame frameworks built for distributed computing. Both libraries enable you to process large, tabular datasets in parallel, either locally or on remote instances on-prem or in the cloud.

If you are currently using Dask, you may want to consider migrating to Daft if you:

- Are working with **multimodal data types**, such as nested JSON, tensors, Images, URLs, etc.,
- Need faster computations through **query planning and optimization**,
- Are executing **machine learning workloads** at scale,
- Need deep support for **data catalogs, predicate pushdowns and metadata pruning** from Iceberg, Delta, and Hudi
- Want to benefit from **native Rust concurrency**

You may want to stick with using Dask if you:

- Want to only write **pandas-like syntax**,
- Need to parallelize **array-based workloads** or arbitrary **Python code that does not involve DataFrames** (with Dask Array, Dask Delayed and/or Dask Futures)

Read more detailed comparisons in the [Dask Migration Guide](../migration/dask_migration.md).

## Ray Datasets

Ray Datasets make it easy to feed data really efficiently into Ray's model training and inference ecosystem. Datasets also provide basic functionality for data preprocessing such as mapping a function over each data item, filtering data etc.

However, Ray Datasets are not a fully-fledged Dataframe abstraction (and [it is explicit in not being an ETL framework for data science](https://docs.ray.io/en/latest/data/data.html)) which means that it lacks key features in data querying, visualization and aggregations.

Instead, Ray Data serves as an ideal destination for processed data from Daft, easily sent with a simple [df.to_ray_dataset()][daft.DataFrame.to_ray_dataset] call. This makes it a convenient entry point into your model training and inference pipeline.
