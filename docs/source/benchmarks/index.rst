Benchmarks
##########

TPC-H Benchmark
***************
Here we compare Daft against some popular Distributed Dataframes such as Spark, Modin, and Dask on the TPC-H benchmark.

Our goals for this benchmark is to demonstrate that Daft is able to meet the following development goals:

#. **Solid out of the box performance:** great performance without having to tune esoteric flags or configurations specific to this workload
#. **Reliable out-of-core execution:** highly performant and reliable processing on larger-than-memory datasets, without developer intervention and Out-Of-Memory (OOM) errors
#. **Ease of use:** getting up and running should be easy on cloud infrastructure for an individual developer or in an enterprise cloud setting


A great stress test for Daft is the `TPC-H benchmark <https://www.tpc.org/tpch/>`_, which is a standard benchmark for analytical query engines.
This benchmark helps ensure that while Daft makes it very easy to work with multimodal data, it can also do a great job at larger scales (terabytes) of more traditional tabular analytical workloads.

Setup
-----
The basic setup for our benchmarks are as follows:

#. We run questions 1 to 10 of the TPC-H benchmarks using Daft and other commonly used Python Distributed Dataframes.
#. The data for the queries are stored and retrieved from AWS S3 as partitioned Apache Parquet files, which is typical of enterprise workloads. No on disk/in-memory caching was performed.
#. We run each framework on a cluster of AWS i3.2xlarge instances that each have:

   * 8 vCPUs
   * 61G of memory
   * 1900G of NVMe SSD space


The frameworks that we benchmark against are Spark, Modin, and Dask. We chose these comparable Dataframes as they are the most commonly referenced frameworks for running large scale distributed analytical queries in Python.

For benchmarking against Spark, we use AWS EMR which is a hosted Spark service.

For other benchmarks, we host our own Ray and Dask clusters on Kubernetes.

Please refer to the section on our :ref:`Detailed Benchmarking Setup <benchmarking-setup>` for additional information.

Results
-------


Highlights
^^^^^^^^^^
   #. Out of all the benchmarked frameworks, **only Daft and EMR Spark are able to run terabyte scale queries reliably** on out-of-the-box configurations.
   #. **Daft is consistently much faster** (3.3x faster than EMR Spark, 7.7x faster than Dask Dataframes, and 44.4x faster than Modin).


.. note::
   We were unable to obtain full results for Modin due to cluster OOMs, errors and timeouts (one hour limit per question attempt).
   Similarly, Dask was unable to provide comparable results for the terabyte scale benchmark.
   It is possible that these frameworks may perform and function better with additional tuning and configuration.
   Logs for all the runs are provided in a public AWS S3 bucket.

100 Scale Factor
^^^^^^^^^^^^^^^^

First we run TPC-H 100 Scale Factor (around 100GB) benchmark  on 4 i3.2xlarge worker instances.
In total, these instances add up to 244GB of cluster memory which will require the Dataframe library to perform disk spilling and out-of-core processing for certain questions that have a large join or sort.

.. raw:: html
   :file: ../_static/tpch-100sf.html

+-----------+---------------------+----------------------+------------------+
| Dataframe | Questions Completed | Total Time (seconds) | Relative to Daft |
+===========+=====================+======================+==================+
| Daft      | 10/10               | 785                  | 1.0x             |
+-----------+---------------------+----------------------+------------------+
| Spark     | 10/10               | 2648                 | 3.3x             |
+-----------+---------------------+----------------------+------------------+
| Dask      | 10/10               | 6010                 | 7.7x             |
+-----------+---------------------+----------------------+------------------+
| Modin     | 5/10                | Did not finish       | 44.4x*           |
+-----------+---------------------+----------------------+------------------+

*\* Only for queries that completed.*

From the results we see that Daft, Spark, and Dask are able to complete all the questions and Modin completes less than half.
We also see that Daft is **3.3x** faster than Spark and **7.7x** faster than Dask including S3 IO.
We expect these speed-ups to be much larger if the data is loaded in memory instead of cloud storage, which we will show in future benchmarks.

1000 Scale Factor
^^^^^^^^^^^^^^^^^
.. _1000_scale_factor:

Next we scale up the data size by 10x while keeping the cluster size the same. Since we only have 244GB of memory and 1TB+ of tabular data,
the DataFrame library will be required to perform disk spilling and out-of-core processing for all questions at nearly all stages of the query.

.. raw:: html
   :file: ../_static/tpch-1000sf.html

+-----------+---------------------+----------------------+------------------+
| Dataframe | Questions Completed | Total Time (seconds) | Relative to Daft |
+===========+=====================+======================+==================+
| Daft      | 10/10               | 7774                 | 1.0x             |
+-----------+---------------------+----------------------+------------------+
| Spark     | 10/10               | 27161                | 3.5x             |
+-----------+---------------------+----------------------+------------------+
| Dask      | 3/10                | Did not finish       | 5.8x*            |
+-----------+---------------------+----------------------+------------------+
| Modin     | 0/10                | Did not finish       | No data          |
+-----------+---------------------+----------------------+------------------+

*\* Only for queries that completed.*

From the results we see that only Daft and Spark are able to complete all the questions.
Dask completes less than a third and Modin is unable to complete any due to OOMs and cluster crashes.
Since we can only compare to Spark here, we see that Daft is **3.5x** faster including S3 IO.
This shows that Daft and Spark are the only Dataframes in this comparison capable of processing data larger than memory, with Daft standing out as the significantly faster option.

1000 Scale Factor - Node Count Ablation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Finally, we compare how Daft performs on varying size clusters on the terabyte scale dataset.
We run the same Daft TPC-H questions on the same dataset as the :ref:`previous section<1000_scale_factor>` but sweep the worker node count.

.. raw:: html
   :file: ../_static/tpch-nodes-count-daft-1000-sf.html


We note two interesting results here:

#. Daft can process 1TB+ of analytical data on a single 61GB instance without being distributed (16x more data than memory).
#. Daft query times scale linearly with the number of nodes (e.g. 4 nodes being 4 times faster than a single node). This allows for faster queries while maintaining the same compute cost!

Detailed Benchmarking Setup
---------------------------
.. _benchmarking-setup:

Benchmarking Code
^^^^^^^^^^^^^^^^^

Our benchmarking scripts and code can be found in the `distributed-query-benchmarks <https://github.com/Eventual-Inc/distributed-query-benchmarking>`_ GitHub repository.

* TPC-H queries for Daft were written by us.
* TPC-H queries for SparkSQL was adapted from `this repository <https://github.com/Bodo-inc/Bodo-examples/blob/master/06-Compare-Bodo-with-Spark/tpch/pyspark_notebook.ipynb>`_.
* TPC-H queries for Dask and Modin were adapted from these repositories for questions `Q1-7 <https://github.com/pola-rs/tpch>`_ and `Q8-10 <https://github.com/xprobe-inc/benchmarks/tree/main/tpch>`_.

Infrastructure
^^^^^^^^^^^^^^
Our infrastructure runs on an EKS Kubernetes cluster.

=========================== ===================================================================================
**Driver Instance**         i3.2xlarge
**Worker Instance**         i3.2xlarge
**Number of Workers**       1/4/8
**Networking**              All instances colocated in the same Availability Zone in the AWS us-west-2 region
=========================== ===================================================================================


Data
^^^^
Data for the benchmark was stored in AWS S3.
No node-level caching was performed, and data is read directly from AWS S3 on every attempt to simulate realistic workloads.


===================== =================================================================================================================================================================================================================================================================================================================================
**Storage**           AWS S3 Bucket
**Format**            Parquet
**Region**            us-west-2
**File Layout**       Each table is split into 32 (for the 100SF benchmark) or 512 (for the 1000SF benchmark) separate Parquet files. Parquet files for a given table have their paths prefixed with that table’s name, and are laid out in a flat folder structure under that prefix. Frameworks are instructed to read Parquet files from that prefix.
**Data Generation**   TPC-H data was generated using the utilities found in the open-sourced `Daft repository. <https://github.com/Eventual-Inc/Daft/blob/main/benchmarking/tpch/pipelined_data_generation.py>`_ This data is also available on request if you wish to reproduce any results!
===================== =================================================================================================================================================================================================================================================================================================================================

Cluster Setup
^^^^^^^^^^^^^

Dask and Ray
============

To help us run the Distributed Dataframe libraries, we used Kubernetes for deploying Dask and Ray clusters.
The configuration files for these setups can be found in our `open source benchmarking repository. <https://github.com/Eventual-Inc/distributed-query-benchmarking/tree/main/cluster_setup>`_

Our benchmarks for Daft and Modin were run on a `KubeRay <https://github.com/ray-project/kuberay>`_ cluster, and our benchmarks for Dask was run on a `Dask-on-Kubernetes <https://github.com/dask/dask-kubernetes>`_ cluster.
Both projects are owned and maintained officially by the creators of these libraries as one of the main methods of deploying.

Spark
=====
For benchmarking Spark we used AWS EMR, the official managed Spark solution provided by AWS.
For more details on our setup and approach, please consult our Spark benchmarks `README <https://github.com/Eventual-Inc/distributed-query-benchmarking/tree/main/distributed_query_benchmarking/spark_queries>`_.

Logs
^^^^

================================== ============== ======= =====================================================================================================================================================================================================================================================================================================================
Dataframe                          Scale Factor   Nodes   Links
================================== ============== ======= =====================================================================================================================================================================================================================================================================================================================
Daft                               1000           8       #. s3://daft-public-data/benchmarking/logs/daft.0_1_3.1tb.8-i32xlarge.log
Daft                               1000           4       #. s3://daft-public-data/benchmarking/logs/daft.0_1_3.1tb.4-i32xlarge.log
Daft                               1000           1       #. s3://daft-public-data/benchmarking/logs/daft.1tb.1.i3-2xlarge.part1.log
                                                          #. s3://daft-public-data/benchmarking/logs/daft.1tb.1.i3-2xlarge.part2.log
Daft                               100            4       #. s3://daft-public-data/benchmarking/logs/daft.0_1_3.100gb.4-i32xlarge.log
Spark                              1000           4       #. s3://daft-public-data/benchmarking/logs/emr-spark.6_10_0.1tb.4-i32xlarge.log
Spark                              100            4       #. s3://daft-public-data/benchmarking/logs/emr-spark.6_10_0.100gb.4-i32xlarge.log.gz
Dask (failed, multiple retries)    1000           16      #. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.1tb.16-i32xlarge.0.log
                                                          #. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.1tb.16-i32xlarge.1.log
                                                          #. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.1tb.16-i32xlarge.2.log
                                                          #. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.1tb.16-i32xlarge.3.log
Dask (failed, multiple retries)    1000           4       #. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.1tb.4-i32xlarge.q126.log
Dask (multiple retries)            100            4       #. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.100gb.4-i32xlarge.0.log
                                                          #. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.100gb.4-i32xlarge.0.log
                                                          #. s3://daft-public-data/benchmarking/logs/dask.2023_5_0.100gb.4-i32xlarge.1.log
Modin (failed, multiple retries)   1000           16      #. s3://daft-public-data/benchmarking/logs/modin.0_20_1.1tb.16-i32xlarge.0.log
                                                          #. s3://daft-public-data/benchmarking/logs/modin.0_20_1.1tb.16-i32xlarge.1.log
Modin (failed, multiple retries)   100            4       #. s3://daft-public-data/benchmarking/logs/modin.0_20_1.100gb.4-i32xlarge.log
================================== ============== ======= =====================================================================================================================================================================================================================================================================================================================
