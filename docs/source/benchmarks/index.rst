Benchmarks
======================

TPCH Benchmark
--------------
Here we compare Daft against some popular Distributed Dataframes such as Spark, Modin, and Dask on the TPCH benchmark.

Our goals for this benchmark is to demonstrate that Daft is able to meet the following development goals:

#. **Solid out of the box performance:** great performance without having to tune esoteric flags or configurations specific to this workload
#. **Reliable out-of-core execution:** highly performant and reliable processing on larger-than-memory datasets, without developer intervention and Out-Of-Memory (OOM) errors
#. **Ease of use:** getting up and running should be easy on typical cloud infrastructure that is easy to spin up for an individual developer or in an enterprise cloud setting


A great stress-test of Daft is the `TPC-H benchmark <https://www.tpc.org/tpch/>`_, which is a standard benchmark for analytical query engines.
This benchmark helps ensure that while Daft makes it very easy to work with multimodal data, it can also do a great job at larger scales (Terabytes) of more traditional tabular analytical workloads.

Setup
-----
The basic setup for our benchmarks are as follows:

#. We run questions 1 to 10 of the TPC-H benchmarks using Daft and other commonly used Python distributed query engines.
#. The data for the queries are stored and retrieved from AWS S3 as partitioned Apache Parquet files which is typical of enterprise workloads. No on disk/in-memory caching was performed.
#. We run each framework on a cluster of AWS i3.2xlarge instances that each have:

   * 8 vCPUs
   * 61G of memory
   * 1900G of NVMe SSD space


The frameworks that we benchmark against are Spark, Modin and Dask. We chose these comparable frameworks as they are the most commonly referenced frameworks for running large scale distributed analytical queries in Python.

For benchmarking against Spark, we used AWS EMR, which is a hosted Spark service.

For other benchmarks, we were able to host our own Ray and Dask clusters on Kubernetes.

Please refer to the section on our Detailed Benchmarking Setup for additional information. [add reference]

Results
-------


Highlights
^^^^^^^^^^
#. Out of all the benchmarked frameworks, only Daft and EMR Spark were able to run Terabyte scale queries reliably on out-of-the-box configurations.
#. Daft is consistently much faster (3.3x faster than EMR Spark and 7.7x faster than Dask Dataframes).


.. note::
   We were unable to obtain results for Modin due to cluster OOMs, errors and timeouts (after one hour).
   Similarly, Dask was unable to provide comparable results for the Terabyte scale benchmark.
   It is possible that these frameworks may perform and function better with additional tuning and configuration.
   Logs for all the runs are provided in a public AWS S3 bucket.

100 Scale Factor
^^^^^^^^^^^^^^^^


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
| Modin     | 4/10                | Did not finish       | Did not finish   |
+-----------+---------------------+----------------------+------------------+

1000 Scale Factor
^^^^^^^^^^^^^^^^^

We are only able to compare Daft against EMR Spark at the TPCH 1000 Scale Factor (Terabyte Scale), as the other frameworks that we attempted to use were unable to provide sufficient comparable results due to OOMs and errors.

.. raw:: html
   :file: ../_static/tpch-1000sf.html

+-----------+---------------------+----------------------+------------------+
| Dataframe | Questions Completed | Total Time (seconds) | Relative to Daft |
+===========+=====================+======================+==================+
| Daft      | 10/10               | 7774                 | 1.0x             |
+-----------+---------------------+----------------------+------------------+
| Spark     | 10/10               | 27161                | 3.5x             |
+-----------+---------------------+----------------------+------------------+
| Dask      | 3/10                | Did not finish       | Did not finish   |
+-----------+---------------------+----------------------+------------------+
| Modin     | 0/10                | Did not finish       | Did not finish   |
+-----------+---------------------+----------------------+------------------+



.. raw:: html
   :file: ../_static/tpch-nodes-count-daft-1000sf.html



Detailed Benchmarking Setup
---------------------------

Benchmarking Code
^^^^^^^^^^^^^^^^^
