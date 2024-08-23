Partitioning
============

Daft is a **distributed** dataframe. This means internally, data is represented as partitions which are then spread out across your system:

<TODO: Diagram of how partitions are spread in a cluster>

Why do we need partitions?
--------------------------

When running in a distributed settings (a cluster of machines), Daft spreads your dataframe's data across these machines. This means that your
workload is able to efficiently utilize all the resources in your cluster because each machine is able to work on its assigned partition(s) independently.

Additionally, certain global operations in a distributed setting requires data to be partitioned in a specific way for the operation to be correct, because
all the data matching a certain criteria needs to be on the same machine and in the same partition. For example, in a groupby-aggregation Daft needs to bring
together all the data for a given key into the same partition before it can perform a definitive local groupby-aggregation which is then globally correct.
Daft refers to this as a "clustering specification", and you are able to see this in the plans that it constructs as well.

<TODO: Diagram of how groupby partitioning works>

.. NOTE::
    When running locally on just a single machine, Daft is currently still using partitioning as well. This is still useful for
    controlling parallelism and how much data is being materialized at a time.

    However, Daft's new experimental execution engine will remove the concept of partitioning entirely for local execution.
    You may enable it with ``DAFT_ENABLE_NATIVE_EXECUTOR=1``. Instead of using partitioning to control parallelism,
    this new execution engine performs a streaming-based execution on small "morsels" of data, which provides much
    more stable memory utilization while improving the user experience with not having to worry about partitioning.

This user guide helps you think about how to correctly partition your data to improve performance as well as memory stability in Daft.

General rule of thumb:

1. **Have Enough Partitions**: our general recommendation for high throughput and maximal resource utilization is to have *at least* ``2 x TOTAL_NUM_CPUS`` partitions, which allows Daft to fully saturate your CPUs.
2. **More Partitions**: if you are observing memory issues (excessive spilling or out-of-memory (OOM) issues) then you may choose to increase the number of partitions. This increases the amount of overhead in your system, but improves overall memory stability (since each partition will be smaller).
3. **Fewer Partitions**: if you are observing a large amount of overhead (especially during shuffle operations such as joins and sorts), then you may choose to decrease the number of partitions. This decreases the amount of overhead in the system, at the cost of using more memory (since each partition will be larger).

.. seealso::
    :doc:`./memory` - a guide for dealing with memory issues when using Daft

How is my data partitioned?
---------------------------

Daft will automatically use certain heuristics to determine the number of partitions for you when you create a DataFrame. When reading data from files (e.g. Parquet, CSV or JSON),
each file is by default one partition on its own, but Daft will also perform splitting of partitions (for files that are egregiously large) and coalescing of partitions (for small files)
to improve the sizing and number of partitions in your system.

To interrogate the partitioning of your current DataFrame, you may use the :meth:`df.explain(show_all=True) <daft.DataFrame.explain>` method. Here is an example output from a simple
`df = daft.read_parquet(...)` call on a fairly large number of Parquet files.

<TODO: show output of a read parquet which demonstrates coalescing and splitting>

How can I change the way my data is partitioned?
------------------------------------------------

You can change the way your data is partitioned by leveraging certain DataFrame methods:

1. :meth:`df.repartition(N, col("x"), col("y"), ...) <daft.DataFrame.repartition>`: repartitions your data into `N` partitions by performing a hash-bucketing that ensure that all data with the same values for the specified columns ends up in the same partition. Expensive, requires data movement between partitions and machines.
2. :meth:`df.into_partitions(N) <daft.DataFrame.into_partitions>`: splits or coalesces adjacent partitions to meet the specified target number of total partitions. This is less expensive than a call to ``df.repartition`` because it does not require shuffling or moving data between partitions.
3. Many global dataframe operations such as :meth:`daft.DataFrame.join`, :meth:`daft.DataFrame.sort` and :meth:`daft.Dataframe.agg` will change the partitioning of your data. This is because they require shuffling data between partitions to be globally correct.

Note that many of these methods will change both the *number of partitions* as well as the *clustering specification* of the new partitioning. For example, when calling ``df.repartition(8, col("x"))``, the resultant dataframe will now have 8 partitions in total with the additional guarantee that all rows with the same value of ``col("x")`` are in the same partition! This is called "hash partitioning".

<TODO: Add diagram>

Another example is after a sort - `df.sort(col("x"))` will globally sort your dataframe by `col("x")`, and after the sort your data has the same number of partitions but now has a new clustering scheme called a "range partitioning". Range partitioning gives an ordering guarantee to your partitions, where all rows with values of ``LOWER <= col("x") <= UPPER`` end up in the same partition.

<TODO: Add diagram>
