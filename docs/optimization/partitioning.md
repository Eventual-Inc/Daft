# Partitioning and Batching

Daft provides two complementary mechanisms for controlling parallelism: partitions and batches. Partitions let you split data into a fixed number of units, while batches split data based on by size.

> **TL;DR**
>
> - Use [df.repartition(...)][daft.DataFrame.repartition] if you are running on a distributed runner and want to split your data into a fixed number of partitions (finite units of work) across workers.
> - Use [df.into_batches(...)][daft.DataFrame.into_batches]  if you want to split your data so that each unit (batch) contains a similar number of rows, regardless of whether you are running distributed or on a single node.

## Partitioning: Finite Units of Work

Partitions are only available on distributed runners, i.e. [Kubernetes](../distributed/kubernetes.md) and [Ray](../distributed/ray.md). They let you split data into a fixed number of independent units that are distributed across workers.

When you repartition with [df.repartition(...)][daft.DataFrame.repartition], you decide how many partitions to split your data into and how to distribute rows across those pieces. This is useful when you know your cluster topology and want to control how work is divided.

Daft supports two partitioning strategies:

- **Hash partitioning**: Distributes rows based on specified columns, do this by passing in a column (or multiple) into repartition, i.e. `df.repartition(4, "col_a")`. Rows with the same values for those columns go to the same partition. By default, Daft will repartition before hash joins or groupbys, and the number of partitions will be determined automatically. You can override this by inserting your own repartition before the join or groupby.
- **Random partitioning**: Evenly distributes rows randomly across partitions, for balanced load across workers. This is the default behavior if no columns are passed in. Useful for when you only care about balanced loads.

**Default behavior:** Daft typically creates one partition per input file. Global operators like joins and aggregations may dynamically repartition data to satisfy their clustering requirements.

## Batching: Size-Based Splitting

Batching work on all runners (distributed and native). Unlike partitions, batches don't split data into a fixed count—instead, they split by size (number of rows). This is useful when you don't know your cluster or machine size ahead of time, but you know how many rows work well for a specific operation.

For example, if you're running expensive UDFs or decoding operations, you might know that processing 1,000 rows at a time keeps memory usage reasonable, whether you're on a 4-core laptop or a 100-node Ray cluster.

You can use the [df.into_batches(...)][daft.DataFrame.into_batches] to customize the batch size for subsequent operations.

**Default behavior:** Daft will automatically determine batch sizes based on the operation. Typically, inflationary operations like UDFs and downloads will have smaller batch sizes.

## Choosing the Right Strategy

- **When you know your cluster size** (distributed runners): Use partitions to control how work is split across workers. A good starting point is to match the number of partitions to the number of workers, so each worker gets an even share. Use hash partitioning before expensive joins or groupbys to customize the number of partitions for the operation.

- **When you know your operation's characteristics** (all runners): Use batches to control memory usage based on the nature of the operation, regardless of cluster size. For example, set batches to 1,000 rows before image decoding or exploding.

For additional memory-focused tips, see [Managing Memory Usage](memory.md).
