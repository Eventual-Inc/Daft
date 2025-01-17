Managing Memory Usage
=====================

Managing memory usage and avoiding out-of-memory (OOM) issues while still maintaining efficient throughput is one of the biggest challenges when building resilient big data processing system!

This page is a walkthrough on how Daft handles such situations and possible remedies available to users when you encounter such situations.

Out-of-core Processing
----------------------

Daft supports `out-of-core data processing <https://en.wikipedia.org/wiki/External_memory_algorithm>`_ when running on the Ray runner by leveraging Ray's object spilling capabilities.

This means that when the total amount of data in Daft gets too large, Daft will spill data onto disk. This slows down the overall workload (because data now needs to be written to and read from disk) but frees up space in working memory for Daft to continue executing work without causing an OOM.

You will be alerted when spilling is occurring by log messages that look like this:

```
(raylet, ip=xx.xx.xx.xx) Spilled 16920 MiB, 9 objects, write throughput 576 MiB/s.
...
```

**Troubleshooting**

Spilling to disk is a mechanism that Daft uses to ensure workload completion in an environment where there is insufficient memory, but in some cases this can cause issues.

1. If your cluster is extremely aggressive with spilling (e.g. spilling hundreds of gigabytes of data) it can be possible that your machine may eventually run out of disk space and be killed by your cloud provider
2. Overly aggressive spilling can also cause your overall workload to be much slower

There are some things you can do that will help with this.

1. Use machines with more available memory per-CPU to increase each Ray worker's available memory (e.g. `AWS EC2 r5 instances <https://aws.amazon.com/ec2/instance-types/r5/>`_)
2. Use more machines in your cluster to increase overall cluster memory size
3. Use machines with attached local nvme SSD drives for higher throughput when spilling (e.g. AWS EC2 r5d instances)

For more troubleshooting, you may also wish to consult the `Ray documentation's recommendations for object spilling <https://docs.ray.io/en/latest/ray-core/objects/object-spilling.html>`_.

Dealing with out-of-memory (OOM) errors
---------------------------------------

While Daft is built to be extremely memory-efficient, there will inevitably be situations in which it has poorly estimated the amount of memory that it will require for a certain operation, or simply cannot do so (for example when running arbitrary user-defined Python functions).

Even with object spilling enabled, you may still sometimes see errors indicating OOMKill behavior on various levels such as your operating system, Ray or a higher-level cluster orchestrator such as Kubernetes:

1. On the local PyRunner, you may see that your operating system kills the process with an error message ``OOMKilled``.
2. On the RayRunner, you may notice Ray logs indicating that workers are aggressively being killed by the Raylet with log messages such as: ``Workers (tasks / actors) killed due to memory pressure (OOM)``
3. If you are running in an environment such as Kubernetes, you may notice that your pods are being killed or restarted with an ``OOMKill`` reason

These OOMKills are often recoverable (Daft-on-Ray will take care of retrying work after reviving the workers), however they may often significantly affect the runtime of your workload or if we simply cannot recover, fail the workload entirely.

**Troubleshooting**

There are some options available to you.

1. Use machines with more available memory per-CPU to increase each Ray worker's available memory (e.g. AWS EC2 r5 instances)
2. Use more machines in your cluster to increase overall cluster memory size
3. Aggressively filter your data so that Daft can avoid reading data that it does not have to (e.g. ``df.where(...)``)
4. Request more memory for your UDFs (see: :ref:`resource-requests`) if your UDFs are memory intensive (e.g. decompression of data, running large matrix computations etc)
5. Increase the number of partitions in your dataframe (hence making each partition smaller) using something like: ``df.into_partitions(df.num_partitions() * 2)``

If your workload continues to experience OOM issues, perhaps Daft could be better estimating the required memory to run certain steps in your workload. Please contact Daft developers on our forums!
