|Banner|

|CI| |PyPI| |Latest Tag| |Coverage| |Slack|

`Website <https://www.getdaft.io>`_ • `Docs <https://docs.getdaft.io>`_ • `Installation <https://docs.getdaft.io/en/stable/install/>`_ • `Daft Quickstart <https://docs.getdaft.io/en/stable/quickstart/>`_ • `Community and Support <https://github.com/Eventual-Inc/Daft/discussions>`_

Daft: Unified Engine for Data Analytics, Engineering & ML/AI
============================================================


`Daft <https://www.getdaft.io>`_ is a distributed query engine for large-scale data processing using Python or SQL, implemented in Rust.

* **Familiar interactive API:** Lazy Python Dataframe for rapid and interactive iteration, or SQL for analytical queries
* **Focus on the what:** Powerful Query Optimizer that rewrites queries to be as efficient as possible
* **Data Catalog integrations:** Full integration with data catalogs such as Apache Iceberg
* **Rich multimodal type-system:** Supports multimodal types such as Images, URLs, Tensors and more
* **Seamless Interchange**: Built on the `Apache Arrow <https://arrow.apache.org/docs/index.html>`_ In-Memory Format
* **Built for the cloud:** `Record-setting <https://blog.getdaft.io/p/announcing-daft-02-10x-faster-io>`_ I/O performance for integrations with S3 cloud storage

**Table of Contents**

* `About Daft`_
* `Getting Started`_
* `Benchmarks`_
* `Contributing`_
* `Telemetry`_
* `Related Projects`_
* `License`_

About Daft
----------

Daft was designed with the following principles in mind:

1. **Any Data**: Beyond the usual strings/numbers/dates, Daft columns can also hold complex or nested multimodal data such as Images, Embeddings and Python objects efficiently with it's Arrow based memory representation. Ingestion and basic transformations of multimodal data is extremely easy and performant in Daft.
2. **Interactive Computing**: Daft is built for the interactive developer experience through notebooks or REPLs - intelligent caching/query optimizations accelerates your experimentation and data exploration.
3. **Distributed Computing**: Some workloads can quickly outgrow your local laptop's computational resources - Daft integrates natively with `Ray <https://www.ray.io>`_ for running dataframes on large clusters of machines with thousands of CPUs/GPUs.

Getting Started
---------------

Installation
^^^^^^^^^^^^

Install Daft with ``pip install daft``.

For more advanced installations (e.g. installing from source or with extra dependencies such as Ray and AWS utilities), please see our `Installation Guide <https://docs.getdaft.io/en/stable/install/>`_

Quickstart
^^^^^^^^^^

  Check out our `quickstart <https://docs.getdaft.io/en/stable/quickstart/>`_!

In this example, we load images from an AWS S3 bucket's URLs and resize each image in the dataframe:

.. code:: python

    import daft

    # Load a dataframe from filepaths in an S3 bucket
    df = daft.from_glob_path("s3://daft-public-data/laion-sample-images/*")

    # 1. Download column of image URLs as a column of bytes
    # 2. Decode the column of bytes into a column of images
    df = df.with_column("image", df["path"].url.download().image.decode())

    # Resize each image into 32x32
    df = df.with_column("resized", df["image"].image.resize(32, 32))

    df.show(3)


|Quickstart Image|


Benchmarks
----------
|Benchmark Image|

To see the full benchmarks, detailed setup, and logs, check out our `benchmarking page. <https://docs.getdaft.io/en/stable/resources/benchmarks/tpch/>`_


More Resources
^^^^^^^^^^^^^^

* `Daft Quickstart <https://docs.getdaft.io/en/stable/quickstart/>`_ - learn more about Daft's full range of capabilities including dataloading from URLs, joins, user-defined functions (UDF), groupby, aggregations and more.
* `User Guide <https://docs.getdaft.io/en/stable/>`_ - take a deep-dive into each topic within Daft
* `API Reference <https://docs.getdaft.io/en/stable/api/>`_ - API reference for public classes/functions of Daft
* `SQL Reference <https://docs.getdaft.io/en/stable/sql/>`_ - Daft SQL reference

Contributing
------------

We <3 developers! To start contributing to Daft, please read `CONTRIBUTING.md <https://github.com/Eventual-Inc/Daft/blob/main/CONTRIBUTING.md>`_ This document describes the development lifecycle and toolchain for working on Daft. It also details how to add new functionality to the core engine and expose it through a Python API.

Here's a list of `good first issues <https://github.com/Eventual-Inc/Daft/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22>`_ to get yourself warmed up with Daft. Comment in the issue to pick it up, and feel free to ask any questions!

Telemetry
---------

To help improve Daft, we collect non-identifiable data via Scarf (https://scarf.sh).

To disable this behavior, set the environment variable ``DO_NOT_TRACK=true``.

The data that we collect is:

1. **Non-identifiable:** Events are keyed by a session ID which is generated on import of Daft
2. **Metadata-only:** We do not collect any of our users’ proprietary code or data
3. **For development only:** We do not buy or sell any user data

Please see our `documentation <https://docs.getdaft.io/en/stable/resources/telemetry/>`_ for more details.

.. image:: https://static.scarf.sh/a.png?x-pxid=31f8d5ba-7e09-4d75-8895-5252bbf06cf6

Related Projects
----------------

+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+
| Engine                                            | Query Optimizer | Multimodal    | Distributed | Arrow Backed    | Vectorized Execution Engine | Out-of-core |
+===================================================+=================+===============+=============+=================+=============================+=============+
| Daft                                              | Yes             | Yes           | Yes         | Yes             | Yes                         | Yes         |
+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+
| `Pandas <https://github.com/pandas-dev/pandas>`_  | No              | Python object | No          | optional >= 2.0 | Some(Numpy)                 | No          |
+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+
| `Polars <https://github.com/pola-rs/polars>`_     | Yes             | Python object | No          | Yes             | Yes                         | Yes         |
+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+
| `Modin <https://github.com/modin-project/modin>`_ | Eagar           | Python object | Yes         | No              | Some(Pandas)                | Yes         |
+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+
| `Pyspark <https://github.com/apache/spark>`_      | Yes             | No            | Yes         | Pandas UDF/IO   | Pandas UDF                  | Yes         |
+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+
| `Dask DF <https://github.com/dask/dask>`_         | No              | Python object | Yes         | No              | Some(Pandas)                | Yes         |
+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+

Check out our `engine comparison page <https://docs.getdaft.io/en/stable/resources/engine_comparison/>`_ for more details!

License
-------

Daft has an Apache 2.0 license - please see the LICENSE file.

.. |Quickstart Image| image:: https://github.com/Eventual-Inc/Daft/assets/17691182/dea2f515-9739-4f3e-ac58-cd96d51e44a8
   :alt: Dataframe code to load a folder of images from AWS S3 and create thumbnails
   :height: 256

.. |Benchmark Image| image:: https://github-production-user-asset-6210df.s3.amazonaws.com/2550285/243524430-338e427d-f049-40b3-b555-4059d6be7bfd.png
   :alt: Benchmarks for SF100 TPCH

.. |Banner| image:: https://daft.ai/images/diagram.png
   :target: https://www.daft.ai
   :alt: Daft dataframes can load any data such as PDF documents, images, protobufs, csv, parquet and audio files into a table dataframe structure for easy querying

.. |CI| image:: https://github.com/Eventual-Inc/Daft/actions/workflows/pr-test-suite.yml/badge.svg
   :target: https://github.com/Eventual-Inc/Daft/actions/workflows/pr-test-suite.yml?query=branch:main
   :alt: Github Actions tests

.. |PyPI| image:: https://img.shields.io/pypi/v/daft.svg?label=pip&logo=PyPI&logoColor=white
   :target: https://pypi.org/project/daft
   :alt: PyPI

.. |Latest Tag| image:: https://img.shields.io/github/v/tag/Eventual-Inc/Daft?label=latest&logo=GitHub
   :target: https://github.com/Eventual-Inc/Daft/tags
   :alt: latest tag

.. |Coverage| image:: https://codecov.io/gh/Eventual-Inc/Daft/branch/main/graph/badge.svg?token=J430QVFE89
   :target: https://codecov.io/gh/Eventual-Inc/Daft
   :alt: Coverage

.. |Slack| image:: https://img.shields.io/badge/slack-@distdata-purple.svg?logo=slack
   :target: https://join.slack.com/t/dist-data/shared_invite/zt-2e77olvxw-uyZcPPV1SRchhi8ah6ZCtg
   :alt: slack community
