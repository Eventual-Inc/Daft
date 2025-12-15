|Banner|

|CI| |PyPI| |Latest Tag| |Coverage| |Slack|

`Website <https://www.daft.ai>`_ • `Docs <https://docs.daft.ai>`_ • `Installation <https://docs.daft.ai/en/stable/install/>`_ • `Daft Quickstart <https://docs.daft.ai/en/stable/quickstart/>`_ • `Community and Support <https://github.com/Eventual-Inc/Daft/discussions>`_

Daft: High-Performance Data Engine for AI and Multimodal Workloads
==================================================================

|TrendShift|

`Daft <https://www.daft.ai>`_ is a high-performance data engine for AI and multimodal workloads. Process images, audio, video, and structured data at any scale.

* **Native multimodal processing:** Process images, audio, video, and embeddings alongside structured data in a single framework
* **Built-in AI operations:** Run LLM prompts, generate embeddings, and classify data at scale using OpenAI, Transformers, or custom models
* **Python-native, Rust-powered:** Skip the JVM complexity with Python at its core and Rust under the hood for blazing performance
* **Seamless scaling:** Start local, scale to distributed clusters on `Ray <https://docs.daft.ai/en/stable/distributed/ray/>`_, `Kubernetes <https://docs.daft.ai/en/stable/distributed/kubernetes/>`_, or `Daft Cloud <https://www.daft.ai/cloud>`_
* **Universal connectivity:** Access data anywhere (S3, GCS, Iceberg, Delta Lake, Hugging Face, Unity Catalog)
* **Out-of-box reliability:** Intelligent memory management and sensible defaults eliminate configuration headaches

Getting Started
---------------

Installation
^^^^^^^^^^^^

Install Daft with ``pip install daft``. Requires Python 3.10 or higher.

For more advanced installations (e.g. installing from source or with extra dependencies such as Ray and AWS utilities), please see our `Installation Guide <https://docs.daft.ai/en/stable/install/>`_

Quickstart
^^^^^^^^^^

Get started in minutes with our `Quickstart <https://docs.daft.ai/en/stable/quickstart/>`_ - load a real-world e-commerce dataset, process product images, and run AI inference at scale.


More Resources
^^^^^^^^^^^^^^

* `Examples <https://docs.daft.ai/en/stable/examples/>`_ - see Daft in action with use cases across text, images, audio, and more
* `User Guide <https://docs.daft.ai/en/stable/>`_ - take a deep-dive into each topic within Daft
* `API Reference <https://docs.daft.ai/en/stable/api/>`_ - API reference for public classes/functions of Daft

Benchmarks
----------
|Benchmark Image|

To see the full benchmarks, detailed setup, and logs, check out our `benchmarking page. <https://docs.daft.ai/en/stable/benchmarks>`_

Contributing
------------

We <3 developers! To start contributing to Daft, please read `CONTRIBUTING.md <https://github.com/Eventual-Inc/Daft/blob/main/CONTRIBUTING.md>`_. This document describes the development lifecycle and toolchain for working on Daft. It also details how to add new functionality to the core engine and expose it through a Python API.

Here's a list of `good first issues <https://github.com/Eventual-Inc/Daft/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22>`_ to get yourself warmed up with Daft. Comment in the issue to pick it up, and feel free to ask any questions!

Telemetry
---------

To help improve Daft, we collect non-identifiable data via Scarf (https://scarf.sh).

To disable this behavior, set the environment variable ``DO_NOT_TRACK=true``.

The data that we collect is:

1. **Non-identifiable:** Events are keyed by a session ID which is generated on import of Daft
2. **Metadata-only:** We do not collect any of our users’ proprietary code or data
3. **For development only:** We do not buy or sell any user data

Please see our `documentation <https://docs.daft.ai/en/stable/resources/telemetry/>`_ for more details.

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
| `Modin <https://github.com/modin-project/modin>`_ | Yes             | Python object | Yes         | No              | Some(Pandas)                | Yes         |
+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+
| `Ray Data <https://github.com/ray-project/ray>`_  | No              | Yes           | Yes         | Yes             | Some(PyArrow)               | Yes         |
+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+
| `PySpark <https://github.com/apache/spark>`_      | Yes             | No            | Yes         | Pandas UDF/IO   | Pandas UDF                  | Yes         |
+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+
| `Dask DF <https://github.com/dask/dask>`_         | No              | Python object | Yes         | No              | Some(Pandas)                | Yes         |
+---------------------------------------------------+-----------------+---------------+-------------+-----------------+-----------------------------+-------------+

License
-------

Daft has an Apache 2.0 license - please see the LICENSE file.

.. |Quickstart Image| image:: https://github.com/Eventual-Inc/Daft/assets/17691182/dea2f515-9739-4f3e-ac58-cd96d51e44a8
   :alt: Dataframe code to load a folder of images from AWS S3 and create thumbnails
   :height: 256

.. |Benchmark Image| image:: https://raw.githubusercontent.com/Eventual-Inc/Daft/refs/heads/main/assets/benchmark.png
   :alt: AI Benchmarks

.. |Banner| image:: https://daft.ai/images/diagram.png
   :target: https://www.daft.ai
   :alt: Daft dataframes can load any data such as PDF documents, images, protobufs, csv, parquet and audio files into a table dataframe structure for easy querying

.. |CI| image:: https://github.com/Eventual-Inc/Daft/actions/workflows/pr-test-suite.yml/badge.svg
   :target: https://github.com/Eventual-Inc/Daft/actions/workflows/pr-test-suite.yml?query=branch:main
   :alt: GitHub Actions tests

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
   :target: https://join.slack.com/t/dist-data/shared_invite/zt-3l411uyaq-Ky30cBfnirJVlKDnsCRM_w
   :alt: slack community

.. |TrendShift| image:: https://trendshift.io/api/badge/repositories/8239
   :target: https://trendshift.io/repositories/8239
   :alt: Eventual-Inc/Daft | Trendshift
   :width: 250px
   :height: 55px
