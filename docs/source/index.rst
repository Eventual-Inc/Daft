Daft Documentation
==================

Daft is a unified data engine for **data engineering, analytics and ML/AI**.

Daft exposes both a **SQL and Python DataFrame interface** and is implemented in Rust.

Daft provides a **snappy and delightful local interactive experience**, but also seamlessly **scales to petabyte-scale distributed workloads**.

Use-Cases
---------

Data Engineering
****************

*Provides the local performance and memory stability of DuckDB/Polars with the scalability of Apache Spark*

* **Extract → Transform → Load (ETL):** Perform data engineering on messy multimodal data at scales ranging from MB to PB, on a single node or a distributed cluster
* **Cloud-native:** Native integrations with modern cloud storage (e.g. S3), open catalogs/table formats (e.g. Apache Iceberg, DeltaLake) and open data formats (e.g. Apache Parquet)

Data Analytics
**************

*Provides a SQL interface with the snappiness of local engines such as DuckDB and scalability of engines such as Spark/Trino*

* **Local Analytics:** Snappy interactive data exploration and aggregations from Python notebooks using DataFrames or SQL with the performance/development experience of local engines such as DuckDB/Polars
* **Distributed Analytics:** Powerful capabilities to scale to the cloud when required to process larger datasets, outperforming distributed analytics engines such as Spark and Trino

ML/AI
*****

*Replaces opinionated data formats such as Mosaic Data Shard (MDS) or TFRecords with dataloading directly from open formats (Apache Parquet, JPEG) into Pytorch or Numpy while saturating network bandwidth*

* **Dataloading for training:** Fast and memory efficient dataloaders from open file formats such as Parquet and JPEG
* **Model batch inference on GPUs:** Schedule large-scale model batch inference on a fleet of GPUs on a distributed cluster.

Technology
----------

Daft boasts strong integrations with technologies common across these workloads:

* **Cloud Object Storage:** Record-setting I/O performance for integrations with S3 cloud storage, `battle-tested at exabyte-scale at Amazon <https://aws.amazon.com/blogs/opensource/amazons-exabyte-scale-migration-from-apache-spark-to-ray-on-amazon-ec2/>`_
* **ML/AI Python Ecosystem:** first-class integrations with `PyTorch <https://pytorch.org/>`_ and `NumPy <https://numpy.org/>`_ for efficient interoperability with your ML/AI stack
* **Data Catalogs/Table Formats:** capabilities to effectively query table formats such as `Apache Iceberg <https://iceberg.apache.org/>`_, `Delta Lake <https://delta.io/>`_ and `Apache Hudi <https://hudi.apache.org/>`_
* **Seamless Data Interchange:** zero-copy integration with `Apache Arrow <https://arrow.apache.org/docs/index.html>`_
* **Multimodal/ML Data:** native functionality for data modalities such as tensors, images, URLs, long-form text and embeddings


Installing Daft
---------------

To install Daft, run this from your terminal:

``pip install getdaft``

Learn about other more advanced installation options in our :doc:`Installation Guide <install>`.


Learning Daft
-------------

* :doc:`Quickstart Notebook <10-min>`: up-and-running with Daft in less than 10 minutes!
* :doc:`Daft User Guide <user_guide/index>`: useful learning material to learn key Daft concepts
* :doc:`Daft API Documentation <api_docs/index>`: Python API documentation for reference

Frequently Asked Questions
--------------------------

* **How do I know if Daft is the right framework for me?**: :doc:`faq/dataframe_comparison`
* **How does Daft perform at large scales vs other data engines?**: :doc:`faq/benchmarks`
* **What is the technical architecture of Daft?**: :doc:`faq/technical_architecture`
* **Does Daft perform any telemetry?**: :doc:`faq/telemetry`

.. toctree::
   :hidden:

   Home <self>
   install
   10-min
   user_guide/index
   api_docs/index
   migration_guides/index
   FAQs <faq/index>
   Release Notes <https://github.com/Eventual-Inc/Daft/releases>


.. Indices and tables
.. ==================

.. * :ref:`genindex`
.. * :ref:`modindex`
.. * :ref:`search`
