Daft Documentation
==================

Daft is a unified data engine for **data engineering, analytics and ML/AI**.

Daft exposes both **SQL and Python DataFrame interfaces** as first-class citizens and is written in Rust.

Daft provides a **snappy and delightful local interactive experience**, but also seamlessly **scales to petabyte-scale distributed workloads**.

Use-Cases
---------

Data Engineering
****************

*Combine the performance of DuckDB, Pythonic UX of Polars and scalability of Apache Spark for data engineering from MB to PB scale*

* Scale ETL workflows effortlessly from local to distributed environments
* Enjoy a Python-first experience without JVM dependency hell
* Leverage native integrations with cloud storage, open catalogs, and data formats

Data Analytics
**************

*Blend the snappiness of DuckDB with the scalability of Spark/Trino for unified local and distributed analytics*

* Utilize complementary SQL and Python interfaces for versatile analytics
* Perform snappy local exploration with DuckDB-like performance
* Seamlessly scale to the cloud, outperforming distributed engines like Spark and Trino

ML/AI
*****

*Streamline ML/AI workflows with efficient dataloading from open formats like Parquet and JPEG*

* Load data efficiently from open formats directly into PyTorch or NumPy
* Schedule large-scale model batch inference on distributed GPU clusters
* Optimize data curation with advanced clustering, deduplication, and filtering

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
