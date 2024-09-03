Daft Documentation
==================

Daft is a distributed query engine for large-scale data processing in Python and is implemented in Rust.

* **Familiar interactive API:** Lazy Python Dataframe for rapid and interactive iteration
* **Focus on the what:** Powerful Query Optimizer that rewrites queries to be as efficient as possible
* **Data Catalog integrations:** Full integration with data catalogs such as Apache Iceberg
* **Rich multimodal type-system:** Supports multimodal types such as Images, URLs, Tensors and more
* **Seamless Interchange**: Built on the `Apache Arrow <https://arrow.apache.org/docs/index.html>`_ In-Memory Format
* **Built for the cloud:** `Record-setting <https://blog.getdaft.io/p/announcing-daft-02-10x-faster-io>`_ I/O performance for integrations with S3 cloud storage

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
