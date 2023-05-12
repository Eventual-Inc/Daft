|Banner|

|CI| |PyPI| |Latest Tag| |Coverage| |Slack|

`Website <https://www.getdaft.io>`_ • `Docs <https://www.getdaft.io/projects/docs/>`_ • `Installation`_ • `10-minute tour of Daft <https://www.getdaft.io/projects/docs/en/latest/learn/10-min.html>`_ • `Community and Support <https://github.com/Eventual-Inc/Daft/discussions>`_

Daft: the distributed Python dataframe for complex data
=======================================================


`Daft <https://www.getdaft.io>`_ is a fast, Pythonic and scalable open-source dataframe library built for Python and Machine Learning workloads.

  **Daft is currently in its Alpha release phase - please expect bugs and rapid improvements to the project.**
  **We welcome user feedback/feature requests in our** `Discussions forums <https://github.com/Eventual-Inc/Daft/discussions>`_

**Table of Contents**

* `About Daft`_
* `Getting Started`_
* `License`_

About Daft
----------

The Daft dataframe is a table of data with rows and columns. Columns can contain any Python objects, which allows Daft to support rich complex data types such as images, audio, video and more.

1. **Any Data**: Columns can contain any Python objects, which means that the Python libraries you already use for running machine learning or custom data processing will work natively with Daft!
2. **Notebook Computing**: Daft is built for the interactive developer experience on a notebook - intelligent caching/query optimizations accelerates your experimentation and data exploration.
3. **Distributed Computing**: Rich complex formats such as images can quickly outgrow your local laptop's computational resources - Daft integrates natively with `Ray <https://www.ray.io>`_ for running dataframes on large clusters of machines with thousands of CPUs/GPUs.

Getting Started
---------------

Installation
^^^^^^^^^^^^

Install Daft with ``pip install getdaft``.

For more advanced installations (e.g. installing from source or with extra dependencies such as Ray and AWS utilities), please see our `Installation Guide <https://www.getdaft.io/projects/docs/en/latest/install.html>`_

Quickstart
^^^^^^^^^^

  Check out our `10-minute quickstart <https://www.getdaft.io/projects/docs/en/latest/learn/10-min.html>`_!

In this example, we load images from an AWS S3 bucket and run a simple function to generate thumbnails for each image:

.. code:: python

    import daft as daft

    import io
    from PIL import Image

    def get_thumbnail(img: Image.Image) -> Image.Image:
        """Simple function to make an image thumbnail"""
        imgcopy = img.copy()
        imgcopy.thumbnail((48, 48))
        return imgcopy

    # Load a dataframe from files in an S3 bucket
    df = daft.from_glob_path("s3://daft-public-data/laion-sample-images/*")

    # Get the AWS S3 url of each image
    df = df.select(df["path"].alias("s3_url"))

    # Download images and load as a PIL Image object
    df = df.with_column("image", df["s3_url"].url.download().apply(lambda data: Image.open(io.BytesIO(data)), return_dtype=daft.DataType.python()))

    # Generate thumbnails from images
    df = df.with_column("thumbnail", df["image"].apply(get_thumbnail, return_dtype=daft.DataType.python()))

    df.show(3)

|Quickstart Image|


More Resources
^^^^^^^^^^^^^^

* `10-minute tour of Daft <https://www.getdaft.io/projects/docs/en/latest/learn/10-min.html>`_ - learn more about Daft's full range of capabilities including dataloading from URLs, joins, user-defined functions (UDF), groupby, aggregations and more.
* `User Guide <https://www.getdaft.io/projects/docs/en/latest/learn/user_guides.html>`_ - take a deep-dive into each topic within Daft
* `API Reference <https://www.getdaft.io/projects/docs/en/latest/api_docs/index.html>`_ - API reference for public classes/functions of Daft

Contributing
------------

To start contributing to Daft, please read `CONTRIBUTING.md <https://github.com/Eventual-Inc/Daft/blob/main/CONTRIBUTING.md>`_

Telemetry
---------

To help improve Daft, we collect non-identifiable data.

To disable this behavior, set the following environment variable: ``DAFT_ANALYTICS_ENABLED=0``

The data that we collect is:

1. **Non-identifiable:** events are keyed by a session ID which is generated on import of Daft
2. **Metadata-only:** we do not collect any of our users’ proprietary code or data
3. **For development only:** we do not buy or sell any user data

Please see our `documentation <https://www.getdaft.io/projects/docs/en/latest/telemetry.html>`_ for more details.

License
-------

Daft has an Apache 2.0 license - please see the LICENSE file.

.. |Quickstart Image| image:: https://user-images.githubusercontent.com/17691182/200086119-fb73037b-8b4e-414a-9060-a44122f0c290.png
   :alt: Dataframe code to load a folder of images from AWS S3 and create thumbnails
   :height: 256

.. |Banner| image:: https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png
   :target: https://www.getdaft.io
   :alt: Daft dataframes can load any data such as PDF documents, images, protobufs, csv, parquet and audio files into a table dataframe structure for easy querying

.. |CI| image:: https://github.com/Eventual-Inc/Daft/actions/workflows/python-package.yml/badge.svg
   :target: https://github.com/Eventual-Inc/Daft/actions/workflows/python-package.yml?query=branch:main
   :alt: Github Actions tests

.. |PyPI| image:: https://img.shields.io/pypi/v/getdaft.svg?label=pip&logo=PyPI&logoColor=white
   :target: https://pypi.org/project/getdaft
   :alt: PyPI

.. |Latest Tag| image:: https://img.shields.io/github/v/tag/Eventual-Inc/Daft?label=latest&logo=GitHub
   :target: https://github.com/Eventual-Inc/Daft/tags
   :alt: latest tag

.. |Coverage| image:: https://codecov.io/gh/Eventual-Inc/Daft/branch/main/graph/badge.svg?token=J430QVFE89
   :target: https://codecov.io/gh/Eventual-Inc/Daft
   :alt: Coverage

.. |Slack| image:: https://img.shields.io/badge/slack-@distdata-purple.svg?logo=slack
   :target: https://join.slack.com/t/dist-data/shared_invite/zt-1t44ss4za-1rtsJNIsQOnjlf8BlG05yw
   :alt: slack community
