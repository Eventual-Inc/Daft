|Banner|

|CI| |PyPI| |Latest Tag|

`Website <https://www.getdaft.io>`_ • `Docs <https://www.getdaft.io/docs>`_ • `Installation`_ • `10-minute tour of Daft <https://getdaft.io/docs/learn/10-min.html>`_ • `Community and Support <https://github.com/Eventual-Inc/Daft/discussions>`_

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

Quickstart
^^^^^^^^^^

  Check out our `full quickstart tutorial <https://getdaft.io/docs/learn/quickstart.html>`_!

In this example, we load images from an AWS S3 bucket and run a simple function to generate thumbnails for each image:

.. code:: python

    from daft import DataFrame, lit

    import io
    from PIL import Image

    def get_thumbnail(img: Image.Image) -> Image.Image:
        """Simple function to make an image thumbnail"""
        imgcopy = img.copy()
        imgcopy.thumbnail((48, 48))
        return imgcopy

    # Load a dataframe from files in an S3 bucket
    df = DataFrame.from_files("s3://daft-public-data/laion-sample-images/*")

    # Get the AWS S3 url of each image
    df = df.select(lit("s3://").str.concat(df["name"]).alias("s3_url"))

    # Download images and load as a PIL Image object
    df = df.with_column("image", df["s3_url"].url.download().apply(lambda data: Image.open(io.BytesIO(data))))

    # Generate thumbnails from images
    df = df.with_column("thumbnail", df["image"].apply(get_thumbnail))

    df.show(3)

|Quickstart Image|


More Resources
^^^^^^^^^^^^^^

* `10-minute tour of Daft <https://getdaft.io/docs/learn/10-min.html>`_ - learn more about Daft's full range of capabilities including dataloading from URLs, joins, user-defined functions (UDF), groupby, aggregations and more.
* `User Guide <https://getdaft.io/docs/learn/user_guides.html>`_ - take a deep-dive into each topic within Daft
* `API Reference <https://getdaft.io/docs/api_docs/index.html>`_ - API reference for public classes/functions of Daft

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
