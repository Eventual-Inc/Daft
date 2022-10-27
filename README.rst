|Banner|

|CI| |PyPI| |Latest Tag|

`Website <https://www.getdaft.io>`_ • `Docs <https://www.getdaft.io>`_ • `Installation`_ • `10-minute tour of Daft <https://getdaft.io/learn/10-min.html>`_ • `Community and Support <https://github.com/Eventual-Inc/Daft/discussions>`_

Daft: the distributed Python dataframe for media data
=====================================================


`Daft <https://www.getdaft.io>`_ is a fast, Pythonic and scalable open-source dataframe library built for Python and Machine Learning workloads.

  **Daft is currently in its Alpha release phase - please expect bugs and rapid improvements to the project.**
  **We welcome user feedback/feature requests in our** `Discussions forums <https://github.com/Eventual-Inc/Daft/discussions>`_

**Table of Contents**

* `About Daft`_
* `Getting Started`_
* `License`_

About Daft
----------

The Daft dataframe is a table of data with rows and columns. Columns can contain any Python objects, which allows Daft to support rich media data types such as images, audio, video and more.

1. **Any Data**: Columns can contain any Python objects, which means that the Python libraries you already use for running machine learning or custom data processing will work natively with Daft!
2. **Notebook Computing**: Daft is built for the interactive developer experience on a notebook - intelligent caching/query optimizations accelerates your experimentation and data exploration.
3. **Distributed Computing**: Rich media formats such as images can quickly outgrow your local laptop's computational resources - Daft integrates natively with `Ray <https://www.ray.io>`_ for running dataframes on large clusters of machines with thousands of CPUs/GPUs.

Getting Started
---------------

Installation
^^^^^^^^^^^^

Install Daft with ``pip install getdaft``.

Quickstart
^^^^^^^^^^

  Check out our `full quickstart tutorial <https://getdaft.io/learn/quickstart.html>`_!

Load a dataframe - in this example we load the MNIST dataset from a JSON file, but Daft also supports many other formats such as CSV, Parquet and folders/buckets of files.

.. code:: python

  from daft import DataFrame

  URL = "https://github.com/Eventual-Inc/mnist-json/raw/master/mnist_handwritten_test.json.gz"

  df = DataFrame.from_json(URL)
  df.show(4)

|MNIST dataframe show|

Filter the dataframe for rows where the ``"label"`` column is equal to 5

.. code:: python

  df = df.where(df["label"] == 5)
  df.show(4)

|MNIST filtered dataframe show|

Run any function on the dataframe (here we convert a list of pixels into an image using Numpy and the Pillow libraries)

.. code:: python

  import numpy as np
  from PIL import Image

  def image_from_pixel_list(pixels: list) -> Image.Image:
      arr = np.array(pixels).astype(np.uint8)
      return Image.fromarray(arr.reshape(28, 28))

  df = df.with_column(
      "image_pil",
      df["image"].apply(image_from_pixel_list),
  )
  df.show(4)

|MNIST dataframe with Pillow show|

More Resources
^^^^^^^^^^^^^^

* `10-minute tour of Daft <https://getdaft.io/learn/10-min.html>`_ - learn more about Daft's full range of capabilities including dataloading from URLs, joins, user-defined functions (UDF), groupby, aggregations and more.
* `User Guide <https://getdaft.io/learn/user_guides.html>`_ - take a deep-dive into each topic within Daft
* `API Reference <https://getdaft.io/api_docs.html>`_ - API reference for public classes/functions of Daft

License
-------

Daft has an Apache 2.0 license - please see the LICENSE file.


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

.. |MNIST dataframe show| image:: https://user-images.githubusercontent.com/17691182/197297244-79672651-0229-4763-9258-45d8afd48bae.png
  :alt: dataframe of MNIST dataset with Python list of pixels

.. |MNIST filtered dataframe show| image:: https://user-images.githubusercontent.com/17691182/197297274-3ae82ec2-a4bb-414c-b765-2a25c2933e34.png
  :alt: dataframe of MNIST dataset filtered for rows where the label is the digit 5

.. |MNIST dataframe with Pillow show| image:: https://user-images.githubusercontent.com/17691182/197297304-9d25b7da-bbbd-4f82-b9e1-97cd4fb5187f.png
  :alt: dataframe of MNIST dataset with the Python list of pixel values converted to a Pillow image
