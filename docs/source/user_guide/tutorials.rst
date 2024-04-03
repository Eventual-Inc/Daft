Tutorials
=========

MNIST Digit Classification
--------------------------

Load the MNIST image dataset and use a simple deep learning model to run classification on each image. Evaluate the model's performance with simple aggregations.

`Run this tutorial on Google Colab <https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/mnist.ipynb>`__


Running LLMs on the Red Pajamas Dataset
---------------------------------------

Load the Red Pajamas dataset and perform similarity search on Stack Exchange questions using language models and embeddings.

`Run this tutorial on Google Colab <https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/embeddings/daft_tutorial_embeddings_stackexchange.ipynb>`__

Querying Images with UDFs
-------------------------

Query the Open Images dataset to retrieve the top N "reddest" images. This tutorial uses common open-source tools such as numpy and Pillow inside Daft UDFs to execute this query.

`Run this tutorial on Google Colab <https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/image_querying/top_n_red_color.ipynb>`__

Image generation on GPUs
------------------------

Generate images from text prompts using a deep learning model (Mini DALL-E) and Daft UDFs. Run Daft UDFs on GPUs for more efficient resource allocation.

`Run this tutorial on Google Colab <https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/text_to_image/text_to_image_generation.ipynb>`__


.. These can't be run because DeltaLake can't be accessed in anonymous mode from Google Colab
.. ML model batch inference/training on a Data Catalog
.. ---------------------------------------------------

.. Run ML models or train them on data in your data catalog (e.g. Apache Iceberg, DeltaLake or Hudi)

.. 1. `Local batch inference <https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/delta_lake/1-local-image-batch-inference.ipynb>`__
.. 1. `Distributed batch inference <https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/delta_lake/2-distributed-batch-inferece.ipynb>`__
.. 1. `Single-node Pytorch model training <https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/delta_lake/3-pytorch-ray-single-node-training.ipynb>`__



.. Other ideas:
.. Scaling up in the cloud with Ray **[Coming Soon]**
.. Building a HTTP service **[Coming Soon]**
.. Interacting with external services to build a data annotation pipeline **[Coming Soon]**
.. Data preparation for ML model training **[Coming Soon]**
