# Examples

## MNIST Digit Classification

Load the MNIST image dataset and use a simple deep learning model to run classification on each image. Evaluate the model's performance with simple aggregations.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/mnist.ipynb)


## Running LLMs on the Red Pajamas Dataset

Load the Red Pajamas dataset and perform similarity search on Stack Exchange questions using language models and embeddings.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/embeddings/daft_tutorial_embeddings_stackexchange.ipynb)

## Querying Images with UDFs

Query the Open Images dataset to retrieve the top N "reddest" images. This tutorial uses common open-source tools such as numpy and Pillow inside Daft UDFs to execute this query.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/image_querying/top_n_red_color.ipynb)

## Image Generation on GPUs

Generate images from text prompts using a deep learning model (Stable Diffusion) and Daft UDFs. Run Daft UDFs on GPUs for more efficient resource allocation.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/text_to_image/text_to_image_generation.ipynb)

## Window Functions

Compare traditional join operations with more efficient window functions for ranking, calculating deltas, and tracking cumulative sums.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/window_functions/window_functions.ipynb)

## Document Processing

Load a collection of PDFs from S3, OCR or extract text from them, run layout analysis to group text boxes into paragraphs, then compute text embeddings using a locally running LLM. Also showcases how to use custom Pydantic classes as Daft DataTypes for UDFs.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/document_processing/document_processing_tutorial.ipynb)
