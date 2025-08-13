# Examples

## [Document Processing](./document-processing.md)

Load a collection of PDFs from S3, OCR or extract text from them, run layout analysis to group text boxes into paragraphs, then compute text embeddings using a locally running LLM. Also showcases how to use custom Pydantic classes as Daft DataTypes for UDFs.

<div class="grid cards examples" markdown>

- [![colab](../img/colab.png) Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/document_processing/document_processing_tutorial.ipynb)

- [![thumbnail](../img/doc_proc.png) Follow along with the tutorial video](https://youtu.be/BLcKDQRTFKY)

</div>

## [Generate Text Embeddings for Turbopuffer](./text-embeddings.md)

Generate embeddings on text data then store them in a vector database.


## [Running LLMs on the Red Pajamas Dataset](./llms-red-pajamas.md)

Load the Red Pajamas dataset and perform similarity search on Stack Exchange questions using language models and embeddings.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/embeddings/daft_tutorial_embeddings_stackexchange.ipynb)

## [Generate Images from Text with Stable Diffusion](./image-generation.md)

Generate images from text prompts using a deep learning model (Stable Diffusion) and Daft UDFs. Run Daft UDFs on GPUs for more efficient resource allocation.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/text_to_image/text_to_image_generation.ipynb)


## [Querying Images with UDFs](./querying-images.md)

Query the Open Images dataset to retrieve the top N "reddest" images. This tutorial uses common open-source tools such as numpy and Pillow inside Daft UDFs to execute this query.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/image_querying/top_n_red_color.ipynb)


## [MNIST Digit Classification](./mnist.md)

Load the MNIST image dataset and use a simple deep learning model to run classification on each image. Evaluate the model's performance with simple aggregations.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/mnist.ipynb)

## [Window Functions](./window-functions.md)

Compare traditional join operations with more efficient window functions for ranking, calculating deltas, and tracking cumulative sums.

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/window_functions/window_functions.ipynb)
