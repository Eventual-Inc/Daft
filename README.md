[![daft](https://github.com/Eventual-Inc/Daft/actions/workflows/python-package.yml/badge.svg)](https://github.com/Eventual-Inc/Daft/actions/workflows/python-package.yml)

[Website](https://www.getdaft.io) • [Docs](https://www.getdaft.io) • [Installation](#installation) • [10-minute tour of Daft](https://getdaft.io/learn/10-min.html) • [Community and Support](https://github.com/Eventual-Inc/Daft/discussions)

# Daft: the distributed Python dataframe

[Daft](https://www.getdaft.io) is a fast, Pythonic and scalable open-source dataframe library built for Python and Machine Learning workloads.

<!-- toc -->

- [About Daft](#about-daft)
- [Installation](#installation)
- [Getting Started](#getting-started)
  - [Quickstart](#quickstart)
  - [More Resources](#more-resources)
- [License](#license)

<!-- tocstop -->

## About Daft

![Daft dataframes make it easy to load any data such as PDF documents, images, protobufs, csv, parquet and audio files into a table dataframe structure for easy querying](https://user-images.githubusercontent.com/17691182/190476440-28f29e87-8e3b-41c4-9c28-e112e595f558.png)

The Daft dataframe is a table of data with rows and columns.

Like many other dataframe libraries such as Pandas and PySpark, columns have a schema type and can hold values such as text, integers, floats and dates.

Additionally, Daft can handle **any type of data** and does so by providing first-class support for Python objects in your column. This means you can continue to use any open source libraries you might already be using such as Numpy, OpenCV, Pillow and PyTorch, or seamlessly integrate your own Python classes and modules.

### Interactive Computing

Daft :heart: Python for its interactive computing capabilities, which enables fast, iterative experimentation on data.

Daft embraces this philosophy by working seamlessly in a REPL environment such as a Jupyter notebook. It provides the best experience for working with messy real-world datasets consisting of a mishmash of documents, images and associated metadata by performing optimizations such as:

1. Intelligent caching
2. Selective scanning of source data
3. Visualization of common media types

### Distributed Computing

At its core Daft is built for distributed computing and we are obsessed with performance and scalability.

Daft outperforms Pandas when you are running locally. When your dataframe inevitably grows to gigabytes in size and can no longer fit on your local machine, Daft integrates with frameworks such as [Ray](https://www.ray.io) for running on a large cluster of machines in the cloud.

### Open Source

Daft is completely open sourced and integrates with many open data formats such as CSV, Apache Parquet and Apache Iceberg. It also supports all major cloud vendors' object storage options, allowing you to easily integrate with your existing storage solutions.

> **Daft is currently in its Alpha release phase - please expect bugs and rapid improvements to the project.**
> **We welcome user feedback/feature requests in our [Discussions forums](https://github.com/Eventual-Inc/Daft/discussions).**

## Installation

Install Daft with `pip install getdaft`.

## Getting Started

### Quickstart

> Check out our [full quickstart tutorial](https://getdaft.io/learn/quickstart.html)!

Load a dataframe - in this example we load the MNIST dataset from a JSON file, but Daft also supports many other formats such as CSV, Parquet and folders/buckets of files.

```python
from daft import DataFrame

URL = "https://github.com/Eventual-Inc/mnist-json/raw/master/mnist_handwritten_test.json.gz"

df = DataFrame.from_json(URL)
df.show(4)
```

<img width="359" alt="image" src="https://user-images.githubusercontent.com/17691182/197297244-79672651-0229-4763-9258-45d8afd48bae.png">

Filter the dataframe

```python
df = df.where(df["label"] == 5)
df.show(4)
```

<img width="359" alt="image" src="https://user-images.githubusercontent.com/17691182/197297274-3ae82ec2-a4bb-414c-b765-2a25c2933e34.png">

Run any function on the dataframe (here we convert a list of pixels into an image using Numpy and the Pillow libraries)

```python
import numpy as np
from PIL import Image

df = df.with_column(
    "image_pil",
    df["image"].apply(
        lambda pixels: Image.fromarray(np.array(pixels).reshape(28, 28).astype(np.uint8)),
        return_type=Image.Image,
    )
)
df.show(4)
```

<img width="427" alt="image" src="https://user-images.githubusercontent.com/17691182/197297304-9d25b7da-bbbd-4f82-b9e1-97cd4fb5187f.png">

### More Resources

* [10-minute tour of Daft](https://getdaft.io/learn/10-min.html) - learn more about Daft's full range of capabilities including dataloading from URLs, joins, user-defined functions (UDF), groupby, aggregations and more.
* [User Guide](https://getdaft.io/learn/user_guides.html) - take a deep-dive into each topic within Daft
* [API Reference](https://getdaft.io/api_docs.html) - API reference for public classes/functions of Daft

## License

Daft has a Apache 2.0 license - please see the LICENSE file.
