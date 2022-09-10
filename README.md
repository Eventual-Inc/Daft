[![daft](https://github.com/Eventual-Inc/Daft/actions/workflows/python-package.yml/badge.svg)](https://github.com/Eventual-Inc/Daft/actions/workflows/python-package.yml)

# Daft

Daft is a fast, ergonomic and scalable open-source dataframe library: built for Python and Complex Data/Machine Learning workloads.

## Installation

Install Daft with `pip install getdaft`.

## Documentation

Learn more about Daft in our [documentation](https://docs.getdaft.io).

## Why Daft?

Processing Complex Data such as images/audio/pointclouds often requires accelerated compute for geometric or machine learning algorithms, much of which leverages existing tooling from the Python/C++ ecosystem. However, many workloads such as analytics, model training data curation and data processing often also require relational query operations for loading/filtering/joining/aggregations.

Daft marries the two worlds with a Dataframe API, enabling you to run both large analytical queries and powerful Complex Data algorithms from the same interface.

1. **Python-first**: Python and Jupyter notebooks are first-class citizens. Daft handles any Python libraries and datastructures natively - use any Python library such as Numpy, OpenCV and PyTorch for Complex Data processing.

2. **Laptop to Cloud**: Daft is built to run as easily on your laptop for interactive development and on your own [Ray](https://www.ray.io) cluster or [Eventual](https://www.eventualcomputing.com) deployment for terabyte-scale production workloads.

3. **Open Data Formats**: Daft loads from and writes to open data formats such as Apache Parquet and Apache Iceberg. It also supports all major cloud vendors' object storage options, allowing you to easily integrate with your existing storage solutions.
