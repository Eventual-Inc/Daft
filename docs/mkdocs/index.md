# Overview

Welcome to **Daft**!

Daft is a unified data engine for **data engineering, analytics, and ML/AI**. It exposes both **SQL and Python DataFrame interfaces** as first-class citizens and is written in Rust. Daft provides a **snappy and delightful local interactive experience**, but also seamlessly **scales to petabyte-scale distributed workloads**.

## Use Cases

**:material-database-settings: Data Engineering**

*Combine the performance of DuckDB, Pythonic UX of Polars and scalability of Apache Spark for data engineering from MB to PB scale*

- Scale ETL workflows effortlessly from local to distributed environments
- Enjoy a Python-first experience without JVM dependency hell
- Leverage native integrations with cloud storage, open catalogs, and data formats
---

**:simple-simpleanalytics: Data Analytics**

*Blend the snappiness of DuckDB with the scalability of Spark/Trino for unified local and distributed analytics*

- Utilize complementary SQL and Python interfaces for versatile analytics
- Perform snappy local exploration with DuckDB-like performance
- Seamlessly scale to the cloud, outperforming distributed engines like Spark and Trino
---

**:fontawesome-solid-gear: ML/AI**

*Streamline ML/AI workflows with efficient dataloading from open formats like Parquet and JPEG*

- Load data efficiently from open formats directly into PyTorch or NumPy
- Schedule large-scale model batch inference on distributed GPU clusters
- Optimize data curation with advanced clustering, deduplication, and filtering

## Technology

Daft boasts strong integrations with technologies common across these workloads:

* **Cloud Object Storage:** Record-setting I/O performance for integrations with S3 cloud storage, [battle-tested at exabyte-scale at Amazon](https://aws.amazon.com/blogs/opensource/amazons-exabyte-scale-migration-from-apache-spark-to-ray-on-amazon-ec2/)
* **ML/AI Python Ecosystem:** First-class integrations with [PyTorch](https://pytorch.org/>) and [NumPy](https://numpy.org/) for efficient interoperability with your ML/AI stack
* **Data Catalogs/Table Formats:** Capabilities to effectively query table formats such as [Apache Iceberg](https://iceberg.apache.org/), [Delta Lake](https://delta.io/) and [Apache Hudi](https://hudi.apache.org/)
* **Seamless Data Interchange:** Zero-copy integration with [Apache Arrow](https://arrow.apache.org/docs/index.html)
* **Multimodal/ML Data:** Native functionality for data modalities such as tensors, images, URLs, long-form text and embeddings

## Learning Daft

This user guide aims to help Daft users master the usage of Daft for all your data needs.

!!! tip "Looking to get started with Daft ASAP?"

    The Daft User Guide is a useful resource to take deeper dives into specific Daft concepts, but if you are ready to jump into code you may wish to take a look at these resources:

    1. [Quickstart](quickstart.md): Itching to run some Daft code? Hit the ground running with our 10 minute quickstart notebook.

    2. [API Documentation](api_docs/index.html): Searchable documentation and reference material to Daft’s public API.

### Get Started

<div class="grid cards" markdown>

- [:material-download: **Installing Daft**](install.md)

    Install Daft from your terminal and discover more advanced installation options.

- [:material-clock-fast: **Quickstart**](quickstart.md)

    Install Daft, create your first DataFrame, and get started with common DataFrame operations.

- [:octicons-book-16: **Terminology**](terms.md)

    Learn about the terminology related to Daft, such as DataFrames, Expressions, Query Plans, and more.

- [:simple-elasticstack: **Architecture**](resources/architecture.md)

    Understand the different components to Daft under-the-hood.

</div>

### Daft in Depth

<div class="grid cards" markdown>

- [:material-filter: **DataFrame Operations**](core_concepts.md#dataframe)

    Learn how to perform core DataFrame operations in Daft, including selection, filtering, joining, and sorting.

- [:octicons-code-16: **Expressions**](core_concepts.md#expressions)

    Daft expressions enable computations on DataFrame columns using Python or SQL for various operations.

- [:material-file-eye: **Reading Data**](core_concepts.md#reading-data)

    How to use Daft to read data from diverse sources like files, databases, and URLs.

- [:material-file-edit: **Writing Data**](core_concepts.md#reading-data)

    How to use Daft to write data DataFrames to files or other destinations.

- [:fontawesome-solid-square-binary: **DataTypes**](core_concepts.md#datatypes)

    Daft DataTypes define the types of data in a DataFrame, from simple primitives to complex structures.

- [:simple-quicklook: **SQL**](core_concepts.md#sql)

    Daft supports SQL for constructing query plans and expressions, while integrating with Python expressions.

- [:material-select-group: **Aggregations and Grouping**](core_concepts.md#aggregations-and-grouping)

    Daft supports aggregations and grouping across entire DataFrames and within grouped subsets of data.

- [:fontawesome-solid-user: **User-Defined Functions (UDFs)**](core_concepts.md#user-defined-functions-udf)

    Daft allows you to define custom UDFs to process data at scale with flexibility in input and output.

- [:octicons-image-16: **Multimodal Data**](core_concepts.md#multimodal-data)

    Daft is built to work with multimodal data types, including URLs and images.

</div>

### More Resources

<div class="grid cards" markdown>

- [:material-star-shooting: **Advanced Daft**](advanced/memory.md)
- [:material-file-compare: **DataFrame Comparison**](resources/dataframe_comparison.md)
- [:material-file-document: **Tutorials**](resources/tutorials.md)
- [:material-chart-bar: **Benchmarks**](resources/benchmarks/tpch.md)

</div>

## Contribute to Daft

If you're interested in hands-on learning about Daft internals and would like to contribute to our project, join us [on Github](https://github.com/Eventual-Inc/Daft) 🚀

Take a look at the many issues tagged with `good first issue` in our repo. If there are any that interest you, feel free to chime in on the issue itself or join us in our [Distributed Data Slack Community](https://join.slack.com/t/dist-data/shared_invite/zt-2e77olvxw-uyZcPPV1SRchhi8ah6ZCtg) and send us a message in #daft-dev. Daft team members will be happy to assign any issue to you and provide any guidance if needed!

<!-- ## Frequently Asked Questions

todo(docs - jay): Add answers to each and more questions if necessary

??? quote "What does Daft do well? (or What should I use Daft for?)"

    todo(docs): this is from 10 min quickstart, filler answer for now

    Daft is the right tool for you if you are working with:

    - **Large datasets** that don't fit into memory or would benefit from parallelization
    - **Multimodal data types** such as images, JSON, vector embeddings, and tensors
    - **Formats that support data skipping** through automatic partition pruning and stats-based file pruning for filter predicates
    - **ML workloads** that would benefit from interact computation within a DataFrame (via UDFs)

??? quote "What should I *not* use Daft for?"

??? quote "How do I know if Daft is the right framework for me?"

    See [DataFrame Comparison](resources/dataframe_comparison.md)

??? quote "What is the difference between Daft and Ray?"

??? quote "What is the difference between Daft and Spark?"

??? quote "How does Daft perform at large scales vs other data engines?"

    See [Benchmarks](resources/benchmarks/tpch.md)

??? quote "What is the technical architecture of Daft?"

    See [Technical Architecture](resources/architecture.md)

??? quote "Does Daft perform any telemetry?"

    See [Telemetry](resources/telemetry.md) -->
