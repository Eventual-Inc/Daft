# Overview

Welcome to **Daft**!

Daft is a high-performance data engine providing simple and reliable data processing for any modality and scale, from local to petabyte-scale distributed workloads. The core engine is written in Rust and exposes both SQL and Python DataFrame interfaces as first-class citizens.

## Why Daft?

**:octicons-image-24: Unified multimodal data processing**

Break down data silos with a single framework that handles structured tables, unstructured text, and rich media like images—all with the same intuitive API. Why juggle multiple tools when one can do it all?

**:material-language-python: Python-native, no JVM required**

Built for modern AI/ML workflows with Python at its core and Rust under the hood. Skip the JVM complexity, version conflicts, and memory tuning to achieve 20x faster start times—get the performance without the Java tax.

**:fontawesome-solid-laptop: Seamless scaling, from laptop to cluster**

Start local, scale global—without changing a line of code. Daft's Rust-powered engine delivers blazing performance on a single machine and effortlessly extends to distributed clusters with when you need more horsepower.

## Key Features

* **Native Multimodal Processing**: Process any data type—from structured tables to unstructured text and rich media—with native support for images, embeddings, and tensors in a single, unified framework.

* **Rust-Powered Performance**: Experience breakthrough speed with our Rust foundation delivering vectorized execution and non-blocking I/O that processes the same queries with 5x less memory while consistently outperforming industry standards by an order of magnitude.

* **Seamless ML Ecosystem Integration**: Slot directly into your existing ML workflows with zero friction—whether you're using [PyTorch](https://pytorch.org/), [NumPy](https://numpy.org/), [Pandas](https://pola.rs/), or [HuggingFace models](https://huggingface.co/models), Daft works where you work.

* **Universal Data Connectivity**: Access data anywhere it lives—cloud storage ([S3](https://aws.amazon.com/s3/), [Azure](https://azure.microsoft.com/en-us/), [GCS](https://cloud.google.com/storage)), modern table formats ([Apache Iceberg](https://iceberg.apache.org/), [Delta Lake](https://delta.io/), [Apache Hudi](https://hudi.apache.org/)), or enterprise catalogs ([Unity](https://www.unitycatalog.io/), [AWS Glue](https://aws.amazon.com/glue/))—all with zero configuration.

* **Push your code to your data**: Bring your Python functions directly to your data with zero-copy UDFs powered by [Apache Arrow](https://arrow.apache.org/), eliminating data movement overhead and accelerating processing speeds.

* **Out of the Box reliability**: Deploy with confidence—intelligent memory management prevents OOM errors while sensible defaults eliminate configuration headaches, letting you focus on results, not infrastructure.

## Learning Daft

This user guide aims to help Daft users master the usage of Daft for all your data needs.

!!! tip "Looking to get started with Daft ASAP?"

    The Daft User Guide is a useful resource to take deeper dives into specific Daft concepts, but if you are ready to jump into code you may wish to take a look at these resources:

    1. [Quickstart](quickstart.md): Itching to run some Daft code? Hit the ground running with our 10 minute quickstart notebook.

    2. [API Documentation](api/index.md): Searchable documentation and reference material to Daft’s public API.

### Get Started

<div class="grid cards" markdown>

- [:material-download: **Installing Daft**](install.md)

    Install Daft from your terminal and discover more advanced installation options.

- [:material-clock-fast: **Quickstart**](quickstart.md)

    Install Daft, create your first DataFrame, and get started with common DataFrame operations.

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

- [:material-window-closed-variant: **Window Functions**](core_concepts.md#window-functions)

    Daft's window functions allow you to perform calculations across a set of rows related to the current row.

- [:fontawesome-solid-user: **User-Defined Functions (UDFs)**](core_concepts.md#user-defined-functions-udf)

    Daft allows you to define custom UDFs to process data at scale with flexibility in input and output.

- [:octicons-image-16: **Multimodal Data**](core_concepts.md#multimodal-data)

    Daft is built to work with multimodal data types, including URLs and images.

- [:simple-ray: **Distributed Computing**](distributed.md)

    Daft's native support for Ray enables you to run distributed DataFrame workloads at scale.

</div>

### More Resources

<div class="grid cards" markdown>

- [:material-star-shooting: **Advanced Daft**](advanced/memory.md)
- [:material-file-compare: **Engine Comparison**](resources/engine_comparison.md)
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
