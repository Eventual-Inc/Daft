# Quickstart

<!--
todo(docs - jay): Incorporate SQL examples

todo(docs): Add link to notebook to DIY (notebook is in mkdocs dir, but idk how to host on colab)

todo(docs): What does the actual output look like for some of these examples? should we update it visually?
-->

Daft is the best multimodal data processing engine that allows you to load data from anywhere, transform it with a powerful DataFrame API and AI functions, and store it in your destination of choice. In this quickstart, you'll see what this looks like in practice with a realistic e-commerce data workflow.

### Install Daft

You can install Daft using `pip`. Run the following command in your terminal or notebook:

=== "🐍 Python"

    ```python
    pip install daft
    ```

<!-- For more advanced installation options, please see [Installation](install.md). -->

### Load Your Data

Let's start by loading an e-commerce dataset from Hugging Face. [This dataset](https://huggingface.co/datasets/UniqueData/asos-e-commerce-dataset) contains over 30,000 products from ASOS, including product names, prices, descriptions, and images.

=== "🐍 Python"

    ```python
    import daft

    df = daft.read_huggingface("UniqueData/asos-e-commerce-dataset")
    ```

!!! note "Load from anywhere"

    Daft can load data from many sources including [S3](connectors/aws.md), [Iceberg](connectors/iceberg.md), [Delta Lake](connectors/delta_lake.md), [Hudi](connectors/hudi.md), and [more](connectors/index.md). We're using Hugging Face here as a demonstration.

### What's Next?

Now that you have a basic sense of Daft's functionality and features, here are some more resources to help you get the most out of Daft:

!!! tip "Try this on Kubernetes"

    Want to run this example on Kubernetes? Check out our [Kubernetes quickstart](distributed/kubernetes.md).

**Work with your favorite table and catalog formats**:

<div class="grid cards" markdown>

- [**Apache Hudi**](connectors/hudi.md)
- [**Apache Iceberg**](connectors/iceberg.md)
- [**AWS Glue**](connectors/glue.md)
- [**AWS S3Tables**](connectors/s3tables.md)
- [**Delta Lake**](connectors/delta_lake.md)
- [**Hugging Face Datasets**](connectors/huggingface.md)
- [**Unity Catalog**](connectors/unity_catalog.md)
<!-- - [**LanceDB**](io/lancedb.md) -->

</div>

<!-- **Coming from?**

<div class="grid cards" markdown>

- [:simple-dask: **Dask Migration Guide**](migration/dask_migration.md)

</div> -->

**Explore our [Examples](examples/index.md) to see Daft in action:**

<div class="grid cards" markdown>

- [:material-image-edit: **MNIST Digit Classification**](examples/mnist.md)
- [:octicons-search-16: **Running LLMs on the Red Pajamas Dataset**](examples/llms-red-pajamas.md)
- [:material-image-search: **Querying Images with UDFs**](examples/querying-images.md)
- [:material-image-sync: **Image Generation on GPUs**](examples/image-generation.md)
- [:material-window-closed-variant: **Window Functions in Daft**](examples/window-functions.md)

</div>
