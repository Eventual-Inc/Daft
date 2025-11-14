# Quickstart

<!--
todo(docs - jay): Incorporate SQL examples

todo(docs): Add link to notebook to DIY (notebook is in mkdocs dir, but idk how to host on colab)

todo(docs): What does the actual output look like for some of these examples? should we update it visually?
-->

In this quickstart, you will learn the basics of Daft's DataFrame and SQL API and the features that set it apart from frameworks like Pandas, PySpark, Dask, and Ray.

<!-- You will build a database of dog owners and their fluffy companions and see how you can use Daft to download images from URLs, run an ML classifier and call custom UDFs, all within an interactive DataFrame interface. Woof! 🐶 -->

### Install Daft

You can install Daft using `pip`. Run the following command in your terminal or notebook:

=== "🐍 Python"

    ```python
    pip install daft
    ```

<!-- For more advanced installation options, please see [Installation](install.md). -->

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
