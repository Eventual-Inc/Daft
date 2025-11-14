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

### Inspect Your Data

Now let's take a look at what we loaded. You can inspect the DataFrame by simply printing it:

=== "🐍 Python"

    ```python
    df
    ```

```
╭────────┬────────┬────────────┬─────────────┬────────╮
│ url    ┆ name   ┆      …     ┆ description ┆ images │
│ ---    ┆ ---    ┆            ┆ ---         ┆ ---    │
│ String ┆ String ┆ (5 hidden) ┆ String      ┆ String │
╰────────┴────────┴────────────┴─────────────┴────────╯

(No data to display: Dataframe not materialized)
```

You see the above output because **Daft is lazy by default** - it displays the schema (column names and types) but doesn't actually load or process your data until you explicitly tell it to. This allows Daft to optimize your entire workflow before executing anything.

To actually view your data, you have two options:

**Option 1: Preview with `.show()`** - View the first few rows:

=== "🐍 Python"

    ```python
    df.show(2)
    ```

```
╭───────────────────┬───────────────────┬────────────┬───────────────────┬──────────────────╮
│ url               ┆ name              ┆      …     ┆ description       ┆ images           │
│ ---               ┆ ---               ┆            ┆ ---               ┆ ---              │
│ String            ┆ String            ┆ (5 hidden) ┆ String            ┆ String           │
╞═══════════════════╪═══════════════════╪════════════╪═══════════════════╪══════════════════╡
│ https://www.asos. ┆ New Look trench   ┆ …          ┆ [{'Product        ┆ ['https://images │
│ com/stradiva…     ┆ coat in camel     ┆            ┆ Details': 'Coats  ┆ .asos-media.c…   │
│                   ┆                   ┆            ┆ &…                ┆                  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://www.asos. ┆ New Look trench   ┆ …          ┆ [{'Product        ┆ ['https://images │
│ com/stradiva…     ┆ coat in camel     ┆            ┆ Details': 'Coats  ┆ .asos-media.c…   │
│                   ┆                   ┆            ┆ &…                ┆                  │
╰───────────────────┴───────────────────┴────────────┴───────────────────┴──────────────────╯

(Showing first 2 rows)
```

This materializes and displays just the first 2 rows, which is perfect for quickly inspecting your data without loading the entire dataset.

**Option 2: Materialize with `.collect()`** - Load the entire dataset:

=== "🐍 Python"

    ```python
    # df.collect()
    ```

This would materialize the entire DataFrame (all 30,845 rows in this case) into memory. Use `.collect()` when you need to work with the full dataset in memory.

### Working with a Smaller Dataset

For quick experimentation, let's create a smaller version of the dataframe:

=== "🐍 Python"

    ```python
    # Limit to just 5 rows for faster iteration
    df = df.limit(5)
    ```

Now we have a manageable dataset of 5 products that we can use to explore Daft's features without waiting for the entire dataset to process.

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
