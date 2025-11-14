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

Let's start by loading an e-commerce dataset from Hugging Face. [This dataset](https://huggingface.co/datasets/calmgoose/amazon-product-data-2020) contains 10,000 Amazon products from diverse categories including electronics, toys, home goods, and more. Each product includes details like names, prices, descriptions, technical specifications, and product images.

=== "🐍 Python"

    ```python
    import daft

    df = daft.read_huggingface("calmgoose/amazon-product-data-2020")
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
╭─────────┬──────────────┬──────────┬────────────┬──────────┬─────────────┬──────────────────╮
│ Uniq Id ┆ Product Name ┆ Category ┆      …     ┆ Variants ┆ Product Url ┆ Is Amazon Seller │
│ ---     ┆ ---          ┆ ---      ┆            ┆ ---      ┆ ---         ┆ ---              │
│ String  ┆ String       ┆ String   ┆ (9 hidden) ┆ String   ┆ String      ┆ String           │
╰─────────┴──────────────┴──────────┴────────────┴──────────┴─────────────┴──────────────────╯

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
╭──────────────────┬──────────────────┬──────────────────┬────────────┬──────────────────┬─────────────────┬───────────╮
│ Uniq Id          ┆ Product Name     ┆ Category         ┆      …     ┆ Variants         ┆ Product Url     ┆ Is Amazon │
│ ---              ┆ ---              ┆ ---              ┆            ┆ ---              ┆ ---             ┆ Seller    │
│ String           ┆ String           ┆ String           ┆ (9 hidden) ┆ String           ┆ String          ┆ ---       │
│                  ┆                  ┆                  ┆            ┆                  ┆                 ┆ String    │
╞══════════════════╪══════════════════╪══════════════════╪════════════╪══════════════════╪═════════════════╪═══════════╡
│ 4c69b61db1fc16e7 ┆ DB Longboards    ┆ Sports &         ┆ …          ┆ https://www.amaz ┆ https://www.ama ┆ Y         │
│ 013b43fc926e5…   ┆ CoreFlex Crossb… ┆ Outdoors |       ┆            ┆ on.com/DB-Lon…   ┆ zon.com/DB-Lon… ┆           │
│                  ┆                  ┆ Outdoor R…       ┆            ┆                  ┆                 ┆           │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ 66d49bbed043f5be ┆ Electronic Snap  ┆ Toys & Games |   ┆ …          ┆ None             ┆ https://www.ama ┆ Y         │
│ 260fa9f7fbff5…   ┆ Circuits Mini…   ┆ Learning & Edu…  ┆            ┆                  ┆ zon.com/Electr… ┆           │
╰──────────────────┴──────────────────┴──────────────────┴────────────┴──────────────────┴─────────────────┴───────────╯

(Showing first 2 rows)
```

This materializes and displays just the first 2 rows, which is perfect for quickly inspecting your data without loading the entire dataset.

**Option 2: Materialize with `.collect()`** - Load the entire dataset:

=== "🐍 Python"

    ```python
    # df.collect()
    ```

This would materialize the entire DataFrame (all 10,000 rows in this case) into memory. Use `.collect()` when you need to work with the full dataset in memory.

### Working with a Smaller Dataset

For quick experimentation, let's create a smaller, simplified version of the dataframe with just the essential columns:

=== "🐍 Python"

    ```python
    # Select only the columns we need and limit to 5 rows for faster iteration
    df = df.select("Product Name", "About Product", "Image").limit(5)
    ```

Now we have a manageable dataset of 5 products with just the product name, description, and image URLs. This simplified dataset lets us explore Daft's features without the overhead of unnecessary columns.

### Downloading Images

Let's extract and download product images. The `Image` column contains pipe-separated URLs. We'll extract the first URL and download it:

=== "🐍 Python"

    ```python
    # Extract the first image URL from the pipe-separated list
    # The pattern captures everything before the first pipe or the entire string if no pipe
    df = df.with_column(
        "first_image_url",
        daft.functions.regexp_extract(
            df["Image"],
            r"^([^|]+)",  # Extract everything before the first pipe
            1  # Get the first capture group
        )
    )

    # Download the image data
    df = df.with_column(
        "image_data",
        df["first_image_url"].url.download(on_error="null")
    )

    # Check what we have
    df.select("Product Name", "first_image_url").show(3)
    ```

This demonstrates Daft's multimodal capabilities:
- **Pattern extraction**: Use `regexp_extract()` to parse structured text
- **URL handling**: Download content directly with `.url.download()`
- **Error handling**: Use `on_error="null"` to gracefully handle failed downloads

The downloaded image data is now ready for further processing, such as running image classification models, extracting embeddings, or performing transformations.

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
