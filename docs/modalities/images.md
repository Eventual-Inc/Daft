# Working with Images

Daft is built to work comfortably with images.

To setup this example, let's read a Parquet file from a public S3 bucket containing sample dog owners, use the [`daft.col()`][daft.expressions.col] mentioned earlier with the [`df.with_column`][daft.DataFrame.with_column] method to create a new column `full_name`, and join the contents from the `last_name` column to the `first_name` column. Then, let's create a `dogs` DataFrame from a Python dictionary and use [`df.join`][daft.DataFrame.join] to join this with our dataframe of owners:


=== "🐍 Python"

    ```python
    import daft
    from daft import col

    # Read parquet file containing sample dog owners
    df = daft.read_parquet("s3://daft-public-data/tutorials/10-min/sample-data-dog-owners-partitioned.pq/**")

    # Combine "first_name" and "last_name" to create new column "full_name"
    df = df.with_column("full_name", col("first_name") + " " + col("last_name"))
    df.select("full_name", "age", "country", "has_dog").show()

    # Create dataframe of dogs
    df_dogs = daft.from_pydict(
        {
            "urls": [
                "https://live.staticflickr.com/65535/53671838774_03ba68d203_o.jpg",
                "https://live.staticflickr.com/65535/53671700073_2c9441422e_o.jpg",
                "https://live.staticflickr.com/65535/53670606332_1ea5f2ce68_o.jpg",
                "https://live.staticflickr.com/65535/53671838039_b97411a441_o.jpg",
                "https://live.staticflickr.com/65535/53671698613_0230f8af3c_o.jpg",
            ],
            "full_name": [
                "Ernesto Evergreen",
                "James Jale",
                "Wolfgang Winter",
                "Shandra Shamas",
                "Zaya Zaphora",
            ],
            "dog_name": ["Ernie", "Jackie", "Wolfie", "Shaggie", "Zadie"],
        }
    )

    # Join owners with dogs, dropping some columns
    df_family = df.join(df_dogs, on="full_name").exclude("first_name", "last_name", "DoB", "country", "age")
    df_family.show()
    ```

```{title="Output"}
╭───────────────────┬─────────┬────────────────────────────────┬──────────╮
│ full_name         ┆ has_dog ┆ urls                           ┆ dog_name │
│ ---               ┆ ---     ┆ ---                            ┆ ---      │
│ Utf8              ┆ Boolean ┆ Utf8                           ┆ Utf8     │
╞═══════════════════╪═════════╪════════════════════════════════╪══════════╡
│ Wolfgang Winter   ┆ None    ┆ https://live.staticflickr.com… ┆ Wolfie   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
│ Shandra Shamas    ┆ true    ┆ https://live.staticflickr.com… ┆ Shaggie  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
│ Zaya Zaphora      ┆ true    ┆ https://live.staticflickr.com… ┆ Zadie    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
│ Ernesto Evergreen ┆ true    ┆ https://live.staticflickr.com… ┆ Ernie    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┤
│ James Jale        ┆ true    ┆ https://live.staticflickr.com… ┆ Jackie   │
╰───────────────────┴─────────┴────────────────────────────────┴──────────╯

(Showing first 5 of 5 rows)
```

You can use the [`url.download()`][daft.expressions.expressions.ExpressionUrlNamespace.download] expression to download the bytes from a URL. Let's store them in a new column using the [`df.with_column()`][daft.DataFrame.with_column] method:

<!-- todo(docs - cc): add relative path to url.download after figure out url namespace-->

=== "🐍 Python"

    ```python
    df_family = df_family.with_column("image_bytes", col("urls").url.download(on_error="null"))
    df_family.show()
    ```

```{title="Output"}
╭───────────────────┬─────────┬────────────────────────────────┬──────────┬────────────────────────────────╮
│ full_name         ┆ has_dog ┆ urls                           ┆ dog_name ┆ image_bytes                    │
│ ---               ┆ ---     ┆ ---                            ┆ ---      ┆ ---                            │
│ Utf8              ┆ Boolean ┆ Utf8                           ┆ Utf8     ┆ Binary                         │
╞═══════════════════╪═════════╪════════════════════════════════╪══════════╪════════════════════════════════╡
│ Wolfgang Winter   ┆ None    ┆ https://live.staticflickr.com… ┆ Wolfie   ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Shandra Shamas    ┆ true    ┆ https://live.staticflickr.com… ┆ Shaggie  ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Zaya Zaphora      ┆ true    ┆ https://live.staticflickr.com… ┆ Zadie    ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Ernesto Evergreen ┆ true    ┆ https://live.staticflickr.com… ┆ Ernie    ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ James Jale        ┆ true    ┆ https://live.staticflickr.com… ┆ Jackie   ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… │
╰───────────────────┴─────────┴────────────────────────────────┴──────────┴────────────────────────────────╯

(Showing first 5 of 5 rows)
```

Let's turn the bytes into human-readable images using [`image.decode()`][daft.expressions.expressions.ExpressionImageNamespace.decode]:

=== "🐍 Python"

    ```python
    df_family = df_family.with_column("image", daft.col("image_bytes").image.decode())
    df_family.show()
    ```

### Dynamic Execution for Multimodal Workloads

Daft uses **dynamic execution** to automatically adjust batch sizes based on the operation type and data characteristics.

This is necessary because multimodal data such as images, videos, and audio files have different memory and processing characteristics that can cause issues with fixed batching: large batches may exceed available memory, while small batches may not fully utilize hardware optimizations or network bandwidth.

#### How Batch Sizes Are Determined

**Multimodal Downloads:** Downloads for multimodal data use smaller batch sizes (typically a factor of the max_connections parameter) to prevent memory exhaustion when downloading large files, while maintaining network throughput.

**Vectorized Operations:** Operations that can operate on many rows in parallel, such as byte decoding / encoding, aggregations, and scalar projections, will use larger batch sizes that can take advantage of vectorized execution using SIMD.


=== "🐍 Python"
    ```python
    # Each operation uses different batch sizes automatically
    df = daft.read_parquet("metadata.parquet") # Large batches
          .with_column("image_data", col("urls").url.download())  # Small batches
          .with_column("resized", col("image_data").image.resize(224, 224))  # Medium batches
    ```

This approach allows processing of datasets larger than available memory, while maintaining optimal performance for each operation type.

## Example: UDFs in ML + Multimodal Workload

We'll define a function that uses a pre-trained PyTorch model: [ResNet50](https://pytorch.org/vision/main/models/generated/torchvision.models.resnet50.html) to classify the dog pictures. We'll pass the contents of the image `urls` column and send the classification predictions to a new column `classify_breed`.

Working with PyTorch adds some complexity but you can just run the cells below to perform the classification.

First, make sure to install and import some extra dependencies:

```bash

%pip install validators matplotlib Pillow torch torchvision

```

=== "🐍 Python"

    ```python
    # import additional libraries, these are necessary for PyTorch
    import torch
    ```

Define your `ClassifyImages` UDF. Models are expensive to initialize and load, so we want to do this as few times as possible, and share a model across multiple invocations.

=== "🐍 Python"

    ```python
    @daft.udf(return_dtype=daft.DataType.fixed_size_list(dtype=daft.DataType.string(), size=2))
    class ClassifyImages:
        def __init__(self):
            # Perform expensive initializations - create and load the pre-trained model
            self.model = torch.hub.load("NVIDIA/DeepLearningExamples:torchhub", "nvidia_resnet50", pretrained=True)
            self.utils = torch.hub.load("NVIDIA/DeepLearningExamples:torchhub", "nvidia_convnets_processing_utils")
            self.model.eval().to(torch.device("cpu"))

        def __call__(self, images_urls):
            batch = torch.cat([self.utils.prepare_input_from_uri(uri) for uri in images_urls]).to(torch.device("cpu"))

            with torch.no_grad():
                output = torch.nn.functional.softmax(self.model(batch), dim=1)

            results = self.utils.pick_n_best(predictions=output, n=1)
            return [result[0] for result in results]
    ```

Now you're ready to call this function on the `urls` column and store the outputs in a new column we'll call `classify_breed`:

=== "🐍 Python"

    ```python
    classified_images_df = df_family.with_column("classify_breed", ClassifyImages(daft.col("urls")))
    classified_images_df.select("dog_name", "image", "classify_breed").show()
    ```

```{title="Output"}
╭──────────┬──────────────┬────────────────────────────────╮
│ dog_name ┆ image        ┆ classify_breed                 │
│ ---      ┆ ---          ┆ ---                            │
│ Utf8     ┆ Image[MIXED] ┆ FixedSizeList[Utf8; 2]         │
╞══════════╪══════════════╪════════════════════════════════╡
│ Ernie    ┆ <Image>      ┆ [boxer, 52.3%]                 │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Jackie   ┆ <Image>      ┆ [American Staffordshire terri… │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Shaggie  ┆ <Image>      ┆ [standard schnauzer, 29.6%]    │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Zadie    ┆ <Image>      ┆ [Rottweiler, 78.6%]            │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Wolfie   ┆ <Image>      ┆ [collie, 49.6%]                │
╰──────────┴──────────────┴────────────────────────────────╯

(Showing first 5 of 5 rows)
```

!!! note "Note"

    Execute in notebook to see properly rendered images.

<!-- todo(docs - jay): Insert table of dog urls? or new UDF example? This was from the original 10-min quickstart with multimodal -->
