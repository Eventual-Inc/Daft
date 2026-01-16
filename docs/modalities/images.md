# Working with Images


Daft is built to work comfortably with images. This guide shows you how to accomplish common image processing tasks with Daft:

- [Downloading and decoding images](#quickstart)
- [Generate image embeddings](#generate-image-embeddings)
- [Classify images](#classify-images)

It also explains some concepts on [Dynamic execution for multimodal workloads](#dynamic-execution-for-multimodal-workloads) to improve your mental model of how the Daft engine works.

## Quickstart

To setup this example, let's read a Parquet file from a public S3 bucket containing sample dog owners, use [`daft.col()`][daft.expressions.col] with the [`df.with_column`][daft.DataFrame.with_column] method to create a new column `full_name`, and join the contents from the `last_name` column to the `first_name` column. Then, let's create a `dogs` DataFrame from a Python dictionary and use [`df.join`][daft.DataFrame.join] to join this with our dataframe of owners:


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

You can use the [`download()`][daft.expressions.expressions.Expression.download] expression to download the bytes from a URL. Let's store them in a new column using the [`df.with_column()`][daft.DataFrame.with_column] method:

=== "🐍 Python"

    ```python
    df_family = df_family.with_column("image_bytes", col("urls").download(on_error="null"))
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

Let's turn the bytes into human-readable images using [`decode_image()`][daft.expressions.expressions.Expression.decode_image]:

=== "🐍 Python"

    ```python
    df_family = df_family.with_column("image", daft.col("image_bytes").decode_image())
    df_family.show()
    ```

## End-to-End Image Pipeline

This example demonstrates a complete pipeline: `URL -> download -> decode -> resize -> to_tensor -> normalize`.

This is a common preprocessing pipeline for preparing images for Deep Learning models (e.g., PyTorch).

=== "🐍 Python"

    ```python
    import daft
    from daft import col, DataType
    import numpy as np

    # 1. Create a DataFrame with image URLs
    df = daft.from_pydict({
        "urls": [
            "https://live.staticflickr.com/65535/53671838774_03ba68d203_o.jpg",
            "https://live.staticflickr.com/65535/53671700073_2c9441422e_o.jpg",
            "https://live.staticflickr.com/65535/53670606332_1ea5f2ce68_o.jpg",
            "https://live.staticflickr.com/65535/53671838039_b97411a441_o.jpg",
            "https://live.staticflickr.com/65535/53671698613_0230f8af3c_o.jpg",
        ],
    })

    # 2. Define a UDF for normalization (Standard ImageNet normalization)
    @daft.func(return_dtype=DataType.tensor(DataType.float32()))
    def normalize_image(img):
        if img is None:
            return None

        # Standard ImageNet normalization mean and std
        mean = np.array([0.485, 0.456, 0.406], dtype=np.float32)
        std = np.array([0.229, 0.224, 0.225], dtype=np.float32)

        # Convert to float32 and scale to [0, 1]
        # Input img is [H, W, C]
        img_float = img.astype(np.float32) / 255.0

        # Normalize
        # img_float is [H, W, C], mean/std are [3] broadcasting over the last dimension
        normalized = (img_float - mean) / std

        # Transpose to [C, H, W] for PyTorch models
        normalized = normalized.transpose(2, 0, 1)

        return normalized

    # 3. Build the pipeline: URL -> download -> decode -> resize -> to_tensor -> normalize
    df = df.with_column("image", col("urls").download(on_error="null").decode_image().resize(224, 224))
    df = df.with_column("tensor", col("image").image_to_tensor())
    df = df.with_column("normalized", normalize_image(col("tensor")))

    df.collect()
    df.select("urls", "image", "normalized").show()
    ```

```{title="Output"}
╭────────────────────────────────┬───────────────────────┬──────────────────────────────╮
│ urls                           ┆ image                 ┆ normalized                   │
│ ---                            ┆ ---                   ┆ ---                          │
│ String                         ┆ Image[RGB; 224 x 224] ┆ Tensor[Float32]              │
╞════════════════════════════════╪═══════════════════════╪══════════════════════════════╡
│ https://live.staticflickr.com… ┆ <FixedShapeImage>     ┆ <Tensor shape=(3, 224, 224)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://live.staticflickr.com… ┆ <FixedShapeImage>     ┆ <Tensor shape=(3, 224, 224)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://live.staticflickr.com… ┆ <FixedShapeImage>     ┆ <Tensor shape=(3, 224, 224)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://live.staticflickr.com… ┆ <FixedShapeImage>     ┆ <Tensor shape=(3, 224, 224)> │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ https://live.staticflickr.com… ┆ <FixedShapeImage>     ┆ <Tensor shape=(3, 224, 224)> │
╰────────────────────────────────┴───────────────────────┴──────────────────────────────╯

(Showing first 5 of 5 rows)
```

## UDF Best Practices for Images

When processing images with User-Defined Functions (UDFs) in Daft, using libraries like Pillow, OpenCV, or torchvision efficiently is key to performance and robustness.

### 1. Handling `None` Values

Daft data may contain `None` (null) values. Your UDF must handle these gracefully to avoid runtime errors.

=== "🐍 Python"

    ```python
    import daft
    from PIL import Image
    import io

    @daft.func(return_dtype=daft.DataType.binary())
    def process_image(image_bytes):
        # Always check for None!
        if image_bytes is None:
            return None

        try:
            img = Image.open(io.BytesIO(image_bytes))
            # ... processing ...

            # Serialize back to bytes for efficiency
            out = io.BytesIO()
            img.save(out, format=img.format or "PNG")
            return out.getvalue()
        except Exception:
            # Decide whether to return None or raise an error
            return None
    ```

### 2. Choosing the Right `return_dtype`

The `return_dtype` argument in `@daft.func` or `@daft.udf` is crucial. It tells Daft what kind of data to expect, allowing for optimizations and correct schema inference.

- **`daft.DataType.tensor(dtype)`**: Best for returning numerical data (numpy arrays, torch tensors). This allows Daft to treat the column as a native tensor type, enabling further vectorized operations.
- **`daft.DataType.binary()`**: Best for returning raw bytes (e.g. encoded PNG/JPEG data). This is often more memory efficient than full bitmaps, and avoids the pickling overhead associated with Python objects.
- **`daft.DataType.python()`**: Use this if you are returning arbitrary Python objects (like `PIL.Image` objects) that don't map neatly to a Daft type. **Note:** Python objects cannot be serialized as efficiently and may block some downstream optimizations.

### 3. Performance: `numpy` / `torch` vs `PIL.Image`

Returning native arrays (NumPy or PyTorch) is generally more performant than returning Python objects like `PIL.Image`, especially when `return_dtype` is set to a Tensor type.

#### Why?
- **Zero-copy / Low-overhead**: Daft can often manage memory for Arrow/Tensor types more efficiently.
- **Serialization**: `PIL.Image` objects are pickled/unpickled when moved between processes, which is slow. Tensors have efficient binary representations.

#### Example: Returning a Tensor (Recommended)

=== "🐍 Python"

    ```python
    import numpy as np

    @daft.func(return_dtype=daft.DataType.tensor(daft.DataType.uint8()))
    def image_to_numpy(image_bytes):
        if image_bytes is None:
            return None

        img = Image.open(io.BytesIO(image_bytes))
        # Convert to numpy array
        return np.array(img)
    ```

#### Example: Using torchvision

When using `torchvision`, operations typically return `torch.Tensor`. You can return these directly if you specify a Tensor return type.

=== "🐍 Python"

    ```python
    import torch
    import torchvision.transforms.functional as F
    import numpy as np

    @daft.func(return_dtype=daft.DataType.tensor(daft.DataType.float32()))
    def transform_image(image_tensor):
        if image_tensor is None:
            return None

        # Assuming input is already a tensor or numpy array
        if isinstance(image_tensor, np.ndarray):
            image_tensor = torch.from_numpy(image_tensor)

        # Ensure channel-first format (C, H, W) for torchvision
        if image_tensor.ndim == 3 and image_tensor.shape[-1] == 3:
            image_tensor = image_tensor.permute(2, 0, 1)

        # Apply torchvision transforms using F
        image_tensor = F.resize(image_tensor, [224, 224])

        return image_tensor
    ```

### 4. Batch Processing with `@daft.func.batch`

For even higher performance, especially with heavy libraries like OpenCV or PyTorch, consider using batched UDFs to process multiple rows at once, reducing Python function call overhead.

=== "🐍 Python"

    ```python
    @daft.func.batch(return_dtype=daft.DataType.tensor(daft.DataType.uint8()))
    def batch_process_images(series):
        # 'series' is a Daft Series object
        # Convert to list of inputs
        inputs = series.to_pylist()

        results = []
        for item in inputs:
            if item is None:
                results.append(None)
                continue
            # Process item...
            # results.append(processed_item)

        return results
    ```

## Generate Image Embeddings

Image embeddings convert images into numerical vectors that capture semantic meaning. Use them for semantic search, similarity calculations, etc.

### How to use the embed_image function

By default, `embed_image` uses the Transformers provider, which requires the `transformers` [optional dependency](../install.md). By default we also use OpenAI's CLIP model ([`openai/clip-vit-base-patch32`](https://huggingface.co/openai/clip-vit-base-patch32)).

```bash
pip install -U "daft[transformers]"
```

Once installed, we can run:

```python
import daft
from daft.functions.ai import embed_image

(
    daft.read_huggingface("xai-org/RealworldQA")
    .with_column("image", daft.col("image")["bytes"].decode_image())
    .with_column("embedding", embed_image(daft.col("image")))
    .show()
)
```


## Classify Images

We'll define a function that uses a pre-trained PyTorch model: [ResNet50](https://pytorch.org/vision/main/models/generated/torchvision.models.resnet50.html) to classify the dog pictures. We'll pass the contents of the image `urls` column and send the classification predictions to a new column `classify_breed`.

Working with PyTorch adds some complexity but you can just run the cells below to perform the classification.

First, make sure to install and import some extra dependencies:

```bash

pip install validators matplotlib Pillow torch torchvision

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
### Zero Shot Classification

For zero shot classification, you can use our built in `classify_image` function to classify images

=== "🐍 Python"

    ```python
    classify_images_expr = daft.functions.classify_image(
      daft.col("image"), labels=[
        "boxer",
        "schnauzer",
        "rottweiler",
        "staffordshire terrier",
        "collie",
        "chihuahua",
        "corgi"
      ]
    )
    classified_images_df = df_family.with_column("classify_breed", classify_images_expr)
    classified_images_df.select("dog_name", "image", "classify_breed").show()
    ```

```{title="Output"}
╭──────────┬──────────────┬───────────────────────╮
│ dog_name ┆ image        ┆ classify_breed        │
│ ---      ┆ ---          ┆ ---                   │
│ String   ┆ Image[MIXED] ┆ String                │
╞══════════╪══════════════╪═══════════════════════╡
│ Ernie    ┆ <Image>      ┆ boxer                 │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Jackie   ┆ <Image>      ┆ staffordshire terrier │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Wolfie   ┆ <Image>      ┆ collie                │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Shaggie  ┆ <Image>      ┆ schnauzer             │
├╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Zadie    ┆ <Image>      ┆ rottweiler            │
╰──────────┴──────────────┴───────────────────────╯

(Showing first 5 of 5 rows)
```


<!-- todo(docs - jay): Insert table of dog urls? or new UDF example? This was from the original 10-min quickstart with multimodal -->

## Dynamic Execution for Multimodal Workloads

Daft uses **dynamic execution** to automatically adjust batch sizes based on the operation type and data characteristics.

This is necessary because multimodal data such as images, videos, and audio files have different memory and processing characteristics that can cause issues with fixed batching: large batches may exceed available memory, while small batches may not fully utilize hardware optimizations or network bandwidth.

### How Batch Sizes Are Determined

**Multimodal Downloads:** Downloads for multimodal data use smaller batch sizes (typically a factor of the max_connections parameter) to prevent memory exhaustion when downloading large files, while maintaining network throughput.

**Vectorized Operations:** Operations that can operate on many rows in parallel, such as byte decoding / encoding, aggregations, and scalar projections, will use larger batch sizes that can take advantage of vectorized execution using SIMD.


=== "🐍 Python"
    ```python
    # Each operation uses different batch sizes automatically
    df = daft.read_parquet("metadata.parquet") # Large batches
          .with_column("image_data", col("urls").download())  # Small batches
          .with_column("resized", col("image_data").resize(224, 224))  # Medium batches
    ```

This approach allows processing of datasets larger than available memory, while maintaining optimal performance for each operation type.
