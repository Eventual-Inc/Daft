# Querying Image Data with Daft

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/image_querying/top_n_red_color.ipynb)

In this demo we will go through a simple example of using Daft to find the top N most red images out of the OpenImages dataset.

Let's install some dependencies.

```bash
pip install "daft[aws,ray]"
pip install Pillow numpy
```

```python
USE_RAY = False

###
# Settings for running on 10,000 rows in a distributed Ray cluster
###

if USE_RAY:
    import ray

    import daft.context

    # NOTE: Replace with the address to an existing running Ray cluster, or None to start a local Ray cluster
    RAY_CLUSTER_ADDRESS = "ray://localhost:10001"

    ray.init(
        address=RAY_CLUSTER_ADDRESS,
        runtime_env={"pip": ["daft", "pillow", "s3fs"]},
    )

    daft.context.set_runner_ray(address=RAY_CLUSTER_ADDRESS)
```

## Constructing our DataFrame

```python
import daft

IO_CONFIG = daft.io.IOConfig(s3=daft.io.S3Config(anonymous=True))  # Use anonymous S3 access

df = daft.from_glob_path(
    "s3://daft-public-data/open-images/validation-images/*",
    io_config=IO_CONFIG,
)

if USE_RAY:
    df = df.limit(10000)
    df = df.repartition(64)
else:
    df = df.limit(100)
```

## Filtering Data

```python
df = df.where(df["size"] < 300000)
```

**Daft is LAZY**: this filtering doesn't run immediately, but rather queues operations up in a query plan.

Now, let's define another filter with a lower bound for the image size:

```python
df = df.where(df["size"] > 200000)
```

If we look at the plan, there are now two enqueued `Filter` operations!

```python
df.explain()
```

Doing these two `Filter`s one after another is really inefficient since we have to pass through the data twice!

Don't worry though - Daft's query optimizer will actually optimize this at runtime and merge the two `Filter` operations into one. You can view the optimized plan with `show_all=True`:

```python
df.explain(show_all=True)
```

This is just one example of query optimization, and Daft does many other really important ones such as Predicate Pushdowns and Column Pruning to keep your execution plans running efficiently.

Now we can **materialize** the filtered dataframe like so:

```python
# Materializes the dataframe and shows first 10 rows

df.collect()
```

Note that calling `.collect()` **materializes** the data. It will execute the above plan and all the computed data will be materialized in memory as long as the `df` variable is valid. This means that any subsequent operation on `df` will read from this materialized data instead of computing the entire plan again.

Let's prune the columns to just the "path" column, which is the only one we need at the moment:

```python
df = df.select("path")

# Show doesn't materialize the data, but lets us peek at the first N rows
# produced by the current query plan

df.show(5)
```

## Working with Complex Data

Now let's do some data manipulation, starting with some simple ones (URLs) and finally images!

```python
df = df.with_column("image", df["path"].url.download(io_config=IO_CONFIG))

# Materialize the dataframe, so that we don't have to hit S3 again for subsequent operations
df.collect()
```

To decode the raw bytes we're downloading from the URL into an Image, we can simply use Daft's `.image.decode()` expression:

```python
df = df.with_column("image", df["image"].image.decode())
df.show(5)
```

Sometimes you may need to run your own custom Python function. We can do this with the `.apply` Daft expression.

Let's run a custom `magic_red_detector` function, which will detect red pixels in our images and present us with a mask!

```python
import numpy as np
import PIL.Image
from PIL import ImageFilter


def magic_red_detector(img: np.ndarray) -> PIL.Image.Image:
    """Gets a new image which is a mask covering all 'red' areas in the image."""
    img = PIL.Image.fromarray(img)
    lower = np.array([245, 100, 100])
    upper = np.array([10, 255, 255])
    lower_hue, upper_hue = lower[0, np.newaxis, np.newaxis], upper[0, np.newaxis, np.newaxis]
    lower_saturation_intensity, upper_saturation_intensity = (
        lower[1:, np.newaxis, np.newaxis],
        upper[1:, np.newaxis, np.newaxis],
    )
    hsv = img.convert("HSV")
    hsv = np.asarray(hsv).T
    mask = np.all(
        (hsv[1:, ...] >= lower_saturation_intensity) & (hsv[1:, ...] <= upper_saturation_intensity), axis=0
    ) & ((hsv[0, ...] >= lower_hue) | (hsv[0, ...] <= upper_hue))
    img = PIL.Image.fromarray(mask.T)
    img = img.filter(ImageFilter.ModeFilter(size=5))
    return img


df = df.with_column(
    "red_mask",
    df["image"].apply(magic_red_detector, return_dtype=daft.DataType.python()),
)

df.collect()
```

Now to determine how "red" an image is, we can simply sum up the number of pixels in the `red_mask` column!

```python
import numpy as np

def sum_mask(mask: PIL.Image.Image) -> int:
    val = np.asarray(mask).sum()
    return int(val)


df = df.with_column(
    "num_pixels_red",
    df["red_mask"].apply(sum_mask, return_dtype=daft.DataType.int64()),
)

df.sort("num_pixels_red", desc=True).collect()
```
