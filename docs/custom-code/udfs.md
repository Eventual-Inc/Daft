# User-Defined Functions (UDFs)

A key piece of functionality in Daft is the ability to flexibly define custom functions that can run computations on any data in your dataframe. This section walks you through the different types of UDFs that Daft allows you to run.

Let's first create a dataframe that will be used as a running example throughout this tutorial!

=== "🐍 Python"
    ``` python
    import daft
    import numpy as np

    df = daft.from_pydict({
        # the `image` column contains images represented as 2D numpy arrays
        "image": [np.ones((128, 128)) for i in range(16)],
        # the `crop` column contains a box to crop from our image, represented as a list of integers: [x1, x2, y1, y2]
        "crop": [[0, 1, 0, 1] for i in range(16)],
    })
    ```

### Per-column per-row functions using [`.apply()`][daft.expressions.Expression.apply]

You can use [`.apply()`][daft.expressions.Expression.apply] to run a Python function on every row in a column.

For example, the following example creates a new `flattened_image` column by calling `.flatten()` on every object in the `image` column.

=== "🐍 Python"
    ``` python
    df.with_column(
        "flattened_image",
        df["image"].apply(lambda img: img.flatten(), return_dtype=daft.DataType.python())
    ).show(2)
    ```

``` {title="Output"}
╭───────────────────────────┬──────────────┬─────────────────────────╮
│ image                     ┆ crop         ┆ flattened_image         │
│ ---                       ┆ ---          ┆ ---                     │
│ Tensor(Float64)           ┆ List[Int64]  ┆ Python                  │
╞═══════════════════════════╪══════════════╪═════════════════════════╡
│ <Tensor shape=(128, 128)> ┆ [0, 1, 0, 1] ┆ [1. 1. 1. ... 1. 1. 1.] │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ <Tensor shape=(128, 128)> ┆ [0, 1, 0, 1] ┆ [1. 1. 1. ... 1. 1. 1.] │
╰───────────────────────────┴──────────────┴─────────────────────────╯

(Showing first 2 rows)
```

Note here that we use the `return_dtype` keyword argument to specify that our returned column type is a Python column!

### Multi-column per-partition functions using [`@udf`](../api/udf.md#creating-udfs)

[`.apply()`][daft.expressions.Expression.apply] is great for convenience, but has two main limitations:

1. It can only run on single columns
2. It can only run on single items at a time

Daft provides the [`@udf`](../api/udf.md#creating-udfs) decorator for defining your own UDFs that process multiple columns or multiple rows at a time.

For example, let's try writing a function that will crop all our images in the `image` column by its corresponding value in the `crop` column:

=== "🐍 Python"
    ``` python
    @daft.udf(return_dtype=daft.DataType.python())
    def crop_images(images, crops, padding=0):
        cropped = []
        for img, crop in zip(images, crops):
            x1, x2, y1, y2 = crop
            cropped_img = img[x1:x2 + padding, y1:y2 + padding]
            cropped.append(cropped_img)
        return cropped

    df = df.with_column(
        "cropped",
        crop_images(df["image"], df["crop"], padding=1),
    )
    df.show(2)
    ```

``` {title="Output"}

╭───────────────────────────┬──────────────┬───────────╮
│ image                     ┆ crop         ┆ cropped   │
│ ---                       ┆ ---          ┆ ---       │
│ Tensor(Float64)           ┆ List[Int64]  ┆ Python    │
╞═══════════════════════════╪══════════════╪═══════════╡
│ <Tensor shape=(128, 128)> ┆ [0, 1, 0, 1] ┆ [[1. 1.]  │
│                           ┆              ┆  [1. 1.]] │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ <Tensor shape=(128, 128)> ┆ [0, 1, 0, 1] ┆ [[1. 1.]  │
│                           ┆              ┆  [1. 1.]] │
╰───────────────────────────┴──────────────┴───────────╯

(Showing first 2 rows)
```

There's a few things happening here, let's break it down:

1. `crop_images` is a normal Python function. It takes as input:

    a. A list of images: `images`

    b. A list of cropping boxes: `crops`

    c. An integer indicating how much padding to apply to the right and bottom of the cropping: `padding`

2. To allow Daft to pass column data into the `images` and `crops` arguments, we decorate the function with [`@udf`](../api/udf.md#creating-udfs)

    a. `return_dtype` defines the returned data type. In this case, we return a column containing Python objects of numpy arrays

    b. At runtime, because we call the UDF on the `image` and `crop` columns, the UDF will receive a [`daft.Series`][daft.series.Series] object for each argument.

3. We can create a new column in our DataFrame by applying our UDF on the `"image"` and `"crop"` columns inside of a [`df.with_column()`][daft.DataFrame.with_column] call.

#### UDF Inputs

When you specify an Expression as an input to a UDF, Daft will calculate the result of that Expression and pass it into your function as a [`daft.Series`][daft.series.Series] object.

The Daft [`daft.Series`][daft.series.Series] is just an abstraction on a "column" of data! You can obtain several different data representations from a [`daft.Series`][daft.series.Series]:

1. PyArrow Arrays (`pa.Array`): [`s.to_arrow()`][daft.series.Series.to_arrow]
2. Python lists (`list`): [`s.to_pylist()`][daft.series.Series.to_pylist]

Depending on your application, you may choose a different data representation that is more performant or more convenient!

!!! info "Info"

    Certain array formats have some restrictions around the type of data that they can handle:

    1. **Null Handling**: In Pandas and Numpy, nulls are represented as NaNs for numeric types, and Nones for non-numeric types. Additionally, the existence of nulls will trigger a type casting from integer to float arrays. If null handling is important to your use-case, we recommend using one of the other available options.

    2. **Python Objects**: PyArrow array formats cannot support Python columns.

    We recommend using Python lists if performance is not a major consideration, and using the arrow-native formats such as PyArrow arrays and numpy arrays if performance is important.

#### Return Types

The `return_dtype` argument specifies what type of column your UDF will return. Types can be specified using the [`daft.DataType`][daft.datatype.DataType] class.

Your UDF function itself needs to return a batch of columnar data, and can do so as any one of the following array types:

1. Numpy Arrays (`np.ndarray`)
2. PyArrow Arrays (`pa.Array`)
3. Python lists (`list`)

Note that if the data you have returned is not castable to the return_dtype that you specify (e.g. if you return a list of floats when you've specified a `return_dtype=DataType.bool()`), Daft will throw a runtime error!

### Class UDFs

UDFs can also be created on Python classes. With a class UDF, you can set the `concurrency` argument to share the initialization on expensive state between invocations of the UDF, for example downloading data or creating a model.

=== "🐍 Python"
    ``` python
    @daft.udf(
        return_dtype=daft.DataType.int64(),
        concurrency=4,  # initialize 4 instances of the UDF to run across your data
    )
    class RunModel:

        def __init__(self, model_name = "meta-llama/Llama-4-Scout-17B-16E-Instruct"):
            # Perform expensive initializations
            self._model = create_model(model_name)

        def __call__(self, features_col):
            return self._model(features_col)
    ```

Class UDFs can be used the exact same way as function UDFs:

=== "🐍 Python"
    ``` python
    df = df.with_column("image_classifications", RunModel(df["image"]))
    ```

In addition, you can pass in arguments to the `__init__` method using `.with_init_args`:

=== "🐍 Python"
    ``` python
    RunMistral = RunModel.with_init_args(model_name="mistralai/Mistral-7B-Instruct-v0.1")
    df = df.with_column("image_classifications", RunMistral(df["image"]))
    ```

See the `TextEmbedder` UDF in our [document processing tutorial](../resources/tutorials.md#document-processing) for a complete example of using a class UDF with concurrency.

### Resource Requests

Sometimes, you may want to request for specific resources for your UDF. For example, some UDFs need one GPU to run as they will load a model onto the GPU.

To do so, you can create your UDF and assign it a resource request:

=== "🐍 Python"
    ``` python
    @daft.udf(return_dtype=daft.DataType.int64(), num_gpus=1)
    class RunModelWithOneGPU:

        def __init__(self):
            # Perform expensive initializations
            self._model = create_model()

        def __call__(self, features_col):
            return self._model(features_col)
    ```

    ``` python
    df = df.with_column(
        "image_classifications",
        RunModelWithOneGPU(df["image"]),
    )
    ```

In the above example, if Daft ran on a Ray cluster consisting of 8 GPUs and 64 CPUs, Daft would be able to run 8 replicas of your UDF in parallel, thus massively increasing the throughput of your UDF!

UDFs can also be parametrized with new resource requests after being initialized.

=== "🐍 Python"
    ``` python
    RunModelWithTwoGPUs = RunModelWithOneGPU.override_options(num_gpus=2)
    df = df.with_column(
        "image_classifications",
        RunModelWithTwoGPUs(df["image"]),
    )
    ```

### Debugging UDFs

When running Daft locally, UDFs can be debugged using python's built-in debugger, [pdb](https://docs.python.org/3/library/pdb.html), by setting breakpoints in your UDF.

=== "🐍 Python"
    ``` python
    @daft.udf(return_dtype=daft.DataType.python())
    def my_udf(*cols):
        breakpoint()
        ...
    ```

If you are setting breakpoints via IDEs like VS Code, Cursor, or others that use [debugpy](https://github.com/microsoft/debugpy), you need to set `debugpy.debug_this_thread()` in the UDF. This is because `debugpy` does not automatically detect native threads.

=== "🐍 Python"
    ``` python
    import debugpy

    @daft.udf(return_dtype=daft.DataType.python())
    def my_udf(*cols):
        debugpy.debug_this_thread()
        ...
    ```
