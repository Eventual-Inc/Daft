User-Defined Functions (UDF)
============================

A key piece of functionality in DaFt is the ability to flexibly define custom functions that can run on any data in your dataframe. This guide walks you through the different types of UDFs that DaFt allows you to run.

Let's first create a dataframe that will be used as a running example throughout this tutorial!

.. code:: python

    from daft import DataFrame

    df = DataFrame.from_pydict({
        # the `image` column contains images represented as 2D numpy arrays
        "image": [np.ones((128, 128)) for i in range(16)],
        # the `crop` column contains a box to crop from our image, represented as a list of integers: [x1, x2, y1, y2]
        "crop": [[0, 1, 0, 1] for i in range(16)],
    })

Per-column per-row functions using ``Expression.apply``
-------------------------------------------------------

You can use ``Expression.apply`` to run a Python function on every row in a column.

For example, the following example creates a new ``"flattened_image"`` column by calling ``.flatten()`` on every object in the ``"image"`` column.

.. code:: python

    df.with_column(
        "flattened_image",
        df["image"].apply(lambda img: img.flatten(), return_dtype=np.ndarray)
    )

Note here that we use the ``return_type`` keyword argument to specify that our returned column type is of type ``np.ndarray``, so the new ``"flattened_image"``
column will have type ``PY[np.ndarray]``!

Multi-column per-partition functions using ``@udf``
---------------------------------------------------

``Expression.apply`` is great for convenience, but has two main limitations:

1. It can only run on single columns
2. It can only run on single items at a time

Daft provides the ``@udf`` decorator for defining your own UDFs that process multiple columns or multiple rows at a time.

For example, let's try writing a function that will crop all our images in the ``"image"`` column by its corresponding value in the ``"crop"`` column:

.. code:: python

    from daft import udf

    @udf(return_dtype=np.ndarray, input_columns={"images": list, "crops": list})
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

There's a few things happening here, let's break it down:

1. ``crop_images`` is a normal Python function. It takes as input:
    a. A list of images: ``images``
    b. A list of cropping boxes: ``crops``
    c. An integer indicating how much padding to apply to the right and bottom of the cropping: ``padding``
2. To allow Daft to pass column data into the ``images`` and ``crops`` arguments, we decorate the function with ``@udf``
    a. ``return_dtype`` defines the returned data type. In this case, we return a column containing numpy arrays (``np.ndarray``)
    b. The keys in ``input_columns`` defines which input parameters should be columns (in this case ``images`` and ``crops``)
    c. The values in ``input_columns`` defines what container type Daft should pass columns in as, in this case just a ``list``.
3. We can create a new column in our DataFrame by applying our UDF on the ``"image"`` and ``"crop"`` columns inside of a ``df.with_column`` call.

Input Types
^^^^^^^^^^^

In the above example, we indicated that the ``images`` and ``crops`` input parameters should be passed in as a ``list``. Daft supports other
array container types as well:

1. Numpy Arrays (``np.ndarray``)
2. Pandas Series (``pd.Series``)
3. Polars Series (``polars.Series``)
4. PyArrow Arrays (``pa.Array``) or (``pa.ChunkedArray``)
5. Python lists (``list`` or ``typing.List``)

.. NOTE::
    Certain array formats have some restrictions around the type of data that they can handle:

    1. **Null Handling**: In Pandas and Numpy, nulls are represented as NaNs for numeric types, and Nones for non-numeric types.
    Additionally, the existence of nulls will trigger a type casting from integer to float arrays. If null handling is important to
    your use-case, we recommend using one of the other available options.
    2. **Python Objects**: PyArrow array formats cannot support object-type columns.

    We recommend using Python lists if performance is not a major consideration, and using the arrow-native formats such as
    PyArrow arrays and Polars series if performance is important.

Return Types
^^^^^^^^^^^^

The ``return_dtype`` argument specifies what type of column your UDF will return. For user convenience, you may specify a Python
type such as ``str``, ``int``, ``float``, ``bool`` and ``datetime.date``, which will be converted into a Daft dtype for you.

Python types that are not recognized as Daft types will be represented as a Daft Python object dtype. For example, if you specify
``return_dtype=np.ndarray``, then your returned column will have type ``PY[np.ndarray]``.

Your UDF function itself needs to return a batch of columnar data, and can do so as any one of the following array types:

1. Numpy Arrays (``np.ndarray``)
2. Pandas Series (``pd.Series``)
3. Polars Series (``polars.Series``)
4. PyArrow Arrays (``pa.Array``) or (``pa.ChunkedArray``)
5. Python lists (``list`` or ``typing.List``)

Stateful UDFs
-------------

UDFs can also be created on Classes, which allow for initialization on some expensive state that can be shared
between invocations of the class, for example downloading data or creating a model.

.. code:: python

    @udf(return_dtype=int, input_columns={"features_col": np.ndarray})
    class RunModel:

        def __init__(self):
            # Perform expensive initializations
            self._model = create_model()

        def __call__(self, features_col):
            return self._model(features_col)

Running Stateful UDFs are exactly the same as running their Stateless cousins.

.. code:: python

    df = df.with_column("image_classifications", RunModel(df["images"]))


Resource Requests
-----------------

Sometimes, you may want to request for specific resources for your UDF. For example, some UDFs need one GPU to run as they will load a model onto the GPU.

Custom resources can be requested when you call ``.with_column``:

.. code:: python

    from daft.resource_request import ResourceRequest

    # Runs the UDF `func` with the specified resource requests
    df = df.with_column(
        "image_classifications",
        RunModel(df["images"]),
        resource_request=ResourceRequest(num_gpus=1, num_cpus=8),
    )

In the above example, if ran Daft on a Ray cluster consisting of 8 GPUs and 64 CPUs, Daft would be able to run 8 replicas of your UDF in parallel, thus massively increasing the throughput of your UDF!
