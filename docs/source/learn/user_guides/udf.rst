User-Defined Functions (UDF)
============================

A key piece of functionality in DaFt is the ability to flexibly define custom functions that can run on any data in your dataframe. This guide walks you through the different types of UDFs that DaFt allows you to run.

Inline UDFs
-----------

For the simplest use-cases, Daft allows users to run a function on each row of a column.

In the following example, we have a user-defined ``Video`` type.

.. code:: python

    class Video:

        def num_frames(self) -> int:
            return ...

        def get_frame(self, frame_index: int) -> Image:
            return ...

        def clip(self, start: float, end: float) -> Video:
            return ...

Given a Dataframe ``df`` with a column ``"videos"`` containing ``Video`` objects, we can run extract each video's number of frames using the ``.as_py(UserClass).user_method(...)`` inline UDF idiom.

.. code:: python

    # Treat each item in column "videos" as an Video object, and run .num_frames() on it
    df.with_column("num_frames", col("videos").as_py(Video).num_frames())

To run a more complicated function such as a function to "get the last frame of every video", we can use a ``.apply`` function which takes any user-provided Python function and maps it over the column.

.. code:: python

    def get_last_frame(video) -> Image:
        return video.get_frame(video.num_frames() - 1)

    # Run get_last_frame on each item in column "videos"
    df.with_column("last_frame", col("videos").apply(get_last_frame))


Stateless UDFs
--------------

Inline UDFs are great for convenience, but have two main limitations:

1. They can only run on single columns
2. They can only run on single items at a time

Daft provides the ``@udf`` decorator for defining your own UDFs that process multiple columns or multiple rows at a time.

For example, let's try writing a function that will clip Videos in a column, based on a start/end timestamps in another column.

.. code:: python

    from daft import udf

    @udf(return_type=Video)
    def clip_video_udf(
        videos,
        timestamps_start,
        timestamps_end,
    ):
        return [
            video.clip(start, end)
            for video, start, end in
            zip(videos, timestamps_start, timestamps_end)
        ]

    df = df.with_column(
        "clipped_videos",
        clip_video_udf(col("videos"), col("clip_start"), col("clip_end")),
    )

Some important things to note:

Input Types
^^^^^^^^^^^

The inputs to our UDF in the above example (``videos``, ``timestamps_start`` and ``timestamps_end``) are provided to our functions as Python lists by default.

However, if your application can benefit from more efficient datastructures, you may choose to have Daft pass in other datastructures instead such as a Numpy array. You can do so simply by type-annotating your function parameters with the appropriate type:

.. code:: python

    import numpy as np

    @udf(return_type=Video)
    def clip_video_udf(
        videos,
        timestamps_start: np.ndarray,
        timestamps_end: np.ndarray,
    ):
        ...

If you don't provide any type annotations, then Daft just passes in a normal Python list!

Other supported input types and their type annotations are:

* Python list: ``list`` or ``typing.List``
* Numpy array: ``numpy.ndarray``
* Pandas series: ``pandas.Series``
* Polars series: ``polars.Series``
* PyArrow array: ``pyarrow.Array``

.. WARNING::
    Type annotation can be finicky in Python, depending on the version of Python you are using and if you are using typing
    functionality from future Python versions with ``from __future__ import annotations``.

    Daft will throw an error if it cannot infer types from your annotations, and you may choose to provide your types
    explicitly as a dictionary of input parameter name to its type in the ``@udf(type_hints=...)`` keyword argument.

.. NOTE::

    Numpy arrays and Pandas series cannot properly represent Nulls - they will cast Nulls to NaNs! If you need to represent Nulls,
    use Polars series or PyArrow arrays instead.

Return Types
^^^^^^^^^^^^

You can define the return type of the UDF by passing in the ``return_type=`` keyword argument to the ``@udf`` decorator. This will inform Daft what the type of the resulting column from your UDF is.

Inside of your function itself, you can return data in any of the options that are supported as input types (lists, numpy arrays, pandas series, polars series or PyArrow arrays).


Stateful UDFs
-------------

For many Machine Learning applications, we often have expensive initialization steps for our UDFs such as downloading models and loading models into GPU memory. Ideally we would like to do these initialization steps once, and share the cost of running them across multiple invocations of the UDF.

Daft provides an API for Stateful UDFs to do this. Stateful UDFs are just like Stateless UDFs, except that they are represented by Classes instead of Functions. Stateful UDF classes define any expensive initialization steps in their __init__ methods, and run on any columns or data in the __call__ method.

For example, to download and run a model on a column of images:

.. code:: python

    @udf(return_type=int)
    class ClassifyImages:

        def __init__(self):
            # Run any expensive initializations
            self._model = get_model()

        def __call__(self, images: np.ndarray):
            # Run model on columnes
            return self._model(images)

Running Stateful UDFs are exactly the same as running their Stateless cousins.

.. code:: python

    df = df.with_column("image_classifications", ClassifyImages(col("images")))


Resource Requests
-----------------

Sometimes, you may want to request for specific resources for your UDF. For example, some UDFs need one GPU to run as they will load a model onto the GPU.

As of Daft v0.0.22, resource requests are no longer in UDF definition. Instead, custom resources can be requested when you call ``.with_column``:

.. code:: python

    from daft.resource_request import ResourceRequest

    @udf(return_type=int)
    def func():
        model = get_model().cuda()

    # Runs the UDF `func` with the specified resource requests
    df = df.with_column(
        "image_classifications",
        func(df["images"]),
        resource_request=ResourceRequest(num_gpus=1, num_cpus=8),
    )

In the above example, if ran Daft on a Ray cluster consisting of 8 GPUs and 64 CPUs, Daft would be able to run 8 replicas of your UDF in parallel, thus massively increasing the throughput of your UDF!
