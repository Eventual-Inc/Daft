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
        return self.num_frames

    def get_frame(self, frame_index: int) -> Image:
        return self.frames[frame_index]

Given a Dataframe ``df`` with a column ``"videos"`` containing ``Video`` objects, we can run extract each video's number of frames using the ``.as_py(UserClass).user_method(...)`` inline UDF idiom.

.. code:: python

    # Treat each item in column "videos" as an Video object, and run .num_frames() on it
    df.with_column("num_frames", col("videos").as_py(Video).num_frames())

To run a more complicated function such as a function to "get the last frame of every video", we can use a ``.apply`` function which takes any user-provided Python function and maps it over the column.

.. code:: python

    def get_last_frame(video):
    return video.get_frame(video.num_frames() - 1)

    # Run get_last_frame on each item in column "videos"
    df.with_column("last_frame", col("videos").apply(get_last_frame))


Stateless UDFs
--------------

Inline UDFs are great for convenience, but have two main limitations:

1. They can only run on single columns
2. They can only run on single items at a time

Daft provides the ``@polars_udf`` decorator for defining your own UDFs that process multiple columns or multiple rows at a time.

For example, let's try writing a function that will clip Videos in a column, based on a start/end timestamps in another column.

.. code:: python

    from daft import polars_udf

    @polars_udf(return_type=Video)
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

1. The ``@polars_udf`` decorator wraps a normal Python function as a Daft UDF. You can define the return type of the UDF by passing in the ``return_type=`` keyword argument.
2. Daft will pass columns into your UDF as a `Polars Series <https://pola-rs.github.io/polars/py-polars/html/reference/series/index.html>`_. If the UDF operates on multiple columns, all columns will be represented by Polars Series of the same length.
3. Your UDF can execute any arbitrary user-defined logic, but needs to return a sequence equal to the lengths of the inputs. Valid DaFt UDF return sequence types are: ``list``, ``numpy.ndarray``, ``pandas.Series`` and ``polars.Series``.
4. To obtain a new column with our intended results, we simply run our UDF on columns in the DataFrame.


Stateful UDFs
-------------

For many Machine Learning applications, we often have expensive initialization steps for our UDFs such as downloading models and loading models into GPU memory. Ideally we would like to do these initialization steps once, and share the cost of running them across multiple invocations of the UDF.

Daft provides an API for Stateful UDFs to do this. Stateful UDFs are just like Stateless UDFs, except that they are represented by Classes instead of Functions. Stateful UDF classes define any expensive initialization steps in their __init__ methods, and run on any columns or data in the __call__ method.

For example, to download and run a model on a column of images as shown in the :doc:`../quickstart` guide:

.. code:: python

    @udf(return_type=int)
    class ClassifyImages:

    def __init__(self):
        # Run any expensive initializations
        self._model = get_model()

    def __call__(self, images):
        # Run model on columnes
        return self._model(images)

Running Stateful UDFs are exactly the same as running their Stateless cousins.

.. code:: python

    df = df.with_column("image_classifications", ClassifyImages(col("images")))


Resource Requests
-----------------

Sometimes, you may want to request for specific resources for your UDF. For example, some UDFs need one GPU to run as they will load a model onto the GPU.

Daft provides a simple API for indicating GPU and CPU requests.

.. code:: python

    # Requires one GPU and eight CPUs to run
    @udf(return_type=int, num_gpus=1, num_cpus=8)
    def func():
        model = get_model().cuda()

In the above example, if ran Daft on a Ray cluster consisting of 8 GPUs and 64 CPUs, Daft would be able to run 8 replicas of your UDF in parallel, thus massively increasing the throughput of your UDF!
