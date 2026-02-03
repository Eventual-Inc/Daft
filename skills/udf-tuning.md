# Skill: UDF Tuning for Performance

This guide provides recipes for optimizing Daft User-Defined Functions (UDFs) to maximize resource utilization and performance.

## Purpose

To help an AI programming assistant or developer choose the right UDF type and tune its parameters for efficient execution, especially when dealing with expensive computations or I/O-bound tasks.

## UDF APIs: A Quick Overview

Daft has two UDF APIs. The new API is preferred.

1.  **New API (`@daft.func`, `@daft.cls`):** The modern, more Pythonic, and recommended way.
    -   `@daft.func`: For stateless functions. Supports row-wise, async, generator, and batch variants.
    -   `@daft.cls`: For stateful classes, where expensive initialization (e.g., loading a model) can be shared across calls.
2.  **Legacy API (`@daft.udf`):** Older API. Still supported but will be deprecated. It combines function and class-based UDFs into a single decorator.

## How it Works: Tuning for Performance

Performance tuning revolves around matching the UDF's execution strategy to the workload and available hardware (CPU/GPU).

### Choosing Between Row-wise and Batch UDFs

-   **Row-wise (`@daft.func`):**
    -   **When to use:** For simple, non-vectorized logic or I/O-bound tasks. The overhead is higher per-row, but it's simpler to write.
    -   **Performance:** Use the `async` variant for I/O-bound tasks (e.g., API calls, database lookups). This allows Daft to concurrently execute many calls, hiding latency.

-   **Batch (`@daft.func.batch`, `@daft.method.batch`):**
    -   **When to use:** For CPU-bound or GPU-bound computations that can be vectorized. Ideal for machine learning inference, image processing, or numerical calculations.
    -   **Performance:** Significantly faster for vectorized operations because it amortizes the cost of Python function calls over a large batch of data and allows for use of optimized libraries like NumPy, PyArrow, or PyTorch.

### Key Tuning Parameters

| New API (`@daft.cls`)     | Legacy API (`@daft.udf`) | Purpose                                                                                                                              |
| ------------------------- | ------------------------ | ------------------------------------------------------------------------------------------------------------------------------------ |
| `max_concurrency`         | `concurrency`            | Controls the maximum number of parallel UDF instances. Key for tuning CPU-bound task parallelism.                                      |
| `gpus` (e.g., `gpus=1`)   | `num_gpus`               | Requests a specific number of GPUs for each UDF instance. Essential for GPU-based models. Daft's scheduler manages allocation.         |
| `batch_size` (on method)  | *(N/A)*                  | *(New in `@daft.method.batch`)* Controls the number of rows passed to each batch call. Useful for controlling memory usage per batch. |
| *(N/A)*                   | `num_cpus`, `memory_bytes` | More granular resource requests. These are less commonly used and have no direct equivalent in the new API; use `max_concurrency`.   |
| `unnest`                  | *(N/A)*                  | Flattens a returned dictionary/struct into multiple columns. A convenience feature.                                                  |

## Quick Recipes

### Recipe 1: Optimizing an I/O-Bound Task with Async UDF

**Goal:** Speed up a task that calls an external API for each row.

**Steps:**
1.  Define the function as `async def`.
2.  Use an async library like `aiohttp` for non-blocking I/O.
3.  Decorate with `@daft.func`. Daft automatically detects the async nature and runs it concurrently.

```python
import daft
import aiohttp
import asyncio

# Before: A slow, synchronous UDF
# @daft.func
# def fetch_url(url: str) -> str:
#     import requests
#     return requests.get(url).text

# After: An optimized, asynchronous UDF
@daft.func
async def fetch_url(url: str) -> str:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.text()

urls_df = daft.from_pydict({"urls": ["http://example.com", "http://example.org"]})
urls_df = urls_df.with_column("content", fetch_url(urls_df["urls"]))

# Daft will execute these API calls concurrently, dramatically improving throughput.
urls_df.collect()
```

### Recipe 2: High-Throughput GPU Inference with `@daft.cls`

**Goal:** Run a GPU-based ML model over a column of images efficiently.

**Steps:**
1.  Use `@daft.cls` to define a stateful UDF.
2.  Request GPUs with `gpus=1`.
3.  Load the model once in the `__init__` method. This state is reused.
4.  Implement the inference logic in a batch method decorated with `@daft.method.batch`.

```python
import daft
from daft import DataType, Series
import numpy as np

# A placeholder for a real model
class MyFakeModel:
    def __init__(self):
        # Pretend to load a large model onto the GPU
        print("Model loaded!")
        self._model_params = np.random.rand(10, 10)

    def predict_batch(self, image_batch: list[np.ndarray]) -> list[int]:
        # Pretend to run batch inference
        return [img.sum() % 10 for img in image_batch]

@daft.cls(gpus=1)
class ImageClassifier:
    def __init__(self):
        # This runs once per UDF instance on a worker with a GPU
        self.model = MyFakeModel()

    @daft.method.batch()
    def classify(self, image_series: Series) -> Series:
        # image_series.to_pylist() converts the batch to a Python list
        image_batch = image_series.to_pylist()
        # Run inference on the whole batch
        predictions = self.model.predict_batch(image_batch)
        return Series.from_pylist(predictions)

# Create some fake image data
df = daft.from_pydict({"images": [np.ones((32, 32)) for _ in range(100)]})

# Instantiate the UDF and apply it
classifier = ImageClassifier()
df = df.with_column("prediction", classifier.classify(df["images"]))

# If you have 4 GPUs, Daft can run 4 instances of ImageClassifier in parallel.
df.collect()
```

### Recipe 3: Overriding Options for Different Models

**Goal:** Reuse a UDF definition with different resource requests.

**Steps:**
1.  Define a class UDF with default resources.
2.  Use `YourUdfClass.override_options(...)` to create a new UDF with different parameters.

```python
@daft.cls(gpus=1)
class GenericModelRunner:
    # ... (implementation) ...
    pass

# Create a variant that requires more resources
HeavyModelRunner = GenericModelRunner.override_options(gpus=2)

# Now you can use both in the same Daft plan
# df = df.with_column("light_output", GenericModelRunner()(df["data"]))
# df = df.with_column("heavy_output", HeavyModelRunner()(df["data"]))
```

## Example Prompts for ClaudeCODE

-   "This Daft UDF is slow because it calls a web API in a loop. Can you rewrite it using an async UDF to improve performance?"
-   "Here's my PyTorch model. Create a stateful Daft UDF to run batch inference on a GPU. The input column is named 'images'."
-   "Explain the difference between `@daft.func` and `@daft.func.batch`. When should I use each?"

## Common Pitfalls & Checks

-   **Using Row-wise for Vectorized Code:** If your UDF contains a `for` loop that could be replaced by a NumPy or PyArrow batch operation, switch to `@daft.func.batch`.
-   **Loading Models in a Batch UDF:** If you load a model inside a `@daft.func.batch` UDF, it will be reloaded for every batch. This is very inefficient. Use `@daft.cls` to load the model once in `__init__`.
-   **Forgetting to Request GPUs:** If a UDF needs a GPU but `gpus=1` is not specified, it may be scheduled on a CPU-only worker and fail.
-   **Ignoring Batch Size:** For batch UDFs, very large batches can cause out-of-memory errors. Use `df.into_batches(N)` before the UDF call to control the input size. The `@daft.method.batch(batch_size=N)` parameter offers more fine-grained control for class methods.

## References

-   [Official Docs: New UDFs (@daft.func)](https://docs.daft.ai/en/latest/custom-code/func.html)
-   [Official Docs: New Class UDFs (@daft.cls)](https://docs.daft.ai/en/latest/custom-code/cls.html)
-   [Official Docs: Legacy UDFs (@daft.udf)](https://docs.daft.ai/en/latest/custom-code/udfs.html)
-   [Official Docs: Migration Guide](https://docs.daft.ai/en/latest/custom-code/migration.html)
