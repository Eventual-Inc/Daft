---
name: "daft-udf-tuning"
description: "Optimize Daft UDF performance. Invoke when user needs GPU inference, encounters slow UDFs, or asks about async/batch processing."
---

# Daft UDF Tuning

Optimize User-Defined Functions for performance.

## UDF Types

| Type | Decorator | Use Case |
|---|---|---|
| **Stateless** | `@daft.func` | Simple transforms. Use `async` for I/O-bound tasks. |
| **Stateful** | `@daft.cls` | Expensive init (e.g., loading models). Supports `gpus=N`. |
| **Batch** | `@daft.func.batch` | Vectorized CPU/GPU ops (NumPy/PyTorch). Faster. |

## Quick Recipes

### 1. Async I/O (Web APIs)

```python
@daft.func
async def fetch(url: str):
    async with aiohttp.ClientSession() as s:
        return await s.get(url).text()
```

### 2. GPU Batch Inference (PyTorch/Models)

```python
@daft.cls(gpus=1)
class Classifier:
    def __init__(self):
        self.model = load_model().cuda() # Run once per worker

    @daft.method.batch(batch_size=32)
    def predict(self, images):
        return self.model(images.to_pylist())

# Run with concurrency
df.with_column("preds", Classifier(max_concurrency=4).predict(df["img"]))
```

## Tuning Keys

-   **`max_concurrency`**: Total parallel UDF instances.
-   **`gpus=N`**: GPU request per instance.
-   **`batch_size`**: Rows per call. Too small = overhead; too big = OOM.
-   **`into_batches(N)`**: Pre-slice partitions if memory is tight.
