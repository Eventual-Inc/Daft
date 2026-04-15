# Working with GPUs

Daft exposes GPU placement through a single parameter — `gpus` — on both `@daft.func` and `@daft.cls`. You do not need a class to run on the GPU. Use `@daft.func(gpus=...)` when initialization is cheap, and reach for `@daft.cls` only when you need to amortize an expensive setup step (like loading a model) across many rows.

## Requesting a GPU with `@daft.func`

The simplest GPU-backed function: decorate, ask for a GPU, and let Daft handle placement and `CUDA_VISIBLE_DEVICES`.

```python
import daft

@daft.func(gpus=1)
def embed(text: str) -> list[float]:
    import torch
    from sentence_transformers import SentenceTransformer

    # NOTE: loaded per-row here — fine for quick demos, but prefer @daft.cls below
    model = SentenceTransformer("all-MiniLM-L6-v2").cuda()
    return model.encode(text).tolist()

df = daft.from_pydict({"text": ["hello", "world"]})
df = df.select(embed(df["text"]))
```

Use `@daft.func.batch(gpus=1, batch_size=32)` for vectorized inference where the model accepts a batch of inputs at once.

## Amortizing model load with `@daft.cls`

Loading a model per row is almost never what you want. `@daft.cls` initializes the class once per worker, so the GPU-resident model stays loaded across rows.

```python
import daft
from daft import DataType, Series

@daft.cls(gpus=1)
class Embedder:
    def __init__(self, model_name: str):
        from sentence_transformers import SentenceTransformer
        self.model = SentenceTransformer(model_name).cuda()

    @daft.method.batch(return_dtype=DataType.list(DataType.float32()), batch_size=64)
    def encode(self, text: Series) -> list[list[float]]:
        return self.model.encode(text.to_pylist()).tolist()

embedder = Embedder("all-MiniLM-L6-v2")
df = daft.from_pydict({"text": ["hello", "world", "daft"]})
df = df.select(embedder.encode(df["text"]))
```

## Packing multiple workers per GPU

`gpus` accepts fractional values up to 1.0. This is useful when a single worker cannot saturate the GPU — for example, a small model with a lot of preprocessing overhead.

```python
@daft.cls(gpus=0.5, max_concurrency=2)
class SmallModel:
    def __init__(self, name: str):
        import torch
        self.model = torch.load(name).cuda()

    @daft.method.batch(batch_size=16)
    def infer(self, x: Series) -> Series:
        return self.model(x.to_arrow().to_numpy())
```

With `gpus=0.5` and `max_concurrency=2`, two workers share one physical GPU. Daft still isolates `CUDA_VISIBLE_DEVICES` per worker, so device ordinals stay consistent inside each worker.

Values above 1.0 must be integers (e.g. `gpus=2` for model parallelism across two GPUs).

## Choosing a batch size

GPU throughput is batch-size sensitive. A few rules of thumb:

- Start by matching the batch size to the model's preferred inference batch size.
- Halve it if you hit out-of-memory errors — large rows (images, embeddings) eat VRAM fast.
- Increase it gradually while watching GPU utilization: if you are under 80%, the GPU is waiting on you.

See [Classes & Methods › Batch Sizing](cls.md#batch-sizing) for additional considerations.

## Retries and error handling

GPU inference can fail for reasons unrelated to your code — OOM, transient driver errors, preempted instances. Use `max_retries` and `on_error` to keep long queries going:

```python
@daft.cls(gpus=1, max_retries=2, on_error="log")
class Model:
    ...
```

See the shared [Resources, Concurrency, and Error Handling](func.md#resources-concurrency-and-error-handling) section for the full parameter reference.
