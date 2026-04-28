# Working with GPUs

For GPU work, always reach for `@daft.cls`: it initializes your model once in `__init__` and reuses it across rows, so the expensive model load is amortized over the whole query. Request a GPU with the `gpus` parameter on the class decorator.

## Basic pattern

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

The class is initialized once per Python worker; the model stays resident on the GPU for the whole query.

## Packing multiple invocations per GPU

`gpus` accepts fractional values up to 1.0. Use this when a single invocation cannot saturate the GPU — for example, a small model with meaningful preprocessing overhead.

```python
@daft.cls(gpus=0.5, max_concurrency=2)
class SmallModel:
    def __init__(self, name: str):
        import torch
        self.model = torch.load(name).cuda()

    @daft.method.batch(return_dtype=DataType.float32(), batch_size=16)
    def infer(self, x: Series) -> list[float]:
        return self.model(x.to_arrow().to_numpy()).tolist()
```

With `gpus=0.5` and `max_concurrency=2`, two instances share one physical GPU. Daft isolates `CUDA_VISIBLE_DEVICES` per process, so device ordinals stay consistent inside each instance.

Values above 1.0 must be integers (e.g. `gpus=2` for model parallelism across two GPUs).

## Examples by model family

### Sentence embeddings (Sentence Transformers)

```python
@daft.cls(gpus=1)
class SentenceEmbedder:
    def __init__(self, model_name: str = "BAAI/bge-large-en-v1.5"):
        from sentence_transformers import SentenceTransformer
        self.model = SentenceTransformer(model_name, device="cuda")

    @daft.method.batch(return_dtype=DataType.list(DataType.float32()), batch_size=128)
    def embed(self, text: Series) -> list[list[float]]:
        return self.model.encode(text.to_pylist(), batch_size=128).tolist()
```

### Image classification / CLIP (Transformers)

```python
@daft.cls(gpus=1)
class CLIPScorer:
    def __init__(self, model_name: str = "openai/clip-vit-base-patch32"):
        import torch
        from transformers import CLIPModel, CLIPProcessor
        self.model = CLIPModel.from_pretrained(model_name).to("cuda").eval()
        self.processor = CLIPProcessor.from_pretrained(model_name)
        self.torch = torch

    @daft.method.batch(return_dtype=DataType.list(DataType.float32()), batch_size=32)
    def embed_images(self, image_bytes: Series) -> list[list[float]]:
        from io import BytesIO
        from PIL import Image

        images = [Image.open(BytesIO(b)).convert("RGB") for b in image_bytes.to_pylist()]
        inputs = self.processor(images=images, return_tensors="pt").to("cuda")
        with self.torch.no_grad():
            features = self.model.get_image_features(**inputs)
        return features.cpu().tolist()
```

### Text generation (vLLM)

For production-grade LLM inference, Daft ships a built-in vLLM integration — see [AI Functions › Prompt](../ai-functions/prompt.md). For smaller custom generation tasks:

```python
@daft.cls(gpus=1)
class Generator:
    def __init__(self, model_name: str = "Qwen/Qwen2.5-1.5B-Instruct"):
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name, torch_dtype=torch.bfloat16, device_map="cuda"
        ).eval()
        self.torch = torch

    @daft.method(return_dtype=DataType.string())
    def generate(self, prompt: str) -> str:
        inputs = self.tokenizer(prompt, return_tensors="pt").to("cuda")
        with self.torch.no_grad():
            output = self.model.generate(**inputs, max_new_tokens=128)
        return self.tokenizer.decode(output[0], skip_special_tokens=True)
```

### Whisper (audio transcription)

```python
@daft.cls(gpus=1)
class Whisper:
    def __init__(self, model_name: str = "openai/whisper-large-v3"):
        import torch
        from transformers import pipeline
        self.pipe = pipeline(
            "automatic-speech-recognition",
            model=model_name,
            torch_dtype=torch.float16,
            device="cuda",
        )

    @daft.method(return_dtype=DataType.string())
    def transcribe(self, audio_path: str) -> str:
        return self.pipe(audio_path)["text"]
```

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
