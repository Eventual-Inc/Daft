# Multimodal Structured Outputs with Daft, Gemma-3n, and vLLM

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/structured_outputs/mm_structured_outputs.ipynb)

## Introduction

This notebook walks through a practical example of evaluating model performance on structured generation and multimodal reasoning. We will explore how to scale multimodal image understanding evaluation using a combination of powerful technologies:

1. Daft for data processing
2. Gemma-3n-e4b-it model for multimodal capabilities
3. vLLM/OpenRouter for efficient inference serving.
4. HuggingFace Datasets

### Table of Contents

1. Setup and Install Dependencies
2. Launch a vLLM OpenAI API compatible server
3. Testing the OpenAI client Gemma-3n client with an API key and new base url.
4. Preprocess the AI2D dataset from huggingface's Cauldron collection
5. Multimodal Inference with Structured Outputs using 3 approaches
6. Analysis of the inference results
7. Putting everything together: Evaluating Gemma-3n-e4b-it across the AI2D subset
8. Conclusion
9. Appendix

## Quickstart

You will be prompted to restart the session following installation, which simply clears local variables. If you ever need to kill the session or restart further into the notebook, you will not need to reinstall dependencies or authenticate with HuggingFace.

```bash
pip install "daft[huggingface]==0.6.1" vllm

# Login to Hugging Face to access Gemma-3n
hf auth login
```

## OpenAI Compatible Online Serving

### Option 1: Launch vLLM OpenAI Compatible Server

Run this in a terminal (for Colab, open the terminal from the left sidebar):

```bash
python -m vllm.entrypoints.openai.api_server \
  --model google/gemma-3n-e4b-it \
  --enable-chunked-prefill \
  --guided-decoding-backend guidance \
  --dtype bfloat16 \
  --gpu-memory-utilization 0.85 \
  --host 0.0.0.0 --port 8000
```

- This config is optimized for Google Colab's A100 and `gemma-3n-e4b-it`.
- Server warmup typically takes about 7.5 minutes before it’s ready.

### Option 2: Connect to a Provider

Override `base_url` and `api_key` when creating the OpenAI client.

- OpenRouter and LM Studio both support `google/gemma-3n-e4b-it`.
- Not all Gemma 3 series text-generation models support image inputs.

### Test OpenAI Client Requests

```python
from openai import OpenAI

api_key = "none"  # or your provider key
base_url = "http://0.0.0.0:8000/v1"
model_id = "google/gemma-3n-e4b-it"
client = OpenAI(api_key=api_key, base_url=base_url)
```

#### List models to verify connectivity:

```python
result = client.models.list()
print(result)
```

#### Simple text completion:

```python
chat_completion = client.chat.completions.create(
    messages=[{"role": "user", "content": "What is the capital of the United States?"}],
    model=model_id,
)

print("Chat completion output:\n", chat_completion.choices[0].message.content)
```

#### Structured output (guided choice):

```python
completion = client.chat.completions.create(
    model=model_id,
    messages=[
        {"role": "user", "content": "Classify this sentiment: Daft is wicked fast!"}
    ],
    extra_body={"guided_choice": ["positive", "negative"]},
)
print(completion.choices[0].message.content)
```

#### Combining Image Inputs with Structured Output

We can play with prompting/structured outputs to understand how prompting and structured outputs can affect results.

Try commenting out the `extra_body` argument or the third user content text prompt to see how results change.

```python
completion = client.chat.completions.create(
    model=model_id,
    messages = [
        {
            "role": "system",
            "content": [{"type": "text", "text": "You are a helpful assistant."}]
        },
        {
            "role": "user",
            "content": [
                {"type": "image_url", "image_url": {"url":image_url}},
                {"type": "text", "text": "Which insect is portrayed in the image: A. Ladybug, B. Beetle, C. Bee, D. Wasp "},
                #{"type": "text", "text": "Answer with only the letter from the multiple choice. "} # Try comment me out
            ]
        }
    ],
    extra_body={"guided_choice": ["A", "B", "C", "D"]}, # Try comment out

)
print(completion.choices[0].message.content)
```

## Step 1: Dataset Preprocessing

### Prepping the HuggingFaceM4/the_cauldron Dataset (AI2D subset)

We can read directly from Hugging Face datasets by leveraging the `hf://` prefix in the URL string.

```python
import daft

# There are a total of 2,434 images in this dataset, at a size of ~500 MB
df_raw = daft.read_parquet(
    'hf://datasets/HuggingFaceM4/the_cauldron/ai2d/train-00000-of-00001-2ce340398c113b79.parquet'
).collect()
df_raw.show(3)
```

Taking a look at the schema we can see the familiar messages nested datatype we are used to in chat completions inside the `texts` column.

```python
print(df_raw.schema())
```

### Decode images and add base64 encoding

Let’s decode the image bytes to preview images and also add a base64-encoded column.

```python
from daft import col

df = df_raw.explode(col("images")).with_columns({
    "image":    df_raw["images"].struct.get("bytes").image.decode(),
    "image_base64": df_raw["images"].struct.get("bytes").encode("base64")
})
df.show(3)
```

### Extract question, choices, and answer from `texts`

Each entry’s `texts` column is a list of dicts resembling OpenAI chat messages. We’ll explode these to extract `user` and `assistant`, then parse question, choices, and answer.

```python
# Explode the List of Dicts inside "texts" to extract "user" and "assistant" messages
df = df.explode(col("texts")).collect()

# Extract User and Assistant Messages
df = df.with_columns({
    "user": df["texts"].struct.get("user"),
    "assistant": df["texts"].struct.get("assistant")
}).collect()
df.show(3)
```

### Parse question, choices, and answer fields.

```python
df_prepped = df.with_columns({
    "question": df["user"]
        .str.extract(r"(?s)Question:\s*(.*?)\s*Choices:")
        .str.replace("Choices:", "")
        .str.replace("Question:", ""),
    "choices_string": df["user"]
        .str.extract(r"(?s)Choices:\s*(.*?)\s*Answer?\.?")
        .str.replace("Choices:\n", "")
        .str.replace("Answer", ""),
    "answer": df["assistant"]
        .str.extract(r"Answer:\s*(.*)$")
        .str.replace("Answer:", ""),
}).collect()

df_prepped.show(3)
```

## Step 2: Multimodal Inference with Structured Outputs

We will explore three methods of implementing structured outputs on images:

- Naive Row-Wise UDF
- Naive Async Batch UDF
- Production Batch UDF

First, set up the client and shared parameters.

```python
from openai import AsyncOpenAI, OpenAI
import time

model_id = 'google/gemma-3n-e4b-it'
api_key = "none"
base_url = "http://0.0.0.0:8000/v1"
client = AsyncOpenAI(api_key=api_key, base_url=base_url)
row_limit = 10000
```

### Naive Row-Wise UDF

```python
import daft

@daft.func()
async def struct_output_rowwise(model_id: str, text_col: str, image_col: str, extra_body: dict | None = None) -> str:
    client_sync = OpenAI(api_key=api_key, base_url=base_url)
    content = [{"type": "text", "text": text_col}]
    if image_col:
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{image_col}"},
        })

    result = client_sync.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": content
            }
        ],
        model=model_id,
        extra_body=extra_body,
    )
    return result.choices[0].message.content
```

Run the Row-wise UDF and compute accuracy:

```python
from daft import col
from daft.functions import format

start = time.time()
df_rowwise_udf = df_prepped.with_column("result", struct_output_rowwise(
    model_id = model_id,
    text_col = format("{} \n {}", col("question"), col("choices_string")),
    image_col = col("image_base64"),
    extra_body={"guided_choice": ["A", "B", "C", "D"]}
)).with_column(
    "is_correct",
    col("result").str.lstrip().str.rstrip() == col("answer").str.lstrip().str.rstrip()
).limit(row_limit).collect()
end = time.time()
print(f"Row-wise UDF - Processed {df_rowwise_udf.count_rows()} rows in {end-start} seconds, {df_rowwise_udf.count_rows()/(end-start)} rows/s")
```

Write down each of your runs here:

- Row-wise UDF - Processed ...

### Async Batch UDF

```python
import asyncio

@daft.udf(return_dtype=daft.DataType.string())
def struct_output_batch(
        model_id: str,
        text_col: daft.Series,
        image_col: daft.Series,
        extra_body: dict | None = None
    ) -> list[str]:

    async def generate(model_id: str, text: str, image: str) -> str:
        content = [{"type": "text", "text": text}]
        if image:
            content.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{image}"},
            })

        result = await client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": content
                }
            ],
            model=model_id,
            extra_body=extra_body,
        )
        return result.choices[0].message.content

    texts = text_col.to_pylist()
    images = image_col.to_pylist()

    async def gather_completions() -> list[str]:
        tasks = [generate(model_id, t, i) for t, i in zip(texts, images)]
        return await asyncio.gather(*tasks)

    return asyncio.run(gather_completions())
```

Run the Async Batch UDF and compute accuracy:

```python
start = time.time()
df_batch_udf = df_prepped.with_column("result", struct_output_batch(
    model_id = model_id,
    text_col = format("{} \n {}", col("question"), col("choices_string")),
    image_col = col("image_base64"),
    extra_body={"guided_choice": ["A", "B", "C", "D"]}
)).with_column(
    "is_correct",
    col("result").str.lstrip().str.rstrip() == col("answer").str.lstrip().str.rstrip()
).limit(row_limit).collect()
end = time.time()
print(f"Batch UDF - Processed {df_batch_udf.count_rows()} rows in {end-start} seconds, {df_batch_udf.count_rows()/(end-start)} rows/s")
```

Write down each of your runs here:

- Batch UDF - Processed ...

Before you move on to the Production UDF, try increasing the `row_limit` variable to 500, 1000, and 2000 rows.

- What happens if you try to run the full dataset (7462 rows)?
- How does row processing rate change when you increase the `row_limit`?
- Do you run into any issues?

### Production UDF

Here is what a production version of our naive user defined functions looks like.

```python
batch_size = 32
concurrency = 4
max_conn = 32
```


```python
from typing import Any

@daft.udf(return_dtype=daft.DataType.string(), concurrency=concurrency, batch_size=batch_size)
class StructuredOutputsProdUDF:
    def __init__(self, base_url: str, api_key: str):
        self.client = AsyncOpenAI(base_url=base_url, api_key=api_key)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    def __call__(self,
        model_id: str,
        text_col: daft.Series,
        image_col: daft.Series,
        sampling_params: dict[str, Any] | None = None,
        extra_body: dict[str, Any] | None = None
        ) -> list[str]:

        async def generate(text: str, image: str) -> str:
            content = []
            if image:
                content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{image}"},
                })
            if text:
                content.append({"type": "text", "text": text})

            result = await self.client.chat.completions.create(
                messages=[
                    {
                        "role": "user",
                        "content": content  # Dataset prefers image first
                    }
                ],
                model=model_id,
                extra_body=extra_body,
                **sampling_params
            )
            return result.choices[0].message.content

        async def infer_with_semaphore(t, i):
            return await generate(t, i)

        async def gather_completions(texts, images) -> list[str]:
            tasks = [infer_with_semaphore(t, i) for t, i in zip(texts, images)]
            return await asyncio.gather(*tasks)

        texts = text_col.to_pylist()
        images = image_col.to_pylist()

        return self.loop.run_until_complete(gather_completions(texts, images))
```

Run the Production UDF and compute accuracy:

```python
start = time.time()
df_prod_udf = df_prepped.with_column("result", StructuredOutputsProdUDF.with_init_args(
    base_url=base_url,
    api_key=api_key,
).with_concurrency(concurrency)(
    model_id = model_id,
    text_col = format("{} \n {}", col("question"), col("choices_string")),
    image_col = col("image_base64"),
    extra_body={"guided_choice": ["A", "B", "C", "D"]}
)).with_column(
    "is_correct",
    col("result").str.lstrip().str.rstrip() == col("answer").str.lstrip().str.rstrip()
).limit(row_limit).collect()
end = time.time()
print(f"Prod UDF - Processed {df_prod_udf.count_rows()} rows in {end-start} seconds, {df_prod_udf.count_rows()/(end-start)} rows/s")
```

## Analysis

Evaluate pass/fail rate using the production UDF output.

```python
pass_fail_rate = df_prod_udf.where(col("is_correct")).count_rows() / df_prod_udf.count_rows()
print(f"Pass/Fail Rate: {pass_fail_rate}")
```

How does this compare without images? Here we use Daft’s native inference function `llm_generate`.

```python
from daft.functions import llm_generate

start = time.time()
df_prod_no_img = df_prepped.with_column("result", llm_generate(
    input_column = format("{} \n {}", col("question"), col("choices_string")),
    model = model_id,
    extra_body={"guided_choice": ["A", "B", "C", "D"]},
    api_key=api_key,
    base_url=base_url,
    provider = "openai"
)).with_column(
    "is_correct",
    col("result").str.lstrip().str.rstrip() == col("answer").str.lstrip().str.rstrip()
).collect()
end = time.time()
print(f"llm_generate - Processed {df_prod_no_img.count_rows()} rows in {end-start} seconds, {df_prod_no_img.count_rows()/(end-start)} rows/s")
```

```python
pass_fail_rate_no_img = df_prod_no_img.where(col("is_correct")).count_rows() / df_prod_no_img.count_rows()
print(f"Pass/Fail Rate (no image): {pass_fail_rate_no_img}")
```

## Evaluating Gemma across the AI2D Dataset

Now that we have walked through implementing this image understanding evaluation pipeline end to end, let’s put it all together to take advantage of lazy evaluation and enable future extensibility and reuse.

```python
from typing import Any
import asyncio
import base64

import daft
from daft import col
from daft.functions import format
from openai import AsyncOpenAI

import logging

logger = logging.getLogger(__name__)

@daft.udf(return_dtype=daft.DataType.string(), concurrency=4)
class StructuredOutputsProdUDF:
    def __init__(self, base_url: str, api_key: str):
        self.client = AsyncOpenAI(base_url=base_url, api_key=api_key)
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

    def __call__(self,
        model_id: str,
        text_col: daft.Series,
        image_col: daft.Series,
        sampling_params: dict[str, Any] | None = None,
        extra_body: dict[str, Any] | None = None
        ):

        async def generate(text: str, image: str) -> str:
            content = []
            if image:
                content.append({
                    "type": "image_url",
                    "image_url": {"url": f"data:image/png;base64,{image}"},
                })
            if text:
                content.append({"type": "text", "text": text})

            result = await self.client.chat.completions.create(
                messages=[
                    {
                        "role": "user",
                        "content": content  # Dataset prefers image first
                    }
                ],
                model=model_id,
                extra_body=extra_body,
                **(sampling_params or {})
            )
            return result.choices[0].message.content

        async def infer_with_semaphore(t, i):
            return await generate(t, i)

        async def gather_completions(texts, images) -> list[str]:
            tasks = [infer_with_semaphore(t, i) for t, i in zip(texts, images)]
            return await asyncio.gather(*tasks)

        texts = text_col.to_pylist()
        images = image_col.to_pylist()

        return self.loop.run_until_complete(gather_completions(texts, images))

class TheCauldronImageUnderstandingEvaluationPipeline:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key

    def __call__(self,
        model_id: str,
        dataset_uri: str,
        sampling_params: dict[str,Any] | None = None,
        concurrency: int = 4,
        row_limit: int | None = None,
        is_eager: bool = False,
    ) -> daft.DataFrame:
        """Executes dataset loading, preprocessing, inference, and post-processing.
        Evaluation must be run separately since it requires materialization.
        """

        if is_eager:
            # Load Dataset and Materialize
            df = self.load_dataset(dataset_uri)
            df = df.limit(row_limit) if row_limit else df
            df = self._log_processing_time(df)

            # Preprocess
            df = self.preprocess(df)
            df = self._log_processing_time(df)

            # Perform Inference
            df = self.infer(df, model_id, sampling_params)
            df = self._log_processing_time(df)

            # Post-Process
            df = self.postprocess(df)
            df = self._log_processing_time(df)
        else:
            df = self.load_dataset(dataset_uri)
            df = self.preprocess(df)
            df = self.infer(df, model_id, sampling_params)
            df = self.postprocess(df)
            df = df.limit(row_limit) if row_limit else df

        return df

    @staticmethod
    def _log_processing_time(df: daft.DataFrame):
        start = time.time()
        df_materialized = df.collect()
        end = time.time()
        num_rows = df_materialized.count_rows()
        logger.info(f"Processed {num_rows} rows in {end-start} sec, {num_rows/(end-start)} rows/s")
        return df_materialized

    def load_dataset(self, uri: str) -> daft.DataFrame:
        return daft.read_parquet(uri)

    def preprocess(self, df: daft.DataFrame) -> daft.DataFrame:
        # Convert png image byte string to base64
        df = df.explode(col("images")).with_column(
            "image_base64",
            df["images"].struct.get("bytes").encode("base64")
        )

        # Explode Lists of User Prompts and Assistant Answer Pairs
        df = df.explode(col("texts")).with_columns({
            "user": df["texts"].struct.get("user"),
            "assistant": df["texts"].struct.get("assistant")
        })

        # Parse the Question/Answer Strings
        df = df.with_columns({
            "question": df["user"]
                .str.extract(r"(?s)Question:\s*(.*?)\s*Choices:")
                .str.replace("Choices:", "")
                .str.replace("Question:",""),
            "choices_string": df["user"]
                .str.extract(r"(?s)Choices:\s*(.*?)\s*Answer?\.?")
                .str.replace("Choices:\n", "")
                .str.replace("Answer",""),
            "answer": df["assistant"]
                .str.extract(r"Answer:\s*(.*)$")
                .str.replace("Answer:",""),
        })
        return df

    def infer(self,
        df: daft.DataFrame,
        model_id: str = 'google/gemma-3n-e4b-it',
        sampling_params: dict[str,Any] = {"temperature": 0.0},
        concurrency: int = 4,
        extra_body: dict[str, Any] = {"guided_choice": ["A", "B", "C", "D"]}
    ) -> daft.DataFrame:

        return df.with_column("result", StructuredOutputsProdUDF.with_init_args(
            base_url=self.base_url,
            api_key=self.api_key,
        ).with_concurrency(concurrency)(
            model_id = model_id,
            text_col = format("{} \n {}", col("question"), col("choices_string")),
            image_col = col("image_base64"),
            sampling_params = sampling_params,
            extra_body=extra_body
        ))

    def postprocess(self, df: daft.DataFrame) -> daft.DataFrame:
        df = df.with_column("is_correct", col("result").str.lstrip().str.rstrip() == col("answer").str.lstrip().str.rstrip())
        return df

    def evaluate(self, df: daft.DataFrame) -> float:
        pass_fail_rate = df.where(col("is_correct")).count_rows() / df.count_rows()
        return pass_fail_rate
```

```python
# Our entire pipeline collapses into three lines
dataset_uri = 'hf://datasets/HuggingFaceM4/the_cauldron/ai2d/train-00000-of-00001-2ce340398c113b79.parquet'

pipeline = TheCauldronImageUnderstandingEvaluationPipeline(
    api_key = "none",
    base_url = "http://0.0.0.0:8000/v1"
)

df = pipeline(
    model_id = 'google/gemma-3n-e4b-it',
    sampling_params={"temperature": 0.1},
    is_eager=True
)
```

```python
# Materialize if not eager
df_mat = df.collect()
```

```python
# Print the Pass/Fail Rate
print(f"Pass/Fail Rate: {pipeline.evaluate(df_mat)}")
```

---

## Conclusion

In this guide we evaluated Gemma-3’s image understanding on the AI2D subset from Hugging Face’s TheCauldron dataset, explored structured outputs with guided choices, and demonstrated scaling with Daft UDFs. For distributed execution across larger datasets or multiple GPUs, consider switching Daft’s execution context to Ray.

```bash
pip install "daft[huggingface,ray]"
```

```python
import daft

daft.set_runner_ray()
```

---

## Appendix

From Gemma Model Card (transformers example):

```python
from transformers import AutoProcessor, Gemma3nForConditionalGeneration
import torch

model_id = "google/gemma-3n-e4b-it"
model = Gemma3nForConditionalGeneration.from_pretrained(
    model_id,
    device_map="auto",
    torch_dtype=torch.bfloat16,
).eval()
processor = AutoProcessor.from_pretrained(model_id)

messages = [
    {
        "role": "system",
        "content": [{"type": "text", "text": "You are a helpful assistant."}]
    },
    {
        "role": "user",
        "content": [
            {"type": "image", "image": "https://huggingface.co/datasets/huggingface/documentation-images/resolve/main/bee.jpg"},
            {"type": "text", "text": "Describe this image in detail."}
        ]
    }
]

inputs = processor.apply_chat_template(
    messages,
    add_generation_prompt=True,
    tokenize=True,
    return_dict=True,
    return_tensors="pt",
).to(model.device)

input_len = inputs["input_ids"].shape[-1]

with torch.inference_mode():
    generation = model.generate(**inputs, max_new_tokens=100, do_sample=False)
    generation = generation[0][input_len:]

decoded = processor.decode(generation, skip_special_tokens=True)
print(decoded)
```
