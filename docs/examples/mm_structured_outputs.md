# Evaluating Image Understanding at Scale with Structured Outputs

*An end-to-end example of **Multimodal Structured Outputs** with Daft and Qwen3-VL-8B*

## Introduction

In this notebook, we'll evaluate [Qwen3-VL](https://github.com/QwenLM/Qwen3-VL)'s image understanding using a multiple choice subset of HuggingFace's [The Cauldron dataset](https://huggingface.co/datasets/HuggingFaceM4/the_cauldron), a massive collection of 50 vision-language datasets.

Our pipeline will:

1. Run structured output inference on image+text prompts
2. Conduct an **ablation study** (with vs. without images) to isolate image understanding
3. Classify results into diagnostic quadrants
4. Use **VLM-as-a-Judge** to explain failures

The steps we'll take in this notebook are a simplified version of the [production-ready pipeline](https://github.com/Eventual-Inc/daft-examples/blob/main/use_cases/image_understanding_eval/eval_image_understanding.py) used to evaluate Qwen3-VL-4B on 20k rows. Check out the [blog post](https://www.daft.ai/blog/multimodal-structured-outputs-evaluating-vlm-image-understanding-at-scale) for the full results and implementation.

### Table of Contents

1. [Setup](#1-setup)
2. [Data Loading](#2-data-loading)
3. [Preprocessing](#3-preprocessing)
4. [Structured Outputs with `prompt`](#4-structured-outputs-with-prompt)
5. [Ablation Study](#5-ablation-study)
6. [Scale with Daft Cloud](#6-scale-with-daft-cloud)
7. [Conclusion](#7-conclusion)

## 1. Setup

First, install the required dependencies:

```bash
pip install daft openai numpy pillow python-dotenv pydantic
```

Then, set up your environment variables and configuration:

```python
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
MODEL_ID = "Qwen/Qwen3-VL-8B-Instruct"
LIMIT = 50  # Keep low for interactive demo

# HuggingFace Inference Provider (hosted Qwen3-VL endpoints)
OPENAI_API_KEY = os.getenv("HF_TOKEN")
OPENAI_BASE_URL = "https://router.huggingface.co/v1"
```

Configure Daft to use the OpenAI-compatible provider:

```python
import daft

# Set the OpenAI-compatible provider
daft.set_provider("openai", api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)
```

## 2. Data Loading

[The Cauldron](https://huggingface.co/datasets/HuggingFaceM4/the_cauldron) is a massive collection of 50 vision-language datasets spanning:
- Visual question answering
- OCR & document understanding
- Chart/figure understanding
- Reasoning & math
- And more...

We'll start with the **AI2D** subset—science diagrams with multiple-choice questions.

```python
df_raw = daft.read_huggingface("HuggingFaceM4/the_cauldron/ai2d").limit(LIMIT).collect()
df_raw.show(3)
```

## 3. Preprocessing

We need to:
1. Decode images into Daft's Image type
2. Extract the question, choices, and correct answer from the text

```python
from daft import col
from daft.functions import unnest

# Decode images
df_img = df_raw.explode(col("images"))
df_img = df_img.with_column("image", col("images")["bytes"].decode_image())

# Extract text fields (user question, assistant answer)
df_text = df_img.explode(col("texts")).select(unnest(col("texts")), "image")

# Parse the answer letter from "Answer: C" format
df_prep = df_text.with_column(
    "answer",
    col("assistant").regexp_replace("Answer: ", "").lstrip().rstrip()
).collect()

df_prep.show(3)
```

## 4. Structured Outputs with `prompt`

Daft's `prompt` function scales OpenAI-compatible calls across dataframes. We'll use a Pydantic model to enforce structured output.

For more info: [API docs](../api/functions/prompt.md) | [User Guide](../ai-functions/prompt.md)

```python
from daft.functions import prompt
from pydantic import BaseModel, Field
import time

class ChoiceResponse(BaseModel):
    """Structured output for multiple choice answers."""
    choice: str = Field(..., description="The letter of the correct choice (e.g., A, B, C, D)")

start = time.time()
df_results = df_prep.with_column(
    "result",
    prompt(
        messages=[col("image"), col("user")],
        model=MODEL_ID,
        use_chat_completions=True,
        return_format=ChoiceResponse,
    )
).limit(LIMIT).collect()
elapsed = time.time() - start

print(f"Processed {df_results.count_rows()} rows in {elapsed:.1f} seconds")
```

Evaluate correctness:

```python
# Evaluate correctness
df_eval = df_results.with_column(
    "is_correct",
    col("result")["choice"].lstrip().rstrip() == col("answer").lstrip().rstrip()
)

accuracy = df_eval.where(col("is_correct")).count_rows() / df_eval.count_rows()
print(f"Accuracy (with image): {accuracy:.1%}")
```

Let's look at some results:

```python
# Let's look at some results
df_eval.select("user", "image", "answer", col("result")["choice"].alias("predicted"), "is_correct").show(5)
```

## 5. Ablation Study

A simple accuracy score tells us *how often* the model is correct, but not *why*. To understand the contribution of image understanding, we'll conduct an **ablation study**—running the same prompts without images.

This lets us classify each example into four quadrants:

| Quadrant | With Image | Without Image | Interpretation |
|----------|------------|---------------|----------------|
| **Both Correct** | ✓ | ✓ | Question may be solvable from text alone |
| **Image Helped** | ✓ | ✗ | True image understanding |
| **Image Hurt** | ✗ | ✓ | Visual confusion |
| **Both Incorrect** | ✗ | ✗ | Hard question or model limitation |

Run the same inference without images:

```python
start = time.time()
df_ablation = df_eval.with_column(
    "result_no_image",
    prompt(
        messages=col("user"),
        model=MODEL_ID,
        use_chat_completions=True,
        return_format=ChoiceResponse,
    )
).with_column(
    "is_correct_no_image",
    col("result_no_image")["choice"].lstrip().rstrip() == col("answer").lstrip().rstrip()
).collect()
elapsed = time.time() - start

print(f"Processed {df_ablation.count_rows()} rows in {elapsed:.1f} seconds")
```

Compare accuracy:

```python
# Compare accuracy
accuracy_no_image = df_ablation.where(col("is_correct_no_image")).count_rows() / df_ablation.count_rows()

print(f"Accuracy with image:    {accuracy:.1%}")
print(f"Accuracy without image: {accuracy_no_image:.1%}")
print(f"Delta:                  {accuracy - accuracy_no_image:+.1%}")
```

Classify results into quadrants:

```python
from daft.functions import when, monotonically_increasing_id

# Classify into quadrants
df_classified = df_ablation.with_column(
    "id", monotonically_increasing_id()
).with_column(
    "quadrant",
    when((col("is_correct") == True) & (col("is_correct_no_image") == True), "Both Correct")
    .when((col("is_correct") == True) & (col("is_correct_no_image") == False), "Image Helped")
    .when((col("is_correct") == False) & (col("is_correct_no_image") == True), "Image Hurt")
    .otherwise("Both Incorrect")
)

# Show distribution
df_classified.groupby("quadrant").count().select("quadrant", col("id").alias("count")).show()
```

Inspect cases where the image helped:

```python
# Inspect cases where the image helped
df_classified.where(col("quadrant") == "Image Helped").select(
    "user", "image", "answer",
    col("result")["choice"].alias("with_image"),
    col("result_no_image")["choice"].alias("without_image")
).show(3)
```

Inspect cases where the image hurt:

```python
# Inspect cases where the image hurt
df_classified.where(col("quadrant") == "Image Hurt").select(
    "user", "image", "answer",
    col("result")["choice"].alias("with_image"),
    col("result_no_image")["choice"].alias("without_image")
).show(3)
```

Show breakdown by quadrant with percentages:

```python
# Show breakdown by quadrant with percentages
total_count = df_classified.count_rows()

df_results = df_classified.groupby("quadrant").count().select(
    "quadrant",
    col("id").alias("count")
).with_column(
    "percentage",
    (col("count") / daft.lit(total_count) * 100)
).collect()

df_results.show()
```

## 6. Scale with Daft Cloud

**Everything above runs locally on 50 rows.**

But The Cauldron contains **millions of rows across 50 subsets**. To run this evaluation at scale with strong consistent performance we can scale on [Daft Cloud](https://daft.ai/cloud). The python script version of this notebook is available in the [daft-examples](https://github.com/Eventual-Inc/daft-examples) repo in the [use_cases/image_understanding_eval](https://github.com/Eventual-Inc/daft-examples/tree/main/use_cases/image_understanding_eval) directory.

👉 [**Sign up for early access**](https://daft.ai/cloud) | [**Book a demo**](https://www.daft.ai/demo)

### Take this one step further with LLM-as-a-judge

For a daft cloud ready-to-run script with a bonus LLM-as-a-judge section, check out [`eval_image_understanding.py`](https://github.com/Eventual-Inc/daft-examples/blob/main/use_cases/image_understanding_eval/eval_image_understanding.py) in the [daft-examples](https://github.com/Eventual-Inc/daft-examples) repo.

## 7. Conclusion

In this notebook, we built a small pipeline to evaluate Qwen3-VL's image understanding:

1. **Structured Outputs**: Used Pydantic models to enforce consistent responses
2. **Ablation Study**: Isolated image understanding from general reasoning
3. **Quadrant Analysis**: Classified results into actionable categories

### Resources

- [Daft Documentation](https://docs.daft.ai)
- [Daft Cloud](https://daft.ai/cloud)
- [The Cauldron Dataset](https://huggingface.co/datasets/HuggingFaceM4/the_cauldron)
- [Qwen3-VL](https://github.com/QwenLM/Qwen3-VL)

**Canonical References:**
- [Getting Structured LLM Output (DeepLearning.ai)](https://learn.deeplearning.ai/courses/getting-structured-llm-output/information)
- [Judging LLM-as-a-Judge (NeurIPS 2023)](https://arxiv.org/abs/2306.05685)
