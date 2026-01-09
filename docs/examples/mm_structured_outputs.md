# Evaluating Image Understanding at Scale with Structured Outputs

*An end-to-end example of **Multimodal Structured Outputs** with Daft and Qwen3-VL-8B*

## Introduction

We'll evaluate [Qwen3-VL](https://github.com/QwenLM/Qwen3-VL)'s image understanding using a multiple choice subset of HuggingFace's [The Cauldron dataset](https://huggingface.co/datasets/HuggingFaceM4/the_cauldron), a massive collection of 50 vision-language datasets.

Our pipeline will:

1. Run structured output inference on image+text prompts
2. Conduct an **ablation study** (with vs. without images) to isolate image understanding
3. Classify results into diagnostic quadrants
4. Use **VLM-as-a-Judge** to explain failures

Check out the [blog post](https://www.daft.ai/blog/multimodal-structured-outputs-evaluating-vlm-image-understanding-at-scale) where we use the [production-ready version](https://github.com/Eventual-Inc/daft-examples/blob/main/use_cases/image_understanding_eval/eval_image_understanding.py) of this pipeline to evaluate Qwen3-VL-4B on 20k rows across 3 datasets.

## About this Tutorial

This tutorial demonstrates the core evaluation pipeline on a small sample (50 rows) so you can inspect examples and understand the methodology. For a production-ready implementation that scales to millions of rows, see [`eval_image_understanding.py`](https://github.com/Eventual-Inc/daft-examples/blob/main/use_cases/image_understanding_eval/eval_image_understanding.py) in the [daft-examples](https://github.com/Eventual-Inc/daft-examples) repo.

### Table of Contents

1. [Setup](#1-setup)
2. [Data Loading](#2-data-loading)
3. [Preprocessing](#3-preprocessing)
4. [Structured Outputs with `prompt`](#4-structured-outputs-with-prompt)
5. [Ablation Study](#5-ablation-study)
6. [VLM-as-a-Judge](#6-vlm-as-a-judge)
7. [Scale with Daft Cloud](#7-scale-with-daft-cloud)
8. [Conclusion](#8-conclusion)

## 1. Setup

First, install the required dependencies:

```bash
pip install daft openai numpy pillow python-dotenv pydantic
```

Next, create a `.env` file in your project directory and add your HuggingFace token:

```bash
# .env
HF_TOKEN=your_huggingface_token_here
```

You can get a HuggingFace token from [https://huggingface.co/settings/tokens](https://huggingface.co/settings/tokens).

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

The dataset contains nested structures with columns:
- `images`: List of image bytes
- `texts`: List of conversation turns with `user` (question) and `assistant` (answer) fields
- Additional metadata fields

Each row represents a multiple-choice question with an accompanying science diagram.

## 3. Preprocessing

We need to:

1. Decode images into Daft's Image type
2. Extract the question, choices, and correct answer from the text

```python
from daft import col
from daft.functions import unnest

df_img = df_raw.explode(col("images"))
df_img = df_img.with_column("image", col("images")["bytes"].decode_image())

df_text = df_img.explode(col("texts")).select(unnest(col("texts")), "image")

df_prep = df_text.with_column(
    "answer",
    col("assistant").regexp_replace("Answer: ", "").lstrip().rstrip()
).collect()

df_prep.show(3)
```

## 4. Structured Outputs with `prompt`

Daft's `prompt` function scales OpenAI-compatible calls across dataframes. We'll use a Pydantic model to enforce structured output.

For more info: [API docs](https://docs.daft.ai/en/stable/api/functions/prompt/) | [User Guide](https://docs.daft.ai/en/stable/ai-functions/prompt/)

```python
from daft.functions import prompt
from pydantic import BaseModel, Field
import time

PARAMS = {"temperature": 0.0, "max_tokens": 2}

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
        **PARAMS,
    )
).limit(LIMIT).collect()
elapsed = time.time() - start

print(f"Processed {df_results.count_rows()} rows in {elapsed:.1f} seconds")
```

```python
df_eval = df_results.with_column(
    "is_correct",
    col("result")["choice"].lstrip().rstrip() == col("answer").lstrip().rstrip()
)

accuracy = df_eval.where(col("is_correct")).count_rows() / df_eval.count_rows()
print(f"Accuracy (with image): {accuracy:.1%}")

df_eval.select("user", "image", "answer", col("result")["choice"].alias("predicted"), "is_correct").show(5)
```

## 5. Ablation Study

A simple accuracy score tells us *how often* the model is correct, but not *why*. Our [full evaluation](https://www.daft.ai/blog/multimodal-structured-outputs-evaluating-vlm-image-understanding-at-scale) found that ~70% of correct answers on image understanding benchmarks don't actually require the image. To understand the true contribution of image understanding, we conduct an **ablation study**—running the same prompts without images.

This lets us classify each example into four quadrants:

| Quadrant | With Image | Without Image | Interpretation |
|----------|------------|---------------|----------------|
| **Both Correct** | ✓ | ✓ | Question may be solvable from text alone |
| **Image Helped** | ✓ | ✗ | True image understanding |
| **Image Hurt** | ✗ | ✓ | Visual confusion |
| **Both Incorrect** | ✗ | ✗ | Hard question or model limitation |

```python
SYSTEM_PROMPT_NO_IMAGE = "Respond to the multiple choice question with just the letter corresponding to the correct answer."

start = time.time()
df_ablation = df_eval.with_column(
    "result_no_image",
    prompt(
        messages=col("user"),
        system_message=SYSTEM_PROMPT_NO_IMAGE,
        model=MODEL_ID,
        use_chat_completions=True,
        return_format=ChoiceResponse,
        **PARAMS,
    )
).with_column(
    "is_correct_no_image",
    col("result_no_image")["choice"].lstrip().rstrip() == col("answer").lstrip().rstrip()
).collect()
elapsed = time.time() - start

print(f"Processed {df_ablation.count_rows()} rows in {elapsed:.1f} seconds")

accuracy_no_image = df_ablation.where(col("is_correct_no_image")).count_rows() / df_ablation.count_rows()

print(f"Accuracy with image:    {accuracy:.1%}")
print(f"Accuracy without image: {accuracy_no_image:.1%}")
print(f"Delta:                  {accuracy - accuracy_no_image:+.1%}")
```

```python
from daft.functions import when, monotonically_increasing_id

df_classified = df_ablation.with_column(
    "id", monotonically_increasing_id()
).with_column(
    "quadrant",
    when((col("is_correct") == True) & (col("is_correct_no_image") == True), "Both Correct")
    .when((col("is_correct") == True) & (col("is_correct_no_image") == False), "Image Helped")
    .when((col("is_correct") == False) & (col("is_correct_no_image") == True), "Image Hurt")
    .otherwise("Both Incorrect")
)

df_classified.groupby("quadrant").count().select("quadrant", col("id").alias("count")).show()
```

Inspect cases where the image helped:

```python
df_classified.where(col("quadrant") == "Image Helped").select(
    "user", "image", "answer",
    col("result")["choice"].alias("with_image"),
    col("result_no_image")["choice"].alias("without_image")
).show(3)
```

```python
df_classified.where(col("quadrant") == "Image Hurt").select(
    "user", "image", "answer",
    col("result")["choice"].alias("with_image"),
    col("result_no_image")["choice"].alias("without_image")
).show(3)
```

```python
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

## 6. VLM-as-a-Judge

We can go beyond pass/fail metrics by using **VLM-as-a-Judge** to explain *why* the model failed—especially on the most informative failure subsets:
- **Image Hurt**: correct without the image, incorrect with the image
- **Both Incorrect**: incorrect with and without the image

We'll use a structured output schema so the judge reliably returns fields we can analyze.

```python
from daft.functions import format

JUDGE_SYSTEM_PROMPT = """
You are an impartial judge reviewing the results of a textbook academic questions multiple choice benchmark.
Inspect the attached image and provide high-signal feedback on why the model chose its answer.
First, reason about the model's answer with the image and the model's answer without the image.
Second, develop a hypothesis for why the model made the choice it did.
Third, attribute the failure to a 'question' issue or an 'image' understanding issue.
Finally, assign whether the model's answer with the image is correct and whether the model's answer without the image is correct.
"""


class JudgeResponse(BaseModel):
    """Structured diagnostic feedback from the VLM judge."""

    reasoning: str = Field(..., description="Why did the model choose the answer it did?")
    hypothesis: str = Field(..., description="What caused the divergence from the correct answer?")
    attribution: str = Field(
        ...,
        description="Was this a 'question' issue or an 'image' understanding issue or 'other'?",
    )
```

```python
judge_template = format(
    """Given the image attached and the multiple choice question of <question>{}</question>,
The model chose the following prediction <model_answer>{}</model_answer> and without the image, the model chose the following prediction <no_image_model_answer>{}</no_image_model_answer>, but the correct answer is <correct_answer>{}</correct_answer>.

Provide diagnostic feedback.
""",
    col("user"),
    col("result")["choice"],
    col("result_no_image")["choice"],
    col("answer"),
)

df_failures = df_classified.where(
    (col("quadrant") == "Image Hurt") | (col("quadrant") == "Both Incorrect")
)

JUDGE_PARAMS = {"temperature": 0.0, "max_tokens": 512}

df_judged = df_failures.with_column(
    "judge_response",
    prompt(
        messages=[col("image"), judge_template],
        system_message=JUDGE_SYSTEM_PROMPT,
        model=MODEL_ID,
        use_chat_completions=True,
        return_format=JudgeResponse,
        **JUDGE_PARAMS,
    ),
).collect()

print(f"Judged {df_judged.count_rows()} failure rows")
```

The judge's `attribution` field helps separate **question issues** (ambiguous prompts) from **image understanding issues** (missed labels, visual ambiguity).

```python
df_judged.select(
    "quadrant",
    "user",
    "image",
    "answer",
    col("result")["choice"].alias("with_image"),
    col("result_no_image")["choice"].alias("without_image"),
    unnest(col("judge_response")),
).show(3)
```

Verify the full pipeline ran:

```python
print(f"Accuracy (with image):    {accuracy:.1%}")
print(f"Accuracy (without image): {accuracy_no_image:.1%}")
print(f"Delta:                   {accuracy - accuracy_no_image:+.1%}")

df_classified.groupby("quadrant").count().show()

print(f"Judge rows: {df_judged.count_rows()}")
```

## 7. Scale with Daft Cloud

This tutorial runs locally on 50 rows. The Cauldron contains **millions of rows across 50 subsets**. To run this evaluation at scale, use [Daft Cloud](https://daft.ai/cloud).

The production-ready script [`eval_image_understanding.py`](https://github.com/Eventual-Inc/daft-examples/blob/main/use_cases/image_understanding_eval/eval_image_understanding.py) includes:
- Multi-dataset evaluation across all Cauldron subsets
- Configurable batch processing
- Result aggregation and export

👉 [**Sign up for early access**](https://daft.ai/cloud) | [**Book a demo**](https://www.daft.ai/demo)

## 8. Conclusion

In this tutorial, we built a small pipeline to evaluate Qwen3-VL's image understanding:

1. **Structured Outputs**: Used Pydantic models to enforce consistent responses
2. **Ablation Study**: Isolated image understanding from general reasoning
3. **Quadrant Analysis**: Classified results into actionable categories
4. **VLM-as-a-Judge**: Diagnosed failures on the most informative subsets ("Image Hurt" + "Both Incorrect")

### Next Steps

**Multi-Dataset Evaluation**: The production pipeline supports evaluating across all 50 Cauldron subsets. See [`eval_image_understanding.py`](https://github.com/Eventual-Inc/daft-examples/blob/main/use_cases/image_understanding_eval/eval_image_understanding.py) for the full implementation with dataset iteration, batch processing, and result aggregation.

**Experiment Tracking**: Wire judge feedback into MLflow or W&B to track improvements over time.

**RLVR Training**: Use the `is_correct` signal and judge attributions for reinforcement learning with verifiable rewards.

### Resources

- [Daft Documentation](https://docs.daft.ai)
- [Daft Cloud](https://daft.ai/cloud)
- [The Cauldron Dataset](https://huggingface.co/datasets/HuggingFaceM4/the_cauldron)
- [Qwen3-VL](https://github.com/QwenLM/Qwen3-VL)

**Canonical References:**
- [Getting Structured LLM Output (DeepLearning.ai)](https://learn.deeplearning.ai/courses/getting-structured-llm-output/information)
- [Judging LLM-as-a-Judge (NeurIPS 2023)](https://arxiv.org/abs/2306.05685)
