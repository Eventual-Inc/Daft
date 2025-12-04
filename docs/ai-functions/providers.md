# AI Providers Overview

Providers in Daft give you flexible control over where and how your AI inference runs. Whether you're using commercial APIs like OpenAI, running local models with LM Studio, or leveraging open-source models from Hugging Face, providers offer a unified interface for managing authentication, routing, and configuration.

This guide covers:

- [Quick provider setup](#quick-provider-setup)
- [Setting named providers within sessions](#setting-a-named-openai-provider-within-a-session)
- [Working with multiple providers](#end-to-end-usage-with-multiple-providers)
- [Prompting with OpenAI-Compatible Providers](#prompting-with-openai-compatible-providers)
- [Prompting with vLLM Online Serving](#prompting-with-vllm-online-serving)

## Quick provider setup

The simplest way to use a provider is with `daft.set_provider()`, which configures a provider globally for your Daft session:

```python
import os
import daft

# Set OpenAI Provider for Global Daft Session
daft.set_provider(
    "openai",
    api_key=os.environ.get("OPENAI_API_KEY"),
    timeout=30.0,
    max_retries=3
)

# Retrieve Provider for an AI Function
provider = daft.get_provider("openai")
```

## Supported Providers

Daft supports the following [AI Providers](../api/ai.md#providers):

- **[OpenAI](https://platform.openai.com/docs/api-reference/introduction)** - State-of-the-art AI models for text generation, natural language processing, computer vision, and more.
- **[Transformers](https://huggingface.co/docs/transformers/index)** - Hugging Face's model framework for machine learning models in text, computer vision, audio, video, and multimodal domains.
- **[Google](https://ai.google.dev/gemini-api/docs)** - Google's Gemini API for text generation, natural language processing, computer vision, and more.
- **[LM Studio](https://lmstudio.ai/)** - Run local AI models like gpt-oss, Qwen, Gemma, DeepSeek and many more on your computer, privately and for free.

## Setting a named OpenAI Provider within a Session

For more control, you can create named providers within a session. This is useful when working with OpenAI-compatible APIs like OpenRouter, or when you need to switch between different configurations.

```python
import os
import daft
from daft.ai.openai.provider import OpenAIProvider

sess = daft.Session()
openrouter_provider = OpenAIProvider(
    name="OpenRouter",
    base_url="https://openrouter.ai/api/v1",
    api_key=os.environ.get("OPENROUTER_API_KEY")
)
sess.attach_provider(openrouter_provider)
sess.set_provider("OpenRouter")

# Retrieve Provider for an AI Function
provider = sess.get_provider("OpenRouter")
```

You can then specify the `name` of the provider in the

## Using the Google Provider

The Google provider enables you to use Google's Gemini models for text generation, multimodal processing, and structured outputs.

```python
import daft
import os

# Set up the Google provider with your API key
with daft.session() as session:
    session.set_provider("google", api_key=os.environ["GOOGLE_API_KEY"])

    # Create a DataFrame with questions
    df = daft.from_pydict({
        "question": [
            "What is the capital of France?",
            "Explain quantum computing in simple terms.",
            "What are the benefits of fusion energy?"
        ]
    })

    # Use the prompt function with Google's Gemini model
    df = df.with_column(
        "answer",
        daft.functions.prompt(
            daft.col("question"),
            provider="google",
            model="gemini-2.5-flash"  # or "gemini-3-pro-preview"
        )
    )

    df.show()
```

The Google provider supports all the same features as other providers:
- **Structured outputs** with Pydantic models
- **Multimodal inputs** (text, images, PDFs, documents)
- **System messages** for custom instructions
- **Custom generation config** (temperature, max_output_tokens, etc.)

## End-to-End Usage with Multiple Providers

For complex workflows, you might need to use different providers for different tasks—for example, using GPT-5 for validation while using a cheaper model for initial classification. Daft makes it easy to manage multiple providers in a single session.

```python
import os
from dotenv import load_dotenv
import daft
from daft.ai.openai.provider import OpenAIProvider
from daft.functions.ai import prompt
from daft.functions import unnest
from daft.session import Session
from pydantic import BaseModel, Field

# Load environment variables
load_dotenv()

class Anime(BaseModel):
    show: str = Field(description="The name of the anime show")
    character: str = Field(description="The name of the character who says the quote")
    explanation: str = Field(description="Why the character says the quote")


openai_provider = OpenAIProvider(
    name="OAI_DEV",
    api_key=os.environ.get("OPENAI_API_KEY_DEV")
)

openrouter_provider = OpenAIProvider(
    name="OpenRouter",
    base_url="https://openrouter.ai/api/v1",
    api_key=os.environ.get("OPENROUTER_API_KEY")
)

# Create a session and attach the provider
sess = Session()
sess.attach_provider(openai_provider)
sess.attach_provider(openrouter_provider)

# Set OpenRouter as Default
sess.set_provider("OpenRouter")

# Create a dataframe with the quotes
df = daft.from_pydict({
    "quote": [
        "I am going to be the king of the pirates!",
        "I'm going to be the next Hokage!",
    ],
})

# Use the prompt function to classify the quotes
df = (
    df
    .with_column(
        "nemotron-response",
        prompt(
            daft.col("quote"),
            system_message="You are an anime expert. Classify the anime based on the text and returns the name, character, and quote.",
            return_format=Anime,
            provider=sess.get_provider("OpenRouter"),
            model="nvidia/nemotron-nano-9b-v2:free"
        )
    )
    .select("quote", unnest(daft.col("nemotron-response")))
    .with_column(
        "gpt-5-response",
        prompt(
            format("""Does the quote "{}" match the assigned anime series name {} and attributed character {}""", daft.col("quote"), daft.col("show"), daft.col("character")),
            system_message="Validate whether the user prompt is correct or not.",
            return_format=Anime,
            provider=sess.get_provider("OAI_DEV"),
            model="gpt-5"
        )
    )
)

df.show(format="fancy", max_width=120)
```

This example demonstrates:

- **Multiple API keys**: Different providers for development and production environments
- **Cost optimization**: Use cheaper models for initial processing, premium models for validation
- **Provider routing**: Explicitly specify which provider to use for each operation

!!! tip "When to Use Multiple Providers"

    Multiple providers are useful when you need to:

    - **Balance cost and quality**: Use cheaper models for bulk processing, premium models for critical tasks
    - **Ensure redundancy**: Fall back to alternative providers if one fails
    - **Compare model outputs**: Run the same prompt through different models for quality assessment
    - **Manage rate limits**: Distribute load across multiple API keys or providers

## Prompting with OpenAI-Compatible Providers

You can use OpenAI-compatible providers with the `prompt` function like OpenRouter, HuggingFace Inference Providers, Databricks, and more.

```python
import os
import daft

# For OpenRouter
# See: https://openrouter.ai/docs/quickstart#using-the-openai-sdk
daft.set_provider(
    "openai",
    base_url="https://openrouter.ai/api/v1",
    api_key=os.getenv["OPENROUTER_API_KEY"]
)

# For HuggingFace Inference Providers
# See: https://huggingface.co/inference/get-started
daft.set_provider(
    "openai",
    base_url="https://router.huggingface.co/v1",
    api_key=os.getenv["HF_TOKEN"]
)

# For Databricks
# How to get your Databricks token: https://docs.databricks.com/en/dev-tools/auth/pat.html
# More info on model serving: https://docs.databricks.com/aws/en/machine-learning/model-serving/score-foundation-models
daft.set_provider(
    "openai",
    base_url="https://<workspace-id>.cloud.databricks.com/serving-endpoints",
    api_key=os.getenv["DATABRICKS_TOKEN"],
)

```

## Prompting with vLLM Online Serving

For vLLM Online Serving, you can set the provider like this. Make sure to set `use_chat_completions=True` in `prompt()` to use the Chat Completions API instead of the new Responses API.

```python
import os
import daft

# For vLLM Online Serving
daft.set_provider(
    "openai",
    api_key="none",
    base_url="http://localhost:8000/v1",
) # Make sure to set use_chat_completions=True in prompt()

df = df.with_column(
    "response",
    prompt(
        daft.col("input"),
        use_chat_completions=True,
        model="google/gemma-3-4b-it",
    )
)

df.show()
```

The following sample cli launch command is optimized for Google Colab's A100 High-RAM instance and Google's gemma-3-4b-it model.

```bash
python -m vllm.entrypoints.openai.api_server \
--model google/gemma-3-4b-it \
--enable-chunked-prefill \
--guided-decoding-backend guidance \
--dtype bfloat16 \
--gpu-memory-utilization 0.85 \
--host 0.0.0.0 --port 8000
```

* Server readiness may take ~7–8 minutes
* For vLLM online serving, set `api_key = "none"` and `base_url = "http://0.0.0.0:8000/v1"`
* `guided-decoding-backend guidance` is required for structured outputs.


## More Resources

- **[AI Functions Overview](overview.md)**
- **[Embedding Text and Images](embed.md)**
- **[Classify Text and Images](classify.md)**
- **[Contributing AI Functions](../contributing/contributing-ai-functions.md)**
