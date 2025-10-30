# AI Providers Overview

Providers in Daft give you flexible control over where and how your AI inference runs. Whether you're using commercial APIs like OpenAI, running local models with LM Studio, or leveraging open-source models from Hugging Face, providers offer a unified interface for managing authentication, routing, and configuration.

This guide covers:

- [Quick provider setup](#quick-provider-setup)
- [Setting named providers within sessions](#setting-a-named-openai-provider-within-a-session)
- [Working with multiple providers](#explicit-usage-with-multiple-providers)

!!! warning "Early Development"

    These APIs are early in their development. Please feel free to [open a feature request and file issues](https://github.com/Eventual-Inc/Daft/issues/new/choose) if you see opportunities for improvements. We're always looking for inputs from the community!

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

## Explicit Usage with Multiple Providers

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

## More Resources

- **[AI Functions Overview](../modalities/index.md)** - Learn about available AI functions and usage patterns
- **[Working with Text](../modalities/text.md)** - Text processing and embeddings
- **[Working with Images](../modalities/images.md)** - Image processing and embeddings
- **[Contributing AI Functions](../contributing/contributing-ai-functions.md)** - Add new providers or AI functions
