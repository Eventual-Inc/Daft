# AI Functions Overview

Daft is purpose-built for scaling Multimodal AI workloads. AI Functions provide a unified interface for running inference tasks across different models and providers. Whether you're generating embeddings, classifying content, or prompting large language models.

This guide shows you common usage patterns for AI functions in Daft:

- [Quickstart: Prompt with OpenAI-compatible providers](#quickstart-prompt-with-openai-compatible-providers)
- [Generate text embeddings](#embedding-text-with-an-implicit-provider)
- [Generate image embeddings](#embedding-images)
- [Use OpenAI models directly](#prompt-gpt-5-with-openai)
- [Get structured outputs with Pydantic](#structured-outputs-with-custom-openai-provider)
- [Classify text with Transformers](#classify-text-with-transformers)

!!! warning "Early Development"

    These APIs are early in their development. Please feel free to [open a feature request and file issues](https://github.com/Eventual-Inc/Daft/issues/new/choose) if you see opportunities for improvements. We're always looking for inputs from the community!

## Available AI Functions

The current list of AI functions includes:

- `prompt` - Generate text completions from language models
- `embed_text` - Create vector embeddings from text
- `embed_image` - Create vector embeddings from images
- `classify_text` - Zero-shot text classification
- `classify_image` - Zero-shot image classification

For more detailed information on the [Providers API](../api/ai.md), see [AI Providers Overview](providers.md). If you'd like to contribute a new AI function or expand provider support, check out [Contributing New AI Functions](../contributing/contributing-ai-functions.md).

## Quickstart: Prompt with OpenAI-compatible providers

This example shows how to use the `prompt` function with an OpenAI-compatible provider like OpenRouter to classify anime quotes.

```python
import os
import daft
from daft.functions.ai import prompt

daft.set_provider(
    "openai",
    base_url="https://openrouter.ai/api/v1",
    api_key=os.environ.get("OPENROUTER_API_KEY")
)

df = daft.from_pydict({
    "quote": [
        "I am going to be the king of the pirates!",
        "I'm going to be the next Hokage!",
    ],
})

df = (
    df
    .with_column(
        "response",
        prompt(
            daft.col("quote"),
            system_message="Classify the anime from the quote and return the show, character name, and explanation.",
            provider="openai",
            model="nvidia/nemotron-nano-9b-v2:free"
        )
    )
)

df.show(format="fancy", max_width=120)
```

```
╭───────────────────────────────────────────┬─────────────────────────────────────────────────────────╮
│ quote                                     ┆ response                                                │
╞═══════════════════════════════════════════╪═════════════════════════════════════════════════════════╡
│ I am going to be the king of the pirates! ┆ **Anime Name:** *One Piece*                             │
│                                           ┆ **Character:** Monkey D. Luffy                          │
│                                           ┆ **Quote:** "I am going to be the king of the pirates!"… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ I'm going to be the next Hokage!          ┆ **Name:** Naruto                                        │
│                                           ┆ **Character:** Naruto Uzumaki                           │
│                                           ┆ **Quote:** *"I'm going to be the next Hokage!"*         │
│                                           ┆                                                         │
│                                           ┆ This quote refl…                                        │
╰───────────────────────────────────────────┴─────────────────────────────────────────────────────────╯
```

## Embedding Text with an implicit provider

You can use AI functions without explicitly configuring a provider. If you already have an `OPENAI_API_KEY` environment variable set, simply specifying `provider="openai"` will automatically create an `OpenAIProvider` for the global session.

```python
import daft
from daft.functions import embed_text

df = daft.from_pydict({"text": ["Hello World"]})

df = df.with_column(
    "embeddings",
    embed_text(
        daft.col("text"),
        provider="openai",
        model="text-embedding-3-small"
    )
)

df.show()
```

```
╭─────────────┬──────────────────────────╮
│ text        ┆ embeddings               │
│ ---         ┆ ---                      │
│ String      ┆ Embedding[Float32; 1536] │
╞═════════════╪══════════════════════════╡
│ Hello World ┆ ▆█▆▆▆▃▆▆▂▄▃▂▃▃▄▁▃▅▂▃▂▂▂▂ │
╰─────────────┴──────────────────────────╯
```

## Embedding Images

Daft natively supports images with the [Image DataType](../modalities/images.md). Multimodal preprocessing that's typically complex becomes simple with built-in utilities like `decode_image`, `convert_image`, and `resize`.

This example demonstrates reading multiple image formats (PNG, JPEG, TIFF, WEBP) from HuggingFace, preprocessing them, and generating embeddings using Apple's state-of-the-art [aimv2-large-patch14-224-lit](https://huggingface.co/apple/aimv2-large-patch14-224-lit) model.

```python
import daft
from daft.functions import embed_image, decode_image

df = (
    # Discover a few images from HuggingFace
    daft.from_glob_path("hf://datasets/datasets-examples/doc-image-3/images")

    # Read the 4 PNG, JPEG, TIFF, WEBP Images
    .with_column("image_bytes", daft.col("path").url.download())

    # Decode the image bytes into a daft Image DataType
    .with_column("image_type", decode_image(daft.col("image_bytes")))

    # Convert Image to RGB and resize the image to 288x288
    .with_column("image_resized", daft.col("image_type").convert_image("RGB").resize(288, 288))

    # Embed the image
    .with_column(
        "image_embeddings",
        embed_image(
            daft.col("image_resized"),
            provider="transformers",
            model="apple/aimv2-large-patch14-224-lit"
        )
    )
)

# Show the dataframe
df.show()
```

```
╭────────────────────────────────┬─────────┬────────────────────────────────┬──────────────┬───────────────────────┬──────────────────────────╮
│ path                           ┆ size    ┆ image_bytes                    ┆ image_type   ┆ image_resized         ┆ image_embeddings         │
│ ---                            ┆ ---     ┆ ---                            ┆ ---          ┆ ---                   ┆ ---                      │
│ String                         ┆ Int64   ┆ Binary                         ┆ Image[MIXED] ┆ Image[RGB; 288 x 288] ┆ Embedding[Float32; 768]  │
╞════════════════════════════════╪═════════╪════════════════════════════════╪══════════════╪═══════════════════════╪══════════════════════════╡
│ hf://datasets/datasets-exampl… ┆ 113469  ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… ┆ <Image>      ┆ <FixedShapeImage>     ┆ ▃▅▅▆▆▂▅▆▅▇█▂▂▄▅▂▆▃▃▅▁▇▃▅ │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/datasets-exampl… ┆ 206898  ┆ b"\x89PNG\r\n\x1a\n\x00\x00\x… ┆ <Image>      ┆ <FixedShapeImage>     ┆ ▃▃▄▆▄▅▃▄▅▅▅▃▂▇▁▁▁▂▃▅▄█▃▅ │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/datasets-exampl… ┆ 1871034 ┆ b"MM\x00*\x00\x1c\x7f4\xff\xf… ┆ <Image>      ┆ <FixedShapeImage>     ┆ ▂▃▃▃▄▄▃▆▆▄▅▂▁▃▁▄▃▅▄▄▂█▆▆ │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/datasets-exampl… ┆ 22022   ┆ b"RIFF\xfeU\x00\x00WEBPVP8 \x… ┆ <Image>      ┆ <FixedShapeImage>     ┆ ▄▂▂▅▆▆▅▇▆▄▅▆▃▅▅▁▃▄▄▄▃█▃▆ │
╰────────────────────────────────┴─────────┴────────────────────────────────┴──────────────┴───────────────────────┴──────────────────────────╯

```

!!! tip "Image Preprocessing"

    When working with images for embedding, it's important to preprocess them correctly. Different models expect different input formats—some require RGB, others accept RGBA. Check your model's documentation for specific requirements regarding image format, size, and normalization.

## Prompt GPT-5 with OpenAI

Use OpenAI's latest models directly by specifying the `openai` provider and your desired model. Make sure your `OPENAI_API_KEY` environment variable is set.

```python
import os
import daft
from daft.functions.ai import prompt

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
        "response",
        prompt(
            daft.col("quote"),
            system_message="Classify the anime from the quote and return the show, character name, and explanation.",
            provider="openai", # Make sure OPENAI_API_KEY is set
            model="gpt-5-nano"
        )
    )
)

df.show(format="fancy", max_width=120)
```

```
╭───────────────────────────────────────────┬─────────────────────────────────────────────────────────╮
│ quote                                     ┆ response                                                │
╞═══════════════════════════════════════════╪═════════════════════════════════════════════════════════╡
│ I am going to be the king of the pirates! ┆ **Anime Name:** *One Piece*                             │
│                                           ┆ **Character:** Monkey D. Luffy                          │
│                                           ┆ **Quote:** "I am going to be the king of the pirates!"… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ I'm going to be the next Hokage!          ┆ **Name:** Naruto                                        │
│                                           ┆ **Character:** Naruto Uzumaki                           │
│                                           ┆ **Quote:** *"I'm going to be the next Hokage!"*         │
│                                           ┆                                                         │
│                                           ┆ This quote refl…                                        │
╰───────────────────────────────────────────┴─────────────────────────────────────────────────────────╯
```

## Structured Outputs with Custom OpenAI Provider

For production workloads, you often need structured, predictable outputs rather than free-form text. Use `return_format` with a Pydantic schema to automatically parse model responses into typed Python objects.

This example shows how to create a custom provider (OpenRouter) and enforce structured outputs.

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

# Create an OpenRouter provider
openrouter_provider = OpenAIProvider(
    name="OpenRouter",
    base_url="https://openrouter.ai/api/v1",
    api_key=os.environ.get("OPENROUTER_API_KEY")
)

# Create a session and attach the provider
sess = Session()
sess.attach_provider(openrouter_provider)
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
        "response",
        prompt(
            daft.col("quote"),
            system_message="Classify the anime from the quote and return the show, character name, and explanation.",
            return_format=Anime,
            provider=sess.get_provider("OpenRouter"),
            model="nvidia/nemotron-nano-9b-v2:free"
        )
    )
    .select("quote", unnest(daft.col("response")))
)

df.show(format="fancy", max_width=120)
```

```
╭───────────────────────────────────────────┬───────────┬─────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ quote                                     ┆ show      ┆ character       ┆ explanation                                                                                                            │
╞═══════════════════════════════════════════╪═══════════╪═════════════════╪════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╡
│ I am going to be the king of the pirates! ┆ One Piece ┆ Monkey D. Luffy ┆ Luffy famously states his dream of becoming the Pirate King throughout the series.                                     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ I'm going to be the next Hokage!          ┆ Naruto    ┆ Naruto Uzumaki  ┆ The phrase 'I'm going to be the next Hokage!' is a recurring aspiration in the *Naruto* series, particularly voiced b… │
╰───────────────────────────────────────────┴───────────┴─────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

!!! tip "Why Structured Outputs?"

    Structured outputs with Pydantic provide type safety, automatic validation, and easier integration with downstream systems. Instead of parsing free-form text with regex or custom logic, you get well-typed Python objects that can be directly inserted into databases, APIs, or other data pipelines.

## Classify Text with Transformers

Perform zero-shot text classification using Hugging Face Transformers without any training data. Simply provide candidate labels and Daft will use a pre-trained model to classify your text.

```python
import daft
from daft.functions import classify_text

df = daft.from_pydict({"text": ["Daft is wicked fast!"]})

df = df.with_column(
    "label",
    classify_text(
        daft.col("text"),
        labels=["Positive", "Negative"],
        provider="transformers",
        model="tabularisai/multilingual-sentiment-analysis"
    )
)

df.show()
```

```
╭─────────────────────┬───────────╮
│ text                ┆ label     │
│ ---                 ┆ ---       │
│ String              ┆ String    │
╞═════════════════════╪═══════════╡
│ Daft is wicked fast!┆ Positive  │
╰─────────────────────┴───────────╯
```

## More Resources

For deeper dives into specific AI function use cases:

- **[AI Providers Overview](providers.md)** - Learn how to configure and manage multiple providers
- **[Working with Text](../modalities/text.md)** - Text embeddings and processing workflows
- **[Working with Images](../modalities/images.md)** - Image embeddings and classification examples
- **[Contributing AI Functions](../contributing/contributing-ai-functions.md)** - Add new AI functions or providers
