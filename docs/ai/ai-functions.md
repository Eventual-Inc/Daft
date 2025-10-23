# AI Functions Overview

Daft is purpose-built for scaling Multimodal AI workloads. We are rapidly expanding support for new [AI Functions](/api/functions/\#ai-functions) covering a variety of inference tasks.


<div class="grid cards" markdown>

- [**prompt**](/en/stable/api/functions/prompt/)

    Generate text completions and structured outputs using language models with customizable prompts and system messages.

- [**classify_text**](../api/functions/classify_text/)

    Classify text against labels using open-source models

- [**embed_image**](../api/functions/classify_text/)

    Generate vector embeddings from images for similarity search, clustering, and other machine learning tasks.

- [**embed_text**](../api/functions/classify_text/)

    Generate vector embeddings from text for semantic search, similarity matching, and retrieval-augmented generation (RAG) applications.

</div>

!!! warning "Warning"

    These APIs are early in their development. Please feel free to [open feature requests and file issues](https://github.com/Eventual-Inc/Daft/issues/new/choose). We'd love hear want you would like, thank you! 🤘


If you would like to contribute a new AI function or would like to expand the list of supported providers, take a look at [Contributing New AI Functions](contributing-new-ai-functions.md). For more detailed information and usage patterns on using the [Providers API](../api/ai.md), see [AI Providers Overview](ai-providers.md).

## Canonical Usage

The canonical means of working with AI Model Functions and Providers is as follows:

```python
import os
from dotenv import load_dotenv

import daft
from daft.ai.openai.provider import OpenAIProvider
from daft.functions.ai import prompt
from daft.session import Session

load_dotenv()

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
            system_message="You are an anime expert. Classify the anime based on the text and returns the name, character, and quote.",
            provider=sess.get_provider("OpenRouter"),
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

## AI Functions Usage Patterns



### Embedding Text with an implicit provider

Setting a provider is not strictly necessary. For example, if already have `OPENAI_API_KEY` environment variable set, simply specifying `"openai"` will automatically add an `OpenAIProvider` to the global [session](../sessions.md).

```python
import daft
from daft.functions import embed_text

# Grab some Text Content
df = daft.from_pydict({"text": ["Hello World"]})

# Embed Text with Defaults
df = df.with_column(
    "embeddings",
    embed_text(
        daft.col("image"),
        provider="openai",
        model="text-embedding-3-small"
    )
)

# Display the Results
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

### Embedding Images

Daft Natively supports images with it's [Image DataType](../modalities/images.md). Working with images can be tricky, but daft simpilfies multimodal preprocessing with handy built-in utilities like `decode_image`, `convert_image`, and `resize`. In this example, we read 4 different image formats from HuggingFace, download their contents to bytes, and then prepare the image for inference. We'll leverage Apple's [apple/aimv2-large-patch14-224-lit](https://huggingface.co/apple/aimv2-large-patch14-224-lit) for state-of-the-art image understanding.

```python
import daft
from daft.functions import embed_image, decode_image

# Embed Text with Defaults
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

### Prompt a Large Language Model like gpt-5 with OpenAI

Next we will

```python
import os
import daft
from daft.ai.openai.provider import OpenAIProvider
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
            system_message="You are an anime expert. Classify the anime based on the text and returns the name, character, and quote.",
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

### Structured Outputs with Custom OpenAI Provider

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
        "nemotron-response",
        prompt(
            daft.col("quote"),
            system_message="Classify the anime from the quote and return the show, character name, and explanation.",
            return_format=Anime,
            provider=sess.get_provider("OpenRouter"),
            model="nvidia/nemotron-nano-9b-v2:free"
        )
    )
    .select("quote", unnest(daft.col("nemotron-response")))
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
