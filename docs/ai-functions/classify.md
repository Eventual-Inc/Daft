# Text and Image Classification with Daft

Daft provides powerful classification functions for both text and images, making it easy to label and categorize your data at scale. Whether you're working with sentiment analysis, content categorization, or visual recognition tasks, Daft's classification functions integrate seamlessly with popular models from Hugging Face and other providers.

!!! tip "Choosing the Right Approach"
    - Use `classify_text` and `classify_image` for fast, efficient classification with pre-defined labels
    - Use `prompt` for more flexible classification that requires explanations or complex reasoning
    - For custom models, Daft supports both Hugging Face Transformers and OpenAI-compatible providers

## Text Classification

The `classify_text` function provides fast, efficient text classification using zero-shot models or fine-tuned classifiers. It's ideal for tasks like sentiment analysis, topic categorization, and intent detection where you have predefined labels.

```python
import daft
from daft.functions import classify_text

df = daft.from_pydict({
    "text": [
        "One day I will see the world",
        "I've always enjoyed preparing dinner for my family",
    ],
})

df = df.with_column(
    "label",
    classify_text(
        daft.col("text"),
        labels=['travel', 'cooking', 'dancing'],
        provider="transformers",
        model="facebook/bart-large-mnli",
        multi_label=True,
    ),
)

df.show()
```

```{title="Output"}
╭────────────────────────────────┬─────────╮
│ text                           ┆ label   │
│ ---                            ┆ ---     │
│ String                         ┆ String  │
╞════════════════════════════════╪═════════╡
│ One day I will see the world   ┆ travel  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┤
│ I've always enjoyed preparing… ┆ cooking │
╰────────────────────────────────┴─────────╯

(Showing first 2 of 2 rows)
```

## Image Classification

The `classify_image` function uses vision models like CLIP for zero-shot image classification. This example shows a complete pipeline including image preprocessing steps:

```python
import daft
from daft.functions import classify_image, decode_image, convert_image, resize

df = (
    # Discover a few images from HuggingFace
    daft.from_glob_path("hf://datasets/datasets-examples/doc-image-3/images")
    # Read the 4 PNG, JPEG, TIFF, WEBP Images
    .with_column("image_bytes", daft.col("path").url.download())
    # Decode the image bytes into a daft Image DataType
    .with_column("image_type", decode_image(daft.col("image_bytes")))
    # Convert Image to RGB and resize the image to 224x224
    .with_column(
        "image_resized", resize(convert_image(daft.col("image_type"), "RGB"), 224, 224)
    )
    # Classify the image
    .with_column(
        "image_label",
        classify_image(
            daft.col("image_resized"),
            provider="transformers",
            labels=["bulbasaur", "catapie", "voltorb", "electrode"],
            model="openai/clip-vit-base-patch32",
        ),
    )
)

df.show()
```

```{title="Output"}
╭────────────────────────────────┬─────────┬──────────┬────────────────────────────────┬──────────────┬───────────────────────┬─────────────╮
│ path                           ┆ size    ┆ num_rows ┆ image_bytes                    ┆ image_type   ┆ image_resized         ┆ image_label │
│ ---                            ┆ ---     ┆ ---      ┆ ---                            ┆ ---          ┆ ---                   ┆ ---         │
│ String                         ┆ Int64   ┆ Int64    ┆ Binary                         ┆ Image[MIXED] ┆ Image[RGB; 224 x 224] ┆ String      │
╞════════════════════════════════╪═════════╪══════════╪════════════════════════════════╪══════════════╪═══════════════════════╪═════════════╡
│ hf://datasets/datasets-exampl… ┆ 113469  ┆ None     ┆ b"\xff\xd8\xff\xe0\x00\x10JFI… ┆ <Image>      ┆ <FixedShapeImage>     ┆ bulbasaur   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/datasets-exampl… ┆ 206898  ┆ None     ┆ b"\x89PNG\r\n\x1a\n\x00\x00\x… ┆ <Image>      ┆ <FixedShapeImage>     ┆ voltorb     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/datasets-exampl… ┆ 1871034 ┆ None     ┆ b"MM\x00*\x00\x1c\x7f4\xff\xf… ┆ <Image>      ┆ <FixedShapeImage>     ┆ voltorb     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/datasets-exampl… ┆ 22022   ┆ None     ┆ b"RIFF\xfeU\x00\x00WEBPVP8 \x… ┆ <Image>      ┆ <FixedShapeImage>     ┆ electrode   │
╰────────────────────────────────┴─────────┴──────────┴────────────────────────────────┴──────────────┴───────────────────────┴─────────────╯

(Showing first 4 of 4 rows)
```

!!! note "When to Use Prompts for Classification"
    Use `classify_image` and `classify_text` when you have a fixed set of labels and need fast, efficient classification. Use `prompt` when you need:

    - Explanations for classification decisions
    - Complex reasoning or multi-step logic
    - Dynamic or unstructured label sets
    - Additional context beyond a simple label

## Flexible Classification with Prompts

When you need more flexibility—such as generating explanations, handling complex reasoning, or working with unstructured labels—use the `prompt` function. This approach trades some speed for significantly more capability.

```python
import os
import daft
from daft.functions import prompt

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

```{title="Output"}
╭───────────────────────────────────────────┬──────────────────────────────────────────────────────────────╮
│ quote                                     ┆ response                                                     │
╞═══════════════════════════════════════════╪══════════════════════════════════════════════════════════════╡
│ I am going to be the king of the pirates! ┆ **Show:** *One Piece*                                        │
│                                           ┆ **Character Name:** Monkey D. Luffy                          │
│                                           ┆ **Explanation:**                                             │
│                                           ┆ The quote "I am going to be the king…                        │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ I'm going to be the next Hokage!          ┆ **Show:** *Naruto*                                           │
│                                           ┆ **Character Name:** Naruto Uzumaki                           │
│                                           ┆ **Explanation:** The quote "I'm going to be the next Hokage… │
╰───────────────────────────────────────────┴──────────────────────────────────────────────────────────────╯
(Showing first 2 of 2 rows)
```
