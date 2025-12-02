# Product Classification with AI

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/examples%2Fproduct-classification/docs/notebooks/product-classification.ipynb)

This example demonstrates how to use Daft's built-in classification functions to automatically categorize products by analyzing both their descriptions and images. We'll use the same Amazon products dataset from the [Quickstart](../quickstart.md) to show how zero-shot classification can help organize product catalogs without any model training.

## Install Dependencies

```bash
pip install -U daft[all]
```

## Load the Data

We'll use an e-commerce dataset from Hugging Face containing Amazon products with names, descriptions, categories, and images.

```python
import daft

df = daft.read_huggingface("calmgoose/amazon-product-data-2020")

# Work with a small subset for quick experimentation
# Filter out rows with missing descriptions (required for text classification)
df = (
    df.select("Product Name", "About Product", "Image")
    .where(daft.col("About Product").not_null())
    .limit(10)
)
df.show(3)
```

## Text Classification

Daft's `classify_text` function uses zero-shot classification to categorize text without needing to train a model. It works by comparing text against a list of candidate labels and returning the most likely match.

### Classifying Product Descriptions by Sentiment

Let's analyze the sentiment of product descriptions to understand how customers might perceive them:

```python
from daft.functions import classify_text

df = df.with_column(
    "sentiment",
    classify_text(
        daft.col("About Product"),
        labels=["positive", "negative", "neutral"],
        provider="transformers",
        model="facebook/bart-large-mnli",
    ),
)

df.select("Product Name", "About Product", "sentiment").show(5)
```

### Classifying Products by Category

You can also classify products into custom categories. This is useful when product descriptions don't match existing category labels or when you want to create new categorization schemes:

```python
df = df.with_column(
    "product_type",
    classify_text(
        daft.col("About Product"),
        labels=["electronics", "toys", "sports", "home", "clothing", "books"],
        provider="transformers",
        model="facebook/bart-large-mnli",
    ),
)

df.select("Product Name", "product_type", "sentiment").show(5)
```

## Image Classification

For visual classification, Daft provides `classify_image` which uses CLIP-based models to match images against text labels. This requires downloading and preprocessing the images first.

### Prepare Images

```python
from daft.functions import classify_image, decode_image, convert_image, resize

# Extract the first image URL from the pipe-separated list
df = df.with_column(
    "image_url",
    daft.functions.regexp_extract(
        daft.col("Image"),
        r"^([^|]+)",
        1
    )
)

# Download and decode images
df = df.with_column(
    "image_bytes",
    daft.functions.download(daft.col("image_url"), on_error="null")
)

df = df.with_column(
    "image",
    decode_image(daft.col("image_bytes"), on_error="null")
)

# Resize for the CLIP model (224x224 is standard)
df = df.with_column(
    "image_resized",
    resize(convert_image(daft.col("image"), "RGB"), 224, 224)
)
```

### Classify Images by Visual Features

Now let's classify the product images based on visual characteristics:

```python
df = df.with_column(
    "visual_category",
    classify_image(
        daft.col("image_resized"),
        labels=["toy", "electronic device", "sports equipment", "furniture", "clothing"],
        provider="transformers",
        model="openai/clip-vit-base-patch32",
    ),
)

df.select("Product Name", "image", "visual_category").show(5)
```

## Combining Text and Image Classification

One powerful pattern is to use both text and image classification together. This can help validate categorizations or catch mismatches:

```python
# Compare text-based and image-based classifications
results = df.select(
    "Product Name",
    "product_type",
    "visual_category",
    "sentiment",
    "image"
).collect()

results.show()
```

## Scaling Up

Once you're satisfied with the classification results on a small sample, you can easily scale up to the full dataset:

```python
# Process more products (filter out null descriptions)
df_large = daft.read_huggingface("calmgoose/amazon-product-data-2020")
df_large = (
    df_large.select("Product Name", "About Product", "Image")
    .where(daft.col("About Product").not_null())
    .limit(100)
)

# Apply text classification
df_large = df_large.with_column(
    "product_type",
    classify_text(
        daft.col("About Product"),
        labels=["electronics", "toys", "sports", "home", "clothing", "books"],
        provider="transformers",
        model="facebook/bart-large-mnli",
    ),
)

# Count products by category
category_counts = df_large.groupby("product_type").count().collect()
category_counts.show()
```

## What's Next?

- Learn more about [Text and Image Classification](../ai-functions/classify.md) functions
- Explore the [Quickstart](../quickstart.md) for more AI capabilities with structured outputs
- See [Distributed Computing](../distributed/index.md) to run classification at scale
