# Generate Embeddings from Text and Images with Daft

Embeddings are dense vector representations of data that capture semantic meaning. Daft provides fast, scalable embedding generation for both text and images, with support for various providers including OpenAI, Transformers, and local models.

## Text Embeddings

The `embed_text` function generates embeddings from text using various models. You can use AI functions without explicitly configuring a provider—if you have an `OPENAI_API_KEY` environment variable set, simply specify `provider="openai"` and it will automatically create a provider in your session.

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

```{title="Output"}
╭─────────────┬──────────────────────────╮
│ text        ┆ embeddings               │
│ ---         ┆ ---                      │
│ String      ┆ Embedding[Float32; 1536] │
╞═════════════╪══════════════════════════╡
│ Hello World ┆ ▆█▆▆▆▃▆▆▂▄▃▂▃▃▄▁▃▅▂▃▂▂▂▂ │
╰─────────────┴──────────────────────────╯
```

### Local Embeddings with LM Studio

For privacy-sensitive workloads or offline environments, you can generate embeddings locally with [LM Studio](https://lmstudio.ai/). This lets you run embedding models like Nomic, Qwen, or Mistral on your own machine, taking advantage of hardware accelerators like [Apple's Metal Performance Shaders (MPS)](https://developer.apple.com/documentation/metalperformanceshaders).

```python
import daft
from daft.functions import embed_text

df = daft.from_pydict({
    "text": ["Hello, world!"]
})

df = df.with_column(
    "embedding",
    embed_text(daft.col("text"), model="text-embedding-nomic-embed-text-v1.5", provider="lm_studio")
)

df.show()
```

```
╭───────────────┬──────────────────────────╮
│ text          ┆ embedding                │
│ ---           ┆ ---                      │
│ String        ┆ Embedding[Float32; 768]  │
╞═══════════════╪══════════════════════════╡
│ Hello, world! ┆ █▄▄▃▃▄▃▄▃▆▂▁▂▅▅▂▂▃▅▄▄▃▂▄ │
╰───────────────┴──────────────────────────╯

(Showing first 1 of 1 rows)
```

## Image Embeddings

The `embed_image` function generates embeddings from images using vision models. Daft natively supports images with the [Image DataType](../modalities/images.md), making multimodal preprocessing straightforward with built-in utilities like `decode_image`, `convert_image`, and `resize`.

This example shows a complete pipeline: reading multiple image formats (PNG, JPEG, TIFF, WEBP) from HuggingFace, preprocessing them to the required format, and generating embeddings using Apple's [aimv2-large-patch14-224-lit](https://huggingface.co/apple/aimv2-large-patch14-224-lit) model.

```python
import daft
from daft.functions import embed_image, decode_image, convert_image, resize

df = (
    # Discover a few images from HuggingFace
    daft.from_glob_path("hf://datasets/datasets-examples/doc-image-3/images")
    # Read the 4 PNG, JPEG, TIFF, WEBP Images
    .with_column("image_bytes", daft.col("path").download())
    # Decode the image bytes into a daft Image DataType
    .with_column("image_type", decode_image(daft.col("image_bytes")))
    # Convert Image to RGB and resize the image to 288x288
    .with_column("image_resized", resize(convert_image(daft.col("image_type"), "RGB"), 288, 288))
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

    When working with images for embedding, it's important to preprocess them to the right size and format. Be sure to check the model card for specific requirements regarding image format, size, and normalization.

## Semantic Search with Cosine Distance

Once you have embeddings, you can use cosine distance to find similar items. This example demonstrates a simple RAG (Retrieval-Augmented Generation) pattern where we find the most relevant documents for a query.

```python
import daft
from daft.functions import embed_text, cosine_distance

# Create a knowledge base with documents
documents = daft.from_pydict({
    "doc_id": [1, 2, 3, 4],
    "text": [
        "Python is a high-level programming language",
        "Machine learning models require training data",
        "Daft is a distributed dataframe library",
        "Embeddings capture semantic meaning of text",
    ]
})

# Embed all documents
documents = documents.with_column(
    "embedding",
    embed_text(daft.col("text"), provider="openai", model="text-embedding-3-small")
)

# Create a query
query = daft.from_pydict({
    "query_text": ["What is Daft?"]
})

# Embed the query
query = query.with_column(
    "query_embedding",
    embed_text(
        daft.col("query_text"),
        provider="openai",             # Requires OPENAI_API_KEY
        model="text-embedding-3-small"
    )
)

# Cross join to compare query against all documents
results = query.join(documents, how="cross")

# Calculate cosine distance (lower is more similar)
results = results.with_column(
    "distance",
    cosine_distance(daft.col("query_embedding"), daft.col("embedding"))
)

# Sort by distance and show top results
results = results.sort("distance").select("query_text", "text", "distance")
results.show()
```

```{title="Output"}
╭───────────────┬────────────────────────────────┬─────────────────────╮
│ query_text    ┆ text                           ┆ distance            │
│ ---           ┆ ---                            ┆ ---                 │
│ String        ┆ String                         ┆ Float64             │
╞═══════════════╪════════════════════════════════╪═════════════════════╡
│ What is Daft? ┆ Daft is a distributed datafra… ┆ 0.36203420173101486 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ What is Daft? ┆ Python is a high-level progra… ┆ 0.91644507004277    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ What is Daft? ┆ Embeddings capture semantic m… ┆ 0.937416795759487   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ What is Daft? ┆ Machine learning models requi… ┆ 0.9697065433855928  │
╰───────────────┴────────────────────────────────┴─────────────────────╯

(Showing first 4 of 4 rows)
```

!!! tip "Scaling Similarity Search"
    For large-scale similarity search, consider:

    - Pre-computing and storing embeddings to avoid re-generation
    - Using approximate nearest neighbor (ANN) algorithms for huge datasets
    - Filtering documents before computing similarity to reduce comparisons
    - Writing results to a vector database like [Turbopuffer](../connectors/turbopuffer.md) for efficient retrieval
