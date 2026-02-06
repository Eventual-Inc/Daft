# Working with Embeddings

Embeddings transform text, images, and other data into dense vector representations that capture semantic meaning—enabling similarity search, retrieval-augmented generation (RAG), and AI-powered discovery. Daft makes it easy to generate, store, and query embeddings at scale.

With the native [`daft.DataType.embedding`](../api/datatypes/embedding.md) type and [`embed_text`](../api/functions/embed_text.md) function, you can:

- **Generate embeddings** from any text column using providers like OpenAI, Cohere, or local models
- **Compute similarity** with built-in distance functions like `cosine_distance`
- **Build search pipelines** that scale from local development to distributed clusters
- **Write to vector databases** like Turbopuffer, Pinecone, or LanceDB

## Semantic Search Example

The following example creates a simple semantic search pipeline—embedding documents, comparing them to a query, and ranking by similarity:

```python
import daft
from daft.functions import embed_text, cosine_distance

# Create a knowledge base with documents
documents = daft.from_pydict(
    {
        "text": [
            "Python is a high-level programming language",
            "Machine learning models require training data",
            "Daft is a distributed dataframe library",
            "Embeddings capture semantic meaning of text",
        ],
    }
)

# Embed all documents
documents = documents.with_column(
    "embedding",
    embed_text(
        daft.col("text"),
        provider="openai",
        model="text-embedding-3-small",
    ),
)

# Create a query
query = daft.from_pydict({"query_text": ["What is Daft?"]})

# Embed the query
query = query.with_column(
    "query_embedding",
    embed_text(
        daft.col("query_text"),
        provider="openai",
        model="text-embedding-3-small",
    ),
)

# Cross join to compare query against all documents
results = query.join(documents, how="cross")

# Calculate cosine distance (lower is more similar)
results = results.with_column(
    "distance", cosine_distance(daft.col("query_embedding"), daft.col("embedding"))
)

# Sort by distance and show top results
results = results.sort("distance").select("query_text", "text", "distance", "embedding")
results.show()
```

```{title="Output"}
╭───────────────┬────────────────────────────────┬────────────────────┬──────────────────────────╮
│ query_text    ┆ text                           ┆ distance           ┆ embedding                │
│ ---           ┆ ---                            ┆ ---                ┆ ---                      │
│ String        ┆ String                         ┆ Float64            ┆ Embedding[Float32; 1536] │
╞═══════════════╪════════════════════════════════╪════════════════════╪══════════════════════════╡
│ What is Daft? ┆ Daft is a distributed datafra… ┆ 0.3621492191359764 ┆ ▄▇▆▅▄▄█▆▄▄▃▂▄▃▃▃▁▄▃▃▄▄▃▂ │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ What is Daft? ┆ Python is a high-level progra… ┆ 0.9163975397319742 ┆ ▇▆▅▇▅▆█▇▃▄▆▄▄▁▅▄▅▃▁▃▃▂▅▃ │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ What is Daft? ┆ Embeddings capture semantic m… ┆ 0.9374004015203741 ┆ ▄█▅▄▅▅▅▇▄▃▂▁▃▄▄▁▃▃▂▂▂▂▁▃ │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ What is Daft? ┆ Machine learning models requi… ┆ 0.9696998373223874 ┆ ▇▇▆▃▄▆▅█▆▂▄▃▄▄▂▄▂▁▂▂▁▃▂▁ │
╰───────────────┴────────────────────────────────┴────────────────────┴──────────────────────────╯

(Showing first 4 of 4 rows)
```

## Building a Document Search Pipeline

For production use cases, you'll typically combine embeddings with LLM-powered metadata extraction and write the results to a vector database.

This example shows an end-to-end pipeline that:

1. Loads PDF documents from cloud storage
2. Extracts structured metadata using an LLM
3. Generates vector embeddings from the abstracts
4. Writes everything to Turbopuffer for semantic search

```python
# /// script
# description = "This example shows how using LLMs and embedding models, Daft chunks documents, extracts metadata, generates vectors, and writes them to any vector database..."
# dependencies = ["daft[openai, turbopuffer]", "pymupdf"]
# ///
import os
import daft
from daft import col, lit
from daft.functions import embed_text, prompt, file, unnest, monotonically_increasing_id
from pydantic import BaseModel

class Classifier(BaseModel):
    title: str
    author: str
    year: int
    keywords: list[str]
    abstract: str

daft.set_execution_config(enable_dynamic_batching=True)
daft.set_provider("openai", api_key=os.environ.get("OPENAI_API_KEY"))

# Load documents and generate vector embeddings
df = (
    daft.from_glob_path("hf://datasets/Eventual-Inc/sample-files/papers/*.pdf").limit(10)
    .with_column(
        "metadata",
        prompt(
            messages=file(col("path")),
            system_message="Read the paper and extract the classifier metadata.",
            return_format=Classifier,
            model="gpt-5-mini",
        )
    )
    .with_column(
        "abstract_embedding",
        embed_text(
            daft.col("metadata")["abstract"],
            model="text-embedding-3-large"
        )
    )
    .with_column("id", monotonically_increasing_id())
    .select("id", "path", unnest(col("metadata")), "abstract_embedding")
)

# Write to Turbopuffer
df.write_turbopuffer(
    namespace="ai_papers",
    api_key=os.environ.get("TURBOPUFFER_API_KEY"),
    distance_metric="cosine_distance",
    region='us-west-2',
    schema={
        "id": "int64",
        "path": "string",
        "title": "string",
        "author": "string",
        "year": "int",
        "keywords": "list[string]",
        "abstract": "string",
        "abstract_embedding": "vector",
    }
)
```
