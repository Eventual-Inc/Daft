# Writing to Qdrant

[Qdrant](https://qdrant.tech/) is an open-source vector search engine built for high-performance and massive-scale.

## Example

This example does the following:

1. Reads the [`Open-Orca/OpenOrca`](https://huggingface.co/datasets/Open-Orca/OpenOrca) dataset from [Hugging Face](https://huggingface.co/).
2. Computes text embeddings on the `response` column, using either an [OpenAI model](https://platform.openai.com/docs/models/text-embedding-3-small) or the [`BAAI/bge-base-en-v1.5`](https://huggingface.co/BAAI/bge-base-en-v1.5) open model on Hugging Face with [Sentence Transformers](https://sbert.net/index.html).
3. Writes the results to a Qdrant collection.

=== "🐍 Python"

    ```python
    # $ pip install -U "daft[qdrant]"
    # $ pip install -U "daft[openai]"
    # OR
    # $ pip install -U "daft[sentence-transformers]"
    import os

    import daft
    from daft.functions import monotonically_increasing_id
    from daft.functions.ai import embed_text
    from qdrant_client import QdrantClient, models

    # Create an embedding with OpenAI, or Sentence Transformers
    # Requires OPENAI_API_KEY to be set (https://platform.openai.com/settings/organization/api-keys)
    if os.getenv("OPENAI_API_KEY"):
        provider = "openai"
        model = "text-embedding-3-small"
        vector_size = 1536
    else:
        provider = "sentence_transformers"
        model = "BAAI/bge-base-en-v1.5"
        vector_size = 768

    # Create the collection once before writing.
    QdrantClient(url=os.environ.get("QDRANT_URL", "http://localhost:6333")).create_collection(
        "daft-qdrant-example",
        vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE),
    )

    (
        daft.read_huggingface("Open-Orca/OpenOrca")
        .limit(8)
        .with_column("vector", embed_text(daft.col("response"), provider=provider, model=model))
        .with_column("id", monotonically_increasing_id())
        .write_qdrant(
            collection_name="daft-qdrant-example",
            url=os.environ.get("QDRANT_URL", "http://localhost:6333"),
        )
    )
    ```

Check out our [`DataFrame.write_qdrant` documentation][daft.DataFrame.write_qdrant] for more customization options.
