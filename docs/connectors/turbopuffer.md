# Writing to turbopuffer

We recommend [turbopuffer](https://turbopuffer.com/) as a fast search engine that combines vector and full-text search using object storage, making all your data easily searchable.

## Example

This example does the following:

1. Reads the [`Open-Orca/OpenOrca`](https://huggingface.co/datasets/Open-Orca/OpenOrca) dataset from [Hugging Face](https://huggingface.co/)
2. Computes text embeddings on the `response` column, using either an [OpenAI model](https://platform.openai.com/docs/models/text-embedding-3-small) or the [`BAAI/bge-base-en-v1.5`](https://huggingface.co/BAAI/bge-base-en-v1.5) open model on Hugging Face with [Sentence Transformers](https://sbert.net/index.html)
3. Writes the results to turbopuffer

=== "🐍 Python"

    ```python
    # $ pip install -U "daft[turbopuffer]"
    # $ pip install -U "daft[openai]"
    # OR
    # $ pip install -U "daft[sentence-transformers]"
    import os

    import daft
    from daft.functions.ai import embed_text

    # Create an embedding with OpenAI, or Sentence Transformers
    # Requires OPENAI_API_KEY to be set (https://platform.openai.com/settings/organization/api-keys)
    if os.getenv("OPENAI_API_KEY"):
        provider = "openai"
        model = "text-embedding-3-small"
    else:
        provider = "sentence_transformers"
        model = "BAAI/bge-base-en-v1.5"

    turbopuffer_config = {
        "namespace": "daft-tpuf-example",
        "region": "gcp-us-central1",
        "distance_metric": "cosine_distance",
        "schema": {
            "text": {
                "type": "string",
                "full_text_search": True,
            }
        }
    }

    (
        daft.read_huggingface("Open-Orca/OpenOrca")
        .limit(8)
        .with_column("vector", embed_text(daft.col("response"), provider=provider, model=model))
        .write_turbopuffer(**turbopuffer_config)  # Requires TURBOPUFFER_API_KEY to be set (https://turbopuffer.com/dashboard)
    )
    ```

For a more a detailed example, check out [our guide on generating text embeddings](../examples/text-embeddings.md).

Check out our [`DataFrame.write_turbopuffer` documentation][daft.DataFrame.write_turbopuffer] for more customization options.
