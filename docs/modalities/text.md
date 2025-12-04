# Working with Text

This how-to guide shows you how to accomplish common text processing tasks with Daft:

- [Generate text embeddings](#generate-text-embeddings)
- [Chunk text into smaller pieces](#chunk-text-into-smaller-pieces)

## Generate text embeddings

Text embeddings convert text into numerical vectors that capture semantic meaning. Use them for semantic search, similarity calculations, and other NLP tasks.

### How to use the embed_text function

By default, `embed_text` uses the [Sentence Transformers provider](#using-sentence-transformers), which requires the `sentence-transformers` [optional dependency](../install.md).

```bash
pip install -U "daft[sentence-transformers]"
```

Once installed, we can run:

```python
import daft
from daft.functions.ai import embed_text

(
    daft.read_huggingface("togethercomputer/RedPajama-Data-1T")
    .with_column("embedding", embed_text(daft.col("text")))
    .show()
)
```

### How to use different providers

#### Using Sentence Transformers

[Sentence Transformers](https://sbert.net/index.html) is a popular module for computing embeddings.

First install the optional Sentence Transformers dependency for Daft.

```bash
pip install -U "daft[transformers]"
```

Then use the `transformers` provider with any desired open model hosted on [Hugging Face](https://huggingface.co/) such as [`BAAI/bge-base-en-v1.5`](https://huggingface.co/BAAI/bge-base-en-v1.5).

```python
import daft
from daft.functions.ai import embed_text

provider = "transformers"
model = "BAAI/bge-base-en-v1.5"

(
    daft.read_huggingface("togethercomputer/RedPajama-Data-1T")
    .with_column("embedding", embed_text(daft.col("text"), provider=provider, model=model))
    .show()
)
```

#### Using OpenAI

[OpenAI](https://platform.openai.com/docs/guides/embeddings) is a popular choice for generating text embeddings.

First install the optional OpenAI dependency for Daft.

```bash
pip install -U "daft[openai]"
```

You will also need to [set your `OPENAI_API_KEY` environment variable](https://platform.openai.com/settings/organization/api-keys).

Then use the `openai` provider with any desired [OpenAI embedding model](https://platform.openai.com/docs/models) such as [`text-embedding-3-small`](https://platform.openai.com/docs/models/text-embedding-3-small).

```python
import daft
from daft.functions.ai import embed_text

provider = "openai"
model = "text-embedding-3-small"

(
    daft.read_huggingface("Open-Orca/OpenOrca")
    .with_column("embedding", embed_text(daft.col("response"), provider=provider, model=model))
    .show()
)
```
!!! tip "Model Constraints"

    Different embedding models have different constraints. For example, OpenAI's `text-embedding-3-small` model has a maximum context length of 8,192 tokens. This means you might encounter error messages like

    ```
    openai.BadRequestError: Error code: 400 - {'error': {'message': "This model's maximum context length is 8192 tokens, however you requested 12839 tokens (12839 in your prompt; 0 for the completion). Please reduce your prompt; or completion length.", 'type': 'invalid_request_error', 'param': None, 'code': None}}
    ```

    In this case you could either use a different model with a larger maximum context length, or could chunk your text into smaller segments before generating embeddings. See our [text embeddings guide](../examples/text-embeddings.md) for examples of text chunking strategies, or refer to the section below on [text chunking](#chunk-text-into-smaller-pieces).

#### Using LM Studio

[LM Studio](https://lmstudio.ai/) is a local AI model platform that lets you run Large Language Models like Qwen, Mistral, Gemma, or gpt-oss on your own machine. If you're running an LM studio server, Daft can use it as a provider for computing embeddings.

First install the optional OpenAI dependency for Daft. This is needed because LM studio uses an OpenAI-compatible API.

```bash
pip install -U "daft[openai]"
```

LM Studio runs on `localhost` port `1234` by default, but you can customize the `base_url` as needed in Daft. In this example, we use the [`nomic-ai/nomic-embed-text-v1.5`](https://huggingface.co/nomic-ai/nomic-embed-text-v1.5) embedding model.

```python
import daft
from daft.ai.provider import load_provider
from daft.functions.ai import embed_text

provider = load_provider("lm_studio", base_url="http://127.0.0.1:1234/v1")  # This base_url parameter is optional if you're using the defaults for LM Studio. You can modify this as needed.
model = "text-embedding-nomic-embed-text-v1.5"  # Select a text embedding model that you've loaded into LM Studio.

(
    daft.read_huggingface("Open-Orca/OpenOrca")
    .with_column("embedding", embed_text(daft.col("response"), provider=provider, model=model))
    .show()
)
```

### How to work with embeddings

It's common to use embeddings for various tasks like similarity search or retrieval with a vector database.

Check out our guide on [writing to turbopuffer](../connectors/turbopuffer.md) to work with a popular fast vector database.


## Chunk text into smaller pieces

When working with large text documents, you often need to break them into smaller chunks.

### How to chunk by sentences

A popular library for sentence chunking is [spaCy](https://spacy.io/).

First, install spaCy and a spaCy model such as `en_core_web_sm`.

```bash
pip install -U spacy
python -m spacy download en_core_web_sm
```

Then, create a [User-defined Function](../custom-code/udfs.md) that uses spaCy.

```python
import daft
import typing

nlp_model_name = "en_core_web_sm"

@daft.func
def chunk_by_sentences(text: str) -> typing.Iterator[str]:
    import spacy
    nlp = spacy.load(nlp_model_name)
    for sentence in nlp(text):
        yield sentence.text


(
    daft.read_huggingface("togethercomputer/RedPajama-Data-1T")
    .limit(8)
    .with_column("chunks", chunk_by_sentences(daft.col("text")))
    .show()
)
```

For a fuller discussion on text chunking strategies, check out the [text chunking section in our tutorial on text embeddings](../examples/text-embeddings.md#step-2-create-text-chunking-udf).

## More examples

Check out our [end-to-end tutorial](../examples/text-embeddings.md) for a complete workflow: chunking text, generating embeddings, and uploading to vector databases like [turbopuffer](../connectors/turbopuffer.md).
