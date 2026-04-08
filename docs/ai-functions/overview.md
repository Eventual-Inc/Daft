# AI Functions Overview

Daft is purpose-built for scaling multimodal AI workloads. AI Functions make it easy to run models on data whether you're generating embeddings, classifying text, or prompting language models. Daft's built-in Image and File abstractions simplify data handling whether it's on your laptop or in the cloud.

The current list of AI functions includes:

- [`prompt`](prompt.md) - Generate text completions from language models
- [`embed_text`](embed.md#text-embeddings) - Create vector embeddings from text
- [`embed_image`](embed.md#image-embeddings) - Create vector embeddings from images
- [`classify_text`](classify.md#text-classification) - Zero-shot text classification
- [`classify_image`](classify.md#image-classification) - Zero-shot image classification

For more detailed information on the Providers, see [AI Providers Overview](providers.md). If you'd like to contribute a new AI function or expand provider support, check out [Contributing New AI Functions & Providers](../contributing/contributing-ai-functions.md).

## See Also

- [Working with Text](../modalities/text.md) - Text processing and chunking strategies
- [Working with Images](../modalities/images.md) - Image processing and transformations
- [Embeddings](../modalities/embeddings.md) - Vector representations for RAG and search
- [Custom Code with @daft.func](../custom-code/func.md) - Write custom Python functions for specialized processing
- [Batch Inference](../use-case/batch-inference.md) - End-to-end batch inference patterns
