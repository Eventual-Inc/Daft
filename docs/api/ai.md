# AI

Daft has a Provider interface and model protocols for various inference APIs.

## Providers

::: daft.ai.provider.Provider
    options:
        filters: ["!^_"]
        heading_level: 3


::: daft.ai.provider.load_provider
    options:
        heading_level: 3

::: daft.ai.provider.load_openai
    options:
        heading_level: 3

::: daft.ai.provider.load_sentence_transformers
    options:
        heading_level: 3

::: daft.ai.provider.load_transformers
    options:
        heading_level: 3

## Model Protocols

::: daft.ai.protocols.TextEmbedder
    options:
        heading_level: 3

::: daft.ai.protocols.TextEmbedderDescriptor
    options:
        heading_level: 3

::: daft.ai.protocols.ImageEmbedder
    options:
        heading_level: 3

::: daft.ai.protocols.ImageEmbedderDescriptor
    options:
        heading_level: 3

::: daft.ai.protocols.TextClassifier
    options:
        heading_level: 3

::: daft.ai.protocols.TextClassifierDescriptor
    options:
        heading_level: 3
