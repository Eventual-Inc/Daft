# AI

Feature coverage table:

| Feature              | OpenAI | Google | vLLM Prefix Caching | LM Studio | Transformers |
| ---                  | ---    | ---    | ---                 | ---       | ---          |
| Prompt               | ✅     | ✅     | ✅                  | ✅        | x            |
| Embedding            | ✅     | x      | x                   | ✅        | ✅           |
| Classification       | x      | x      | x                   | x         | ✅           |
| Image Classification | x      | x      | x                   | x         | ✅           |


!!! note
    Open an issue on our [GitHub](https://github.com/Eventual-Inc/Daft/issues/new/choose) if you would like to see support for a feature.


## Functions

::: daft.functions.ai.prompt
    options:
        heading_level: 3

::: daft.functions.ai.embed_text
    options:
        heading_level: 3

::: daft.functions.ai.embed_image
    options:
        heading_level: 3

::: daft.functions.ai.classify_text
    options:
        heading_level: 3

::: daft.functions.ai.classify_image
    options:
        heading_level: 3


## Model Protocols

::: daft.ai.protocols.Prompter
    options:
        heading_level: 3

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


## Providers

::: daft.ai.provider.Provider
    options:
        filters: ["!^_"]
        heading_level: 3

::: daft.ai.provider.load_provider
    options:
        heading_level: 3

::: daft.ai.provider.load_google
    options:
        heading_level: 3

::: daft.ai.provider.load_lm_studio
    options:
        heading_level: 3

::: daft.ai.provider.load_openai
    options:
        heading_level: 3

::: daft.ai.provider.load_transformers
    options:
        heading_level: 3

::: daft.ai.provider.load_vllm_prefix_caching
    options:
        heading_level: 3
