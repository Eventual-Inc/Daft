# AI Providers Usage Overview

!!! warning "Warning"

    These APIs are early in their development. Please feel free to [open feature requests and file issues](https://github.com/Eventual-Inc/Daft/issues/new/choose). We'd love hear want you would like, thank you! 🤘


Daft separates AI function protocols from Provider APIs to enable uniform interface to run models on data. That way you can configure custom providers to easily route requests to different hosts and configure API keys.

Common use-cases requiring a Provider includes:

- [Configuring OpenAIProvider with an API key within a Daft Session](#configuring-openaiprovider-with-an-api-key-within-a-daft-session)
- [Configuring custom OpenAIProviders](#configuring-custom-openaiproviders) (ie OpenRouter, vLLM)
- [Accessing models from private repositories on HugginFace]


## Configuring OpenAIProvider with an API key within a Daft Session.

While its possible to [use AI Functions without explicitly setting a Provider](ai-functions.embedding-text-with-an-implicit-provider) most real world use-cases

```python
import os
import daft
from daft.ai.openai.provider import OpenAIProvider
from daft.session import Session

openrouter_provider = OpenAIProvider(
    name="OpenRouter",
    base_url="https://openrouter.ai/api/v1",
    api_key=os.environ.get("OPENROUTER_API_KEY")
)

# Create a session and attach the provider
sess = Session()
sess.attach_provider(openrouter_provider)
sess.set_provider("OpenRouter")


# Create a dataframe with the quotes
df = daft.from_pydict({
    "quote": [
        "I am going to be the king of the pirates!",
        "I'm going to be the next Hokage!",
    ],
})

# Use the prompt function to classify the quotes
df = (
    df
    .with_column(
        "response",
        prompt(
            daft.col("quote"),
            system_message="You are an anime expert. Classify the anime based on the text and returns the name, character, and quote.",
            provider=sess.get_provider("OpenRouter"),
            model="nvidia/nemotron-nano-9b-v2:free"
        )
    )
)

df.show(format="fancy", max_width=120)

```

## Configuring custom OpenAIProviders

```python
# Implicit global session with upsert on known provider
daft.set_provider("openai", apiKey=os.environ["KEY"], hasResponses=False)

# Explicit session with upsert on known provider
sess = Session()
sess.set_provider("openai")

# Explicit session with explicit provider instance
sess = Session()
provider = OpenAIProvider(name="openai", **options)
sess.attach_provider(provider) # name="openai"
sess.set_provider("openai")

# create_provider(instance/definition)
# drop_provider(identifier)

# set_provider(identifier)
# get_provider(identifier) -> Provider

# 1. It's using the default global session
# 2. Setting with identifier="openai"

# Creating provider instances manually
providerA = MyProvider(name="provA", "my_options")
providerB = MyProvider(name="provB", throttle=True)

# Attach them so that we get name resolution
daft.attach_provider(providerA)
daft.attach_provider(providerB)

# Set one by name (identifier)
daft.set_provider("provB")

# Using an AI function, explicit provider *name*
df = daft.read_csv("...")
df = df.with_column("embedding", embed_text(col("body"), provider="provA"))

# Using an AI function, explicit provider *instance*
df = daft.read_csv("...")
df = df.with_column("embedding", embed_text(col("body"), provider=providerA))

# No DataFrame or Expressions needed!
embed = daft.get_provider("openai").get_text_embedder().instantiate()
text = ["ABC", "DEF"]
print(embed(text))
```
