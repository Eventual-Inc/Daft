# Prompting Language Models with Daft

A key strength of Daft is its ability to parallelize data processing on structured data. The `prompt` function is a powerful tool for use-cases like knowledge extraction, synthetic data generation, and batch tool-calling.

<div class="grid cards" markdown>

- [**Prompt Basics**](#prompt-basics)

    Learn how to compose prompt templates using DataFrame syntax, configure models, and pass optional parameters like temperature and max_tokens.

- [**Structured Outputs**](#structured-outputs)

    Generate structured data from language models using Pydantic models that automatically convert to Daft DataTypes with native struct support.

- [**Multimodal Inputs (Text, Images, Files)**](#multimodal-inputs)

    Pass text, images, and files (PDF, Markdown, HTML, CSV) as multimodal inputs to language models with automatic content formatting.

- [**Tool Calling**](#tools)

    Use OpenAI's built-in tools (web search, file search, code interpreter) or define custom functions for agentic workflows at scale.

</div>

!!! note
    `prompt` is under active development, so make sure to stay up to date with our [latest releases on github](https://github.com/Eventual-Inc/Daft/releases) or through announcements in our [slack community](https://join.slack.com/t/dist-data/shared_invite/zt-2e77olvxw-uyZcPPV1SRchhi8ah6ZCtg).

## Quick Start

The `prompt` function enables you to call language models on DataFrame columns, making it easy to scale LLM operations across thousands or millions of rows with automatic batching and parallelization.

The default provider for `prompt` is **OpenAI**. This means you can use OpenAI's latest models without specifying a provider. Just make sure your `OPENAI_API_KEY` environment variable is set and an OpenAIProvider will be created and attached to your `daft.Session` for you.

```python
import daft
from daft.functions import prompt

# read some sample data
df = daft.from_pydict({
    "input": [
        "What sound does a cat make?",
        "What sound does a dog make?",
        "What sound does a cow make?",
        "What does the fox say?",
    ]
})

# generate responses using a chat model
df = df.with_column("response", prompt(daft.col("input"), model="gpt-5-mini"))

df.show()
```

```
╭─────────────────────────────┬─────────────────────────────╮
│ input                       ┆ response                    │
│ ---                         ┆ ---                         │
│ String                      ┆ String                      │
╞═════════════════════════════╪═════════════════════════════╡
│ What sound does a cat make? ┆ A cat makes a "meow" sound. │
│ What sound does a dog make? ┆ A dog makes a "woof" sound. │
│ What sound does a cow make? ┆ A cow makes a "moo" sound.  │
│ What does the fox say?      ┆ The fox says "ring-ding-... │
╰─────────────────────────────┴─────────────────────────────╯
```

## Prompt Basics

The `prompt` function supports a wide range of tasks, from simple text generation to complex agentic workflows with tool calling. Daft's native multimodal data handling and scalable execution make it easy to process large datasets efficiently. This section covers core patterns for working with prompts, including templates, parameters, and model configuration.

!!! note
    The default endpoint for OpenAI providers is `v1/responses`. You can pass `use_chat_completions=True` as an option to use the Chat Completions API instead of the new Responses API.


### Working with OpenAI-compatible providers

This example shows how to use the `prompt` function with an OpenAI-compatible provider like OpenRouter to classify anime quotes.

```python
import os
import daft
from daft.functions import prompt

daft.set_provider(
    "openai",
    base_url="https://openrouter.ai/api/v1",
    api_key=os.environ.get("OPENROUTER_API_KEY")
)

df = daft.from_pydict({
    "quote": [
        "I am going to be the king of the pirates!",
        "I'm going to be the next Hokage!",
    ],
})

df = (
    df
    .with_column(
        "response",
        prompt(
            daft.col("quote"),
            system_message="Classify the anime from the quote and return the show, character name, and explanation.",
            provider="openai",
            model="nvidia/nemotron-nano-9b-v2:free"
        )
    )
)

df.show(format="fancy", max_width=120)
```

```{title="Output"}
╭───────────────────────────────────────────┬──────────────────────────────────────────────────────────────╮
│ quote                                     ┆ response                                                     │
╞═══════════════════════════════════════════╪══════════════════════════════════════════════════════════════╡
│ I am going to be the king of the pirates! ┆ **Show:** *One Piece*                                        │
│                                           ┆ **Character Name:** Monkey D. Luffy                          │
│                                           ┆ **Explanation:**                                             │
│                                           ┆ The quote "I am going to be the king…                        │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ I'm going to be the next Hokage!          ┆ **Show:** *Naruto*                                           │
│                                           ┆ **Character Name:** Naruto Uzumaki                           │
│                                           ┆ **Explanation:** The quote "I'm going to be the next Hokage… │
╰───────────────────────────────────────────┴──────────────────────────────────────────────────────────────╯
(Showing first 2 of 2 rows)
```


### Building Prompt Templates with the `format` function

In addition to passing plain strings, you can build dynamic prompt templates using Daft's `format` function. This works similarly to Python's `format`, but works with Daft expressions. You can also define prompt templates with user-defined functions. With `@daft.func` you can pass in expressions or plain python strings for more flexible composition.

```python
import daft
from daft.functions import prompt, format

# Define a prompt template with "format"
def answer_in_another_language(language: str, column_name: str) -> daft.Expression:
    return format(
        "Answer the following text in {}: {}",
        daft.lit(language),
        daft.col(column_name),
    )

# Alternatively define a row-wise UDF
@daft.func
def answer_in_another_language_func(language: str, question: str) -> str:
    return f"Answer the following text in {language}: {question}"

# Define some toy data
df = daft.from_pydict({
    "text": [
        "In what state(s) does water reach 101 degrees celsius?",
        "What are the total number of planets in our solar system?",
        "Who was the first person to discover radioactivity?",
    ]
})

# Lazily plan to create two new columns
df = (
    df
    .with_column(
        "answer1",
        prompt(
            answer_in_another_language("Chinese", "text"),
            model="google/gemma-3-4b",
            provider="lm_studio"  # Make sure you have google/gemma-3-4b loaded & running in lm_studio (local)
        )
    )
    .with_column(
        "answer2",
        prompt(
            answer_in_another_language_func(daft.lit("Spanish"), daft.col("text")),
            model="google/gemma-3-4b",
            provider="lm_studio"
        )
    )
)

df.show(format="fancy", max_width=80)
```


### Optional parameters

Passing additional arguments like `temperature` or `max_tokens` can simply be added to any prompt call as a keyword argument. All `**kwargs` are routed to the inference request unless documented otherwise.

Instructions for the model can also be added via the `system_message` argument which automatically prepends user content messages:

```json
{"role": "system", "content": "You are an expert Chinese translation interpreter"}
```

For instance, if we wanted to call OpenAI via the Chat Completions endpoint, specifying `temperature` and `max_tokens` we would define a script like:

```python
import daft
from daft.functions import prompt

df = daft.from_pydict({
    "prompt": [
        "What is the triple point of water?",
    ]
})

# Prompt Usage for GPT-4.1 Chat Completions
df = df.with_column(
    "result",
    prompt(
        messages=daft.col("prompt"),
        system_message="You are a helpful assistant.",
        model="gpt-4.1-2025-04-14",
        provider="openai",
        temperature=0.717,  # Specify temperature
        max_tokens=200,     # Specify max_tokens
        use_chat_completions=True,
    )
)
```

However, the Responses API uses different parameter names. For GPT-5 models, use `max_output_tokens` instead of `max_tokens`, and note that temperature is not supported:

```python
import daft
from daft.functions import prompt

df = daft.from_pydict({
    "prompt": [
        "What is the triple point of water?",
    ]
})

# Prompt Usage for GPT-5 Responses
df = df.with_column(
    "result",
    prompt(
        messages=daft.col("prompt"),
        system_message="You are a helpful assistant.",
        model="gpt-5",
        provider="openai",
        max_output_tokens=200,  # Specify max_output_tokens
    )
)
```

### Reasoning Models

GPT-5 and other reasoning-capable models support the `reasoning` parameter to control compute allocation. Pass it as you would in a `client.responses.create()` request:

```python
import daft
from daft.functions import prompt

df = daft.from_pydict({
    "prompt": [
        "Find me buy-one-get-one-free burrito deals in SF right now.",
    ]
})

# Prompt Usage for GPT-5 Responses
df = df.with_column(
    "result",
    prompt(
        messages=daft.col("prompt"),
        system_message="You are a helpful assistant.",
        model="gpt-5-2025-08-07",
        provider="openai",
        reasoning={"effort": "high"},  # Adjust reasoning level
    )
)
```


**See also**: [Providers Overview](providers.md).

### Tools

The easiest way to get started with tool calling is to use OpenAI's built-in tools like web-search.

```python
import daft
from daft.functions import prompt
from datetime import datetime

df = daft.from_pydict({
    "query": [
        "Buy one get one free burritos in SF.",
    ]
})

df = df.with_column(
    "search_results",
    prompt(
        daft.col("query"),
        model="gpt-5",
        tools=[{"type": "web_search"}],
    )
)

df.write_csv(f"../.data/prompt/oai_web_search_{str(datetime.now())}")
```

## Multimodal Inputs

Daft is purpose-built for multimodal AI workloads. The `prompt` function accepts text, images, and files as a list of expressions in the `messages` parameter, making it easy to work with mixed content types.

```python
import daft
from daft.functions import prompt

df = daft.from_pydict({
    "prompt": ["What's in this image and file?"],
    "my_image": ["/Users/MyName/Downloads/SigmundTheCat.png"],
    "my_file": ["/Users/MyName/Downloads/report.pdf"],
})

# Decode the image and file paths
df = df.with_column(
    "my_image",
    daft.functions.decode_image(daft.col("my_image").download())
)

df = df.with_column(
    "my_file",
    daft.functions.file(daft.col("my_file"))
)

# Prompt Usage for GPT-5 Responses
df = df.with_column(
    "result",
    prompt(
        messages=[daft.col("prompt"), daft.col("my_image"), daft.col("my_file")],
        system_message="You are a helpful assistant.",
        model="gpt-5-2025-08-07",
        provider="openai",
        reasoning={"effort": "high"},  # Adjust reasoning level
        tools=[{"type": "web_search"}],  # Leverage internal OpenAI Tools
    )
)
```


### Working with Files

The `prompt` function supports multiple file input methods depending on the provider and file type:

- **PDF files**: Passed directly as file inputs (native OpenAI support)
- **Text files** (Markdown, HTML, CSV, etc.): Content is automatically extracted and injected into prompts
- **Images**: Supported via `daft.DataType.Image()` or file paths

#### PDF Files

To include PDF files, create a `daft.File` column and pass it in your messages list:


```python
import daft
from daft import lit, col
from daft.functions import prompt, file, format
from dotenv import load_dotenv

load_dotenv()

# Discover PDF files from HuggingFace
df = daft.from_glob_path("hf://datasets/Eventual-Inc/sample-files/papers/*.pdf")

df = (
    df
    # Create a daft.File column from the path
    .with_column("file", file(col("path")))
    # Prompt GPT-5-nano with PDF files as context
    .with_column(
        "response",
        prompt(
            [lit("What are in the contents of this file?\n"), col("file")],
            model="gpt-5-nano",
            provider="openai",
        )
    )
)
df.show(format="fancy", max_width=80)
```

```{title="Output"}
╭─────────────────────────────────────────────────────────────────┬─────────┬──────────┬────────────────────────────────────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────╮
│ path                                                            ┆ size    ┆ num_rows ┆ file                                                                           ┆ response                                                                       │
╞═════════════════════════════════════════════════════════════════╪═════════╪══════════╪════════════════════════════════════════════════════════════════════════════════╪════════════════════════════════════════════════════════════════════════════════╡
│ hf://datasets/Eventual-Inc/sample-files/papers/2306.03081.pdf   ┆ 666918  ┆ None     ┆ Unknown(path: hf://datasets/Eventual-Inc/sample-files/papers/2306.03081.pdf)   ┆ Here’s what the file contains (a concise overview and outline of contents):    │
│                                                                 ┆         ┆          ┆                                                                                ┆                                                                                │
│                                                                 ┆         ┆          ┆                                                                                ┆ …                                                                              │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/Eventual-Inc/sample-files/papers/2102.04074v1.pdf ┆ 798743  ┆ None     ┆ Unknown(path: hf://datasets/Eventual-Inc/sample-files/papers/2102.04074v1.pdf) ┆ It’s a full research paper by Marcus Hutter on learning curves and data-size … │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/Eventual-Inc/sample-files/papers/2304.06556v2.pdf ┆ 689507  ┆ None     ┆ Unknown(path: hf://datasets/Eventual-Inc/sample-files/papers/2304.06556v2.pdf) ┆ Here’s a concise summary of what the file contains. It’s a research paper tit… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/Eventual-Inc/sample-files/papers/2212.09741v3.pdf ┆ 1263373 ┆ None     ┆ Unknown(path: hf://datasets/Eventual-Inc/sample-files/papers/2212.09741v3.pdf) ┆ This file is a research paper about INSTRUCTOR, a single embedding model that… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/Eventual-Inc/sample-files/papers/2305.09067v1.pdf ┆ 1617825 ┆ None     ┆ Unknown(path: hf://datasets/Eventual-Inc/sample-files/papers/2305.09067v1.pdf) ┆ Here’s a concise rundown of what’s in the file you shared. It’s a research pa… │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/Eventual-Inc/sample-files/papers/2308.09687v4.pdf ┆ 3128193 ┆ None     ┆ Unknown(path: hf://datasets/Eventual-Inc/sample-files/papers/2308.09687v4.pdf) ┆ This file is a research paper about Graph of Thoughts (GoT), a prompting…  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/Eventual-Inc/sample-files/papers/2303.11156v3.pdf ┆ 9913832 ┆ None     ┆ Unknown(path: hf://datasets/Eventual-Inc/sample-files/papers/2303.11156v3.pdf) ┆ Here’s a concise overview of what the file contains.                           │
│                                                                 ┆         ┆          ┆                                                                                ┆                                                                                │
│                                                                 ┆         ┆          ┆                                                                                ┆ - What it is                                                                   │
│                                                                 ┆         ┆          ┆                                                                                ┆   - A rese…                                                                    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ hf://datasets/Eventual-Inc/sample-files/papers/2209.11895v1.pdf ┆ 9957060 ┆ None     ┆ Unknown(path: hf://datasets/Eventual-Inc/sample-files/papers/2209.11895v1.pdf) ┆ Here’s a high-level table of contents for what’s in the file. It’s a full res… │
╰───────────────────────────────────────────────
```

#### Text Files (Markdown, HTML, CSV)

While OpenAI's native API only supports PDFs directly, attempting to pass other file types like Markdown, CSV, or HTML will throw errors like:

```
Invalid input: Expected file type to be a supported format: .pdf but got .jsonl”
```

To work around this, Daft automatically extracts content from any `text/*` MIME type file and injects it into the prompt with XML-wrapped tags. For example, to use Markdown files as context:

```python
import daft
from daft import lit, col
from daft.functions import prompt, file, format

# Discover Markdown Files in your Documents Folder
df = daft.from_glob_path("/Users/MyName/Documents/*.md")

df = (
    df
    # Create a daft.File column from the path
    .with_column("file", file(col("path")))
    # Prompt GPT-5-nano with markdown files as context
    .with_column(
        "response",
        prompt(
            messages=[lit("What are in the contents of this file?\n"), col("file")],
            model="gpt-5-nano",
            provider="openai",
        )
    )
)
df.show(format="fancy", max_width=80)
```

When you pass `[lit("What are in the contents of this file?\n"), col("file")]`, Daft automatically formats the content. The resulting prompt will look like:

```text
What are in the contents of this file?
<file_text_markdown>
## Some Title
Example Markdown content with a [link](https://somelink.com).
</file_text_markdown>
```


## Structured Outputs

Daft natively supports converting Pydantic Models to Daft Datatypes. This simplifies structured generation in a vectorized context where nested fields materialize as structs.

```python
import os
from dotenv import load_dotenv

import daft
from daft.functions import prompt, unnest
from pydantic import BaseModel, Field


load_dotenv()

class Anime(BaseModel):
    show: str = Field(description="The name of the anime show")
    character: str = Field(description="The name of the character who says the quote")
    explanation: str = Field(description="Why the character says the quote")

daft.set_provider(
    "openai",
    base_url="https://openrouter.ai/api/v1",
    api_key=os.environ.get("OPENROUTER_API_KEY"),
)

# Create a dataframe with the quotes
df = daft.from_pydict(
    {
        "quote": [
            "I am going to be the king of the pirates!",
            "I'm going to be the next Hokage!",
        ],
    }
)

# Use the prompt function to classify the quotes
df = df.with_column(
    "nemotron-response",
    prompt(
        daft.col("quote"),
        system_message="You are an anime expert. Classify the anime based on the text and return the name, character, and quote.",
        return_format=Anime,
        provider="openai",
        model="nvidia/nemotron-nano-9b-v2:free",
    ),
).select("quote", unnest(daft.col("nemotron-response")))

df.show(format="fancy", max_width=120)
```


### Structured Outputs with vLLM OpenAI Online Server

Sometimes Pydantic structured outputs is overkill. [vLLM's structured outputs](https://docs.vllm.ai/en/latest/features/structured_outputs/#online-serving-openai-api) syntax uses the `extra_body` param which we can use thanks to how Daft wires `kwargs` to the model request. Keep in mind the normal Pydantic structured outputs via `return_format` also works with vLLM online serving.

One of the fastest ways to get started with online serving with vLLM is with Google Colab. To launch an OpenAI compatible server on vLLM run the following command in your terminal on an L4 GPU instance or larger:

```bash
python -m vllm.entrypoints.openai.api_server \
  --model google/gemma-3-4b-it \
  --guided-decoding-backend guidance \
  --dtype bfloat16 \
  --gpu-memory-utilization 0.85 \
  --host 0.0.0.0 --port 8000
```
If you are in Google Colab, you can open a terminal by clicking the terminal icon in the bottom left of the ui. It usually takes at least **7.5** minutes before the vLLM server is ready.

#### Setting up vLLM under the OpenAI provider

```python
import daft
from daft.functions import prompt, format

daft.set_provider(
    "openai",
    api_key="none",
    base_url="http://localhost:8000/v1",
)
```

#### Guided Choice (Classification)

```python
df = daft.from_pydict({
    "statement": [
        "vLLM is wonderful",
        "Daft is wicked fast",
        "Slow inference sucks",
    ]
})

df = df.with_column(
    "sentiment",
    prompt(
        messages=format("Classify this sentiment: {}", daft.col("statement")),
        model="google/gemma-3-4b-it",
        extra_body={"structured_outputs": {"choice": ["positive", "negative"]}},
    )
)
```

#### Regex

```python
df = daft.from_pydict({
    "name": [
        "Alan Turing",
        "Steve Jobs",
        "Sammy Sidhu",
        "Jay Chia",
    ],
    "company": [
        "enigma.uk",
        "apple.com",
        "daft.ai",
        "daft.ai",
    ]
})

df = df.with_column(
    "email",
    prompt(
        messages=format(
            "Generate an example email address for {}, who works at {}. Example result: alan.turing@enigma.uk\nMake sure to finish with a new line symbol\n",
            daft.col("name"),
            daft.col("company")
        ),
        model="google/gemma-3-4b-it",
        extra_body={"structured_outputs": {"regex": r"\w+@\w+\.com\n"}, "stop": ["\n"]},
    )
)
```
