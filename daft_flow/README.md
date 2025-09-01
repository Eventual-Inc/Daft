# Daft Flow

Daft Flow is a data engine for non-tabular data processing.

10 reasons to use Daft Flow:

1. **Python-first:** Declarative, Composable and Pythonic API
2. **LLM/Model-focused:** Incredibly easy/efficient to run models (locally on GPUs or from model endpoints) on unstructured/multimodal/non-tabular data, with built-in optimizations for context construction and prompt caching
3. **Runs your code on GPUs:** First-class support for user functions and beautiful abstractions for GPU usage for custom models
4. **Parallel by default:** efficiently maximizes use of your resources (cores, network and external LLM APIs)
5. **High-performance IO abstractions:** Figures out how to grab any data for you from any location in the most efficient manner (local disk, S3, GCS, Huggingface -> Parquet, HTML files, images, video, ...)
6. **Memory stable:** manages memory use to avoid Out-Of-Memory (OOM) errors
7. **Observability:** best-in-class observability to ensure you don't bust your wallet trying to process data
8. **Caching/Checkpointing:** built-in caching and checkpointing so your expensive operations don't bankrupt you
9. **Error Handling:**  Extensive support for error handling strategies including logging, retries, dead letter queues and more
10. **It's just freakin' fast:** Built in Rust - single binary that scales indefinitely in the cloud with support for distributed computing

## Installation

`pip install daft-flow`

## Quickstart

```python
import daft_flow as dflow

# Simple example that chunks text into summarizations for each chunk
# daft_flow provides: automatic parallelism/batching/rate-limiting
document_flow = dflow.Flow("document", str)
chunk_flow = document_flow.chunk_text()
chunk_flow["chunk_summary"] = chunk_flow["chunk"].prompt("summarize this chunk of text")

# Run/test on simple Python string(s)
print(chunk_flow.collect("The quick brown fox jumps over the lazy dog"))
print(chunk_flow.collect_batch([
    "The quick brown fox jumps over the lazy dog",
    "The five boxing wizards jump quickly",
    "Sphinx of black quartz, judge my vow",
]))
```

Working with files.

```python
# Seamlessly extend your flows for running on **Files** instead of Python strings
# Easily run on both local and remote files over protocols such as HTTP, S3, GCS
url_flow = dflow.Flow(str).open_file().read().chain(chunk_flow)
print(url_flow.collect("https://raw.githubusercontent.com/Eventual-Inc/Daft/refs/heads/main/CONTRIBUTING.md"))

# Run custom Python functions
#
# For example, we use Pandas here to read a CSV and extract out a filtered list of URLs
def pandas_csv_extractor(csv_file: dflow.File):
    import pandas as pd

    df = pd.read_csv(csv_file)
    df = df.where(df["urls"].str.contains("github"))

    return df["urls"]

csv_flow = dflow.Flow(str).open_file().run_python(pandas_csv_extractor).flat_map(url_flow)
print(csv_flow.collect("test.csv"))
```

## Writing Data

`.collect()` executes the flow on input(s) and returns results in Python memory.

`dflow` exposes powerful primitives to express end-to-end data reading/transformation/writing flows. You can write results to various sinks like files, databases, or cloud storage.

```python
import daft_flow as dflow

# Create document processing flow
doc_flow = dflow.Flow("url", str)
doc_flow["document"] = doc_flow["url"].open_file().read()

# Create chunk processing flow
chunk_flow = doc_flow["document"].chunk_text()
chunk_flow["summary"] = chunk_flow["chunk"].prompt("summarize this chunk of text")

# Write chunk summaries to JSON files
write_summaries = chunk_flow.write_sink(
    JSONFileSink("summaries/"),
    {
        "url": doc_flow["url"],
        "chunk_index": chunk_flow["chunk_index"],
        "summary": chunk_flow["summary"]
    },
)

results = write_summaries.collect("https://en.wikipedia.org/wiki/OpenAI")
print(results)
```

## Complex/Contextual Flows

Flows can get complex:

1. Require context from parent flows (e.g. requiring full document content from the chunking flow)
2. Require re-using content from current flow (e.g. using chunks to construct context as well as embeddings)

```
┌────────────────────┐   chunk_text()   ┌───────────────────────────────┐
│ doc_flow           │      ┌─────────► │ chunk_flow                    │
│                    │      │           │                               │
│ URL ───► Document ─┼──────┼─────────► │ chunk ──────────► embedding   │
│              │     │      │           │   │                   ▲       │
│              │     │      └─────────► │   └──────► context ───┘       │
└──────────────┼─────┘                  └───────────────▲───────────────┘
               │                                        │
               └────────────────────────────────────────┘
                 Entire parent document is also used
                 for constructing embedding context
```

The example below is borrowed from Anthropic's [Contextual Embeddings](https://github.com/anthropics/anthropic-cookbook/blob/main/skills/contextual-embeddings/guide.ipynb) guide which requires both of the above flow patterns.

```python
import daft_flow as dflow

CHUNK_CONTEXT_PROMPT = """
<document>
{doc_content}
</document>

Here is the chunk we want to situate within the whole document
<chunk>
{chunk_content}
</chunk>

Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk.
Answer only with the succinct context and nothing else.
"""

CONTEXTFUL_CHUNK_PROMPT = """
<context>
{chunk_context}
</context>
{chunk_content}
"""

# Create a flow for each document starting with its URL
doc_flow = dflow.Flow("url", str)  # Initial field ("url") assumed to be unique
doc_flow["document"] = doc_flow["url"].open_file()

# Create a new flow for each chunk in the doc
chunk_flow = doc_flow["document"].chunk_text()

# Use both the entire document as well as the chunk contents to create context
chunk_flow["chunk_context"] = chunk_flow.prompt(
    CHUNK_CONTEXT_PROMPT.format(
        doc_content=doc_flow["document"],
        chunk_content=chunk_flow["chunk"],
    )
)

# Use both the chunk context as well as the chunk contents to create embedding
chunk_flow["embedding"] = chunk_flow.embed_text(
    CONTEXTFUL_CHUNK_PROMPT.format(
        chunk_context=chunk_flow["chunk_context"],
        chunk_content=chunk_flow["chunk"],
    ),
)
```

## Writing to multiple sinks

When writing flows, it is often useful to write to multiple sinks from various locations in the flow graph.

The example below shows us saving chunks as txt files in S3 as well as their embeddings into a vector database

```python
    ###
    # Data Persistence
    ###
    # Save each chunk (just the raw text as a .txt) to S3
    chunk_txt_files = chunk_flow.write_sink(
        S3FileSink(...),
        chunk_flow["chunk"],
    )

    # Save each chunk to a vector DB
    write_chunks = chunk_flow.write_sink(
        TurbopufferSink(...),
        {
            "document_url": doc_flow["url"],
            "chunk_start": chunk_flow["start_offset"],
            "chunk_end": chunk_flow["end_offset"],
            "embedding": chunk_flow["embedding"],
            "context": chunk_flow["chunk_context"],
        },
    )

    # Run both sinks
    multi_sink = dflow.Multisink([write_docs, write_chunks])
    s3_results, turbopuffer_results = dflow.collect(multi_sink, "https://www.wikipedia.com")
```

## Error Handling

Errors/retries are handled at the flow run level for simplicity. Users may choose to split flows for more granular error handling at the risk of complicating their overall pipeline.

Errors can optionally be propagated to parent flows (e.g. to fail an entire document when a single chunk has failed), triggering a `ChildFlowError` that can be handled by the parent's failure policy.

Sometimes, batch errors may occur. This will fail more than 1 flow run at a time, and errors will be wrapped in a `BatchError` indicating the size of the batch as well as the index of the offending batch element if applicable.

Each flow defines a

Failure policy:

1. Fail the entire flow recursively up to the parent flow -> utilize the parent flow's retry and logging policy
2. Fail only the current child flow run, but keep the parent flow still running -> utilize the current flow's retry and logging policy

Retry policy:

1. Retry with exponential backoff

Logging:

1. Deadletter queue write sink

## Scaling Up

TODO: `dflow.Cluster` and `dflow.Job` APIs, pointing to a platform endpoint.

## Multimodal Data

TODO: Extend dflow to Image, Video, Audio and Documents.

* Image: examples with PIL, torch and SAM
* Video: examples with sampling video frames as Images
* Audio: examples with transcription and speaker identification
* Documents: examples using Docling

## Configurations

TODO: Configuration of LLM provider and model

# Implementation Plan

## Phase 1: Core Foundation

### 1.1 Type System & Flow Creation
- **Core Types**: Implement strongly-typed system supporting: `str`, `JSON`, `int`, `float`, `bytes`, `File`, `Image`, `Video`, `Audio`, `PDF`, `HTML`, `Embedding`
- **Flow Constructor**: `dflow.Flow(field_name: str, field_type: Type)` with compile-time type validation
- **Field Operations**: Implement `__getitem__` and `__setitem__` for `flow["field_name"]` syntax
- **Flow Graph**: Build immutable flow composition (operations return new flow objects)

### 1.2 Python Function Integration (Priority #1)
- **Sync Functions**: `flow.run_python(fn)` supporting synchronous Python functions
- **Type Marshalling**: Seamless conversion between Rust types and Python primitives/JSON/arrays
- **Error Handling**: Map Python exceptions to Rust errors with full context using `thiserror`
- **Memory Management**: Zero-copy where possible, minimize Python/Rust boundary crossings

### 1.3 Text Processing Operations (Priority #2)
- **Chunk Text**: `flow.chunk_text()` implementing one-to-many flow splitting
- **Basic Operations**: Essential transformations to support the quickstart examples
- **Flow Composition**: Support referencing parent flow fields from child flows

## Phase 2: Execution Engine

### 2.1 Lazy Evaluation System
- **Computation Graph**: Build and optimize execution plan before running
- **Batching Engine**: Automatic batching based on operation type with user hints
- **Execution Triggers**: `collect(input)` and `stream(input)` methods

### 2.2 Arrow Integration
- **Daft Integration**: Use Arrow for structured data exchange with existing Daft functionality
- **Memory Efficiency**: Leverage Arrow's columnar format for batch processing
- **Type Mapping**: Map flow types to Arrow schemas

### 2.3 Streaming & Collection
- **Collection API**: `flow.collect(input_data)` returning Python objects
- **Streaming API**: `flow.stream(input_data)` returning Python iterator
- **Execution Config**: Configure batching, caching, retry policies during flow construction

## Phase 3: Advanced Features

### 3.1 Caching System
- **Per-Operation Caching**: User-configurable caching with unique row IDs
- **Hybrid Storage**: Memory + disk caching with pluggable backends (Redis support)
- **Cache Management**: Manual clearing and automatic cleanup policies

### 3.2 Error Handling & Resilience
- **Error Hierarchy**: `ChildFlowError`, `BatchError` with rich context using `thiserror`
- **Failure Policies**: Configurable propagation (fail parent vs isolate child)
- **Retry Logic**: Exponential backoff with per-flow configuration
- **Dead Letter Queue**: Configurable logging sink for failed operations

### 3.3 Checkpointing
- **Granularity**: Per-flow run checkpointing with user-defined checkpoint support
- **Persistence**: JSON format with pluggable storage backends
- **Recovery**: Automatic resume from checkpoints on failure

## Phase 4: GPU & Advanced User Functions

### 4.1 GPU Function Support
- **Resource Allocation**: Per-function GPU ownership model
- **FastAPI-style UDFs**: Rich function interface with error codes, logging, lifecycle management
- **Async Functions**: Support for async Python functions
- **Deployment Specs**: Autoscaling and deployment configuration (TBD after initial implementation)

### 4.2 Advanced Operations
- **LLM Operations**: `prompt()`, `embed_text()` with provider configuration
- **File Operations**: `open_file()`, `read()` with multi-protocol support
- **Multimodal Support**: Image, Video, Audio, Document processing operations

## Implementation Priorities

1. **Start Here**: Basic Flow + run_python (escape hatch for all other functionality)
2. **Core Operations**: chunk_text (one-to-many flow pattern)
3. **Execution Engine**: Lazy evaluation + collect/stream
4. **Error Handling**: thiserror integration + failure policies
5. **Advanced Features**: Caching, checkpointing, GPU functions

## Success Criteria

- [ ] Can create flows with type validation
- [ ] Can run Python functions on flow data
- [ ] Can chunk text and process results
- [ ] Can collect results with proper error handling
- [ ] Can stream results efficiently
- [ ] Integration tests pass with existing Daft functionality
