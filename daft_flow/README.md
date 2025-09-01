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
chunk_flow = dflow.Flow("text", str).chunk_text()
chunk_flow["chunk_summary"] = chunk_flow["chunk"].prompt("summarize this chunk of text")

# Run/test on simple Python string(s)
print(chunk_flow.collect("The quick brown fox jumps over the lazy dog"))
print(chunk_flow.collect_batch([
    "The quick brown fox jumps over the lazy dog",
    "The five boxing wizards jump quickly",
    "Sphinx of black quartz, judge my vow",
]))

# Seamlessly extend your flows for running on **Files** instead of Python strings
# Easily run on both local and remote files over protocols such as HTTP, S3, GCS
url_flow = dflow.Flow(str).open_file().read().chain(chunk_flow)
print(url_flow.execute("https://raw.githubusercontent.com/Eventual-Inc/Daft/refs/heads/main/CONTRIBUTING.md"))

# Run custom Python functions
#
# For example, we use Pandas here to read a CSV and extract out a filtered list of URLs
def pandas_csv_extractor(csv_file: dflow.File):
    import pandas as pd

    df = pd.read_csv(csv_file)
    df = df.where(df["urls"].str.contains("github"))

    return df["urls"]

csv_flow = dflow.Flow(str).open_file().run_python(pandas_csv_extractor).flat_map(url_flow)
print(csv_flow.eval("test.csv"))
```

## Writing Data

`.execute()` and `.execute_batch` will execute the flow on input(s) and only persist data in Python memory.

`dflow` exposes powerful primitives to express end-to-end data reading/transformation/writing flows. Here is an
example of writing our results to JSON file(s).

```python
import daft_flow as dflow

website_flow = dflow.Flow(str).open_file().read().chunk_text().flat_map().prompt("summarize this chunk of text")
website_to_json_flow = website_flow.write_json("results.json")

written = website_to_json_flow.execute("https://en.wikipedia.org/wiki/OpenAI")
print(written)
```

By default, `dflow` will keep track of results and metadata from every operation. When flatmapping, the parent is referenced by an ID, which can be configured inside of the flow.

```python
website_flow = (
    dflow
        .Flow(str)
        .set_id()  # Set this string as the ID (overrides dflow's random generation)
        .open_file()
        .read()
        .chunk_text()
        .flat_map()
        .prompt("summarize this chunk of text")
)

{
    "__result__": "...",
    "__parent_id__": "https://www.wikipedia.com",
    # ...metadata
    "chunk_index": 0,
}
```

## Scaling Up

`dflow` automatically uses all of your machine's CPU cores, memory, network bandwidth and configured LLM endpoint's rate limits.

If you need to go beyond this, you may submit a job to a remote `dflow` cluster.

```python
# Submit big, long-running jobs to the cloud
#
# Incredibly easy to scale over millions of items (e.g. in a CSV)
with dflow.Cluster("https://my-cluster.daft.ai") as cluster:
    csv_flow = dflow.Flow(str).open_file().run_python(pandas_csv_extractor).flat_map(url_flow)

    # Persist data for this long-running job
    # In this case we write JSON files like this:
    # {"summary": "...", "url": "https://www.google.com", "metadata": {...}}
    csv_flow = csv_flow.write_json(
        "s3://mydata/",
        {
            # Refer to the processed summary
            "summary": dflow.Data(),
            # Refer to metadata from the parent (the non-chunked file)
            "url": dflow.Metadata().parent()["url"],
            # Persist all other metadata as JSON
            "metadata": dflow.Metadata().to_json(),
        },
    )

    job = cluster.run_job(
        chunked_inference_flow,
        "very-large-test.csv",
    )
    print(job)
```

## Composing Complex Flows

```python
import daft_flow

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

with daft_flow.Session() as dflow:
    # Create a flow for each document starting with its URL
    doc_flow = dflow.Flow("url", str)  # Initial field ("url") assumed to be unique
    doc_flow["document"] = doc_flow["url"].open_file()

    # Create a new flow for each chunk in the doc
    # Creates a fixed set of columns:
    # PK: ("__parent_id__", chunk_index: int)
    # Other fields: ["start_offset", "end_offset"]
    chunk_flow = df["document"].chunk_text()
    chunk_flow["chunk_context"] = chunk_flow.chat_completion(
        CHUNK_CONTEXT_PROMPT.format(
            doc_content=doc_flow["document"],
            chunk_content=chunk_flow["chunk"],
        )
    )
    chunk_flow["embedding"] = chunk_flow.embed_text(
        CONTEXTFUL_CHUNK_PROMPT.format(
            chunk_context=chunk_flow["chunk_context"],
            chunk_content=chunk_flow["chunk"],
        ),
    )

    ###
    # Data Persistence
    ###
    # Save each document to S3
    write_docs = doc_flow.write_sink(
        S3FileSink(...),
        doc_flow["document"],
    )

    # Save each chunk to a vector DB
    write_chunks = chunk_flow.write_sink(
        TurbopufferSink(...),
        {
            "document_url": doc_flow["url"],
            "chunk_start": chunk_flow["start_offset"],
            "chunk_end": chunk_flow["end_offset"],
            "embedding": chunk_flow["embedding"],
            "context": chunk_flow["context"],
            "chunk": chunk_flow["chunk"],
        },
    )

    results = dflow.execute([write_docs, write_chunks], "https://www.wikipedia.com")
```

## Multimodal Data

## Configurations

Configure your default model provider.

```python

```
