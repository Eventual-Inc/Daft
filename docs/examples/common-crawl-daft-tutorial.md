# Getting Started with Common Crawl in Daft

<a target="_blank" href="https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/common_crawl/getting_started_with_common_crawl.ipynb">
  <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"/>
</a>

👋 Welcome! This tutorial shows how to use [Daft](http://www.daft.ai/) to work with the [Common Crawl](https://commoncrawl.org/) dataset.

Common Crawl is one of the most important open web datasets, containing more than 250 billion web pages that span 18 years of crawls. Since 2020, it has become a critical source of training data for generative AI, with the vast majority of data used to train models like GPT-3 coming from Common Crawl. [Mozilla Foundation's research](https://www.mozillafoundation.org/en/research/library/generative-ai-training-data/common-crawl/) noted that "Generative AI in its current form would probably not be possible without Common Crawl".

Daft provides a simple, performant, and responsible way to access Common Crawl data.

By the end of the tutorial, we will:
- Show you how to define a DataFrame over crawl data
- Explore a recent crawl and get familiar with the schema
- Explain how to use the crawl data for your own tasks
- Show an example of generating text embeddings using a small LLM

Before we begin, let's install Daft and all of the dependencies we'll use in this tutorial.

```python
!pip install uv && uv pip install "daft[transformers]" spacy
```

This is the complete set of imports that we'll use in this notebook tutorial.

```python
import os

# We need to do this _before_ importing torch
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

import json
from collections.abc import Iterator
from datetime import datetime
from typing import TypedDict

import spacy
import spacy.cli
import torch
from transformers import AutoConfig

import daft
from daft import col
```



# Getting Common Crawl Data

Daft has built-in support for getting Common Crawl via the `daft.datasets.common_crawl` function. We'll show an example of how to use that here. Check out the [How to use Common Crawl with Daft](https://docs.daft.ai/en/stable/datasets/common-crawl/) documentation for more details!

```python
IN_AWS = False  # Set this to `True` if you're running in the us-east-1 AWS region.
# For Google Colab, this must be set to `False`.

df = daft.datasets.common_crawl(
    crawl="CC-MAIN-2025-33",  # The specific crawl to get. Crawls are listed on the
    # Common Crawl website. This one was crawled ~ Spring 2025.
    content="warc",  # Options are "raw", "text", "metadata", "warc", "wet", "wat".
    # These change the names, types, and number of columns you'll get
    # and, importantly, what kind of data you'll be able to work with!
    num_files=1,  # Optional number of files to fetch. None means get all files.
    # Fetching a single file is useful for rapid prototyping.
    in_aws=IN_AWS,  # Let's Daft select the optimal download location! You **MUST**
    # set this to False if running outside of us-east-1 in AWS.
)
```

Let's take a look at the first few records to see what they contain.
```python
# Show is a materializing operation in Daft. It will cause the Daft query to run.
# It only computes the first 8 records and then prints them to STDOUT.
df.show()
```

There's a **ton** of information in the WARC files! We have:
  - 'WARC-Record-ID': uuid
  - 'WARC-Type': is this metadata? the original request made to the website? the entire response from the website
  - 'WARC-Date': Timestamp: when was this accessed?
  - 'Content-Length': int: how long is the response?
  - 'WARC-Identified-Payload-Type': do we know any e.g. encoding or type for the response payload we got back from the website?
  - 'warc_content': UTF-8 bytes of the actual website content (or response, metadata, etc.)
  - 'warc_headers': JSON object that contains
    + "Content-Type": str
    + "WARC-Block-Digest": str
    + "WARC-Refers-To": str
    + "WARC-Target-URI": the website

If you want, or need, to get a very fine level of detail for Common Crawl data, then use `content="warc"`.

For our purposes in this tutorial, we only need the text from the website. We don't need the HTML. So we'll instead use the `content="text"` type. This also means we won't need to process as much data!

Here's a preview of what the `content="text"` data looks like:

```python
# NOTE: `daft.col` is imported here. We use `col("XYY")` to build an expression
#       that represents "the dataframe column called XYZ."

df_sample = (
    daft.datasets.common_crawl(
        crawl="CC-MAIN-2025-33",
        content="text",
        num_files=1,
        in_aws=IN_AWS,
    )
    # we only care about website responses that were converted into
    # this `"text"` content we selected for in the Common Crawl data
    .where(col("WARC-Type") == "conversion")
    # try to decode the byte content as UTF-8 encoded text
    .with_column("warc_content", col("warc_content").try_decode("utf-8"))
    # failed decodes result in a `None` value -- we remove these records
    .drop_null(col("warc_content"))
    .select("WARC-Record-ID", "WARC-Target-URI", "WARC-Date", "Content-Length", "warc_content", "warc_headers")
)

df_sample.show()
```

We can see that Common Crawl has pages in different languages! To make this tutorial a bit more concise, we'll only consider English language pages.

We leave it as an exercise to the reader to extend the tutorial to work with multiple languages 😀


## Getting English Language Pages

Let's explore how we can get content in a particular language! The `warc_headers` column contains JSON objects that contain this information. We're looking for the `WARC-Identified-Content-Language` attribute.

Let's make a Daft [User-Defined Function (UDF)](https://docs.daft.ai/en/stable/custom-code/udfs/) that can parse this JSON. Then, let's filter on the `language`.

```python
WarcHeaders = TypedDict(
    "WarcHeaders",
    {
        "Content-Type": str,
        "WARC-Block-Digest": str,
        "WARC-Identified-Content-Language": str,
        "WARC-Refers-To": str,
        "WARC-Target-URI": str,
    },
)


@daft.func
def json_load_warc_headers(x: str) -> WarcHeaders:
    return json.loads(x)


df_lang = df_sample.with_column("warc_headers", json_load_warc_headers(col("warc_headers"))).with_column(
    "language", col("warc_headers").struct.get("WARC-Identified-Content-Language")
)

df_lang.select("warc_content", "language").show()
```

We can see two important things:
1.   `eng` means english language
2.   Records can have multiple languages!

To simplify, we'll only consider webpage records where _all_ of the text is english.

```python
df_lang = df_lang.where(col("language") == "eng")
df_lang.select("warc_content", "language").show()
```



# Embeddings: Preparing Common Crawl for Downstream Tasks

Now that we're acquainted with the Common Crawl dataset, let's generate some embeddings. An [embedding](https://en.wikipedia.org/wiki/Word_embedding) is a representation of data (text, images, audio etc.), often a vector of numerical values, that encodes semantic information.

These embeddings can then be used in many applications such as semantic search, deduplication, multi-lingual applications, and so on. It's common to store embeddings, along with some identifying metadata, into a vector database.

In order to make embeddings, we need to come up with some way to represent our web page text. In the next section, we'll go over how to break up the text into meaningful pieces. Then, we'll show how we can generate embeddings for these text pieces.


## Configuration

Here, we define some variables we use throughout the rest of the tutorial to configure our Daft code. If you want to change the defaults, then fiddle around with these! Otherwise, you can move on to the next section and learn how these variables are used.

```python
######## CONFIGURATION: Options ########

MAX_SEQ_LEN_SPACY: int = 1_000  # Maximum text length for sentence splitting.
NLP_MODEL_NAME: str = "en_core_web_sm"  # spaCy model for sentence detection
CHUNKING_PARALLELISM: int = 4  # Parallel chunking processes

MAX_SEQ_LEN_SENTENCE_TRANSFORMER: int = 1024 * 1  # Maximum text length for any individual embedding.
EMBEDDING_MODEL_NAME: str = "Qwen/Qwen3-Embedding-0.6B"  # Text embedding model
EMBEDDING_BATCH_SIZE: int = 16  # Batch size for embeddings
EMBEDDING_SIZE: int = AutoConfig.from_pretrained(EMBEDDING_MODEL_NAME).hidden_size

######## CONFIGURATION: Validation ########

if MAX_SEQ_LEN_SPACY <= 0:
    raise ValueError(f"MAX_SEQ_LEN_SPACY must be positive! {MAX_SEQ_LEN_SPACY=}")

if len(NLP_MODEL_NAME) == 0:
    raise ValueError("NLP_MODEL_NAME must be specified!")

if CHUNKING_PARALLELISM <= 0:
    raise ValueError(f"CHUNKING_PARALLELISM must be positive! {CHUNKING_PARALLELISM=}")

if MAX_SEQ_LEN_SENTENCE_TRANSFORMER <= 0:
    raise ValueError(f"MAX_SEQ_LEN_SENTENCE_TRANSFORMER must be positive! {MAX_SEQ_LEN_SENTENCE_TRANSFORMER=}")

if len(EMBEDDING_MODEL_NAME) == 0:
    raise ValueError("EMBEDDING_MODEL_NAME must be specified!")

if EMBEDDING_BATCH_SIZE <= 0:
    raise ValueError(f"EMBEDDING_BATCH_SIZE must be positive! {EMBEDDING_BATCH_SIZE=}")
```


## Download the Spacy Model

Before we can use our sentence chunker, we need to download the appropriate Spacy model.

```python
try:
    spacy.cli.download(NLP_MODEL_NAME)
except:
    print(f"ERROR: Invalid spacy model name: {NLP_MODEL_NAME=}")
    raise
```


## Define Sentence Chunking UDF using Spacy

When creating embeddings, it's useful to split your text into meaningful chunks. Text is hierarchical and can be broken down at different levels:
```
Document → Sections → Paragraphs → Sentences → Words → Characters
```
The chunking strategy to use depends on your use case!
- **Sentence-level** chunking works well for most use cases, especially when the document structure is unclear or inconsistent.
- **Paragraph-level** chunking is good for RAG (Retrieval-Augmented Generation) applications where maintaining context across sentences is important.
- **Section-level** chunking is useful for long documents that have clear structural divisions.
- **Fixed-size** chunks are simple to implement but may break semantic meaning at arbitrary boundaries.

Since we're unsure about the text structure of websites, we'll use sentence chunking. This next section defines a Daft UDF (_User Defined Function_) to perform sentence chunking with the Spacy NLP library.

```python
class TextChunk(TypedDict):
    text: str
    chunk_id: int


@daft.cls(max_concurrency=1, use_process=True)
class ChunkingUDF:
    """Chunks text into sentences using Spacy."""

    def __init__(self) -> None:
        # ensure model is already present via:
        #   python -m spacy download {NLP_MODEL_NAME}
        # Or via Python:
        #   spacy.cli.download(NLP_MODEL_NAME)
        # We **DON'T** download it here otherwise we could have a race
        # condition as Daft _can_ make multiple copies of our UDF.
        self.nlp = spacy.load(NLP_MODEL_NAME)

    @daft.method
    def __call__(self, text: str) -> Iterator[TextChunk]:
        n_truncated_spacy = 0
        n_truncated_sentence_transformer = 0
        if len(text) > MAX_SEQ_LEN_SPACY:
            n_truncated_spacy += 1
            text = text[:MAX_SEQ_LEN_SPACY]

        doc = self.nlp(text)

        for i, sentence in enumerate(doc.sents):
            if len(sentence.text) > MAX_SEQ_LEN_SENTENCE_TRANSFORMER:
                s_text = sentence.text[:MAX_SEQ_LEN_SENTENCE_TRANSFORMER]
                n_truncated_sentence_transformer += 1
            else:
                s_text = sentence.text
            chunked_text = TextChunk(text=s_text, chunk_id=i)
            yield chunked_text

        if n_truncated_spacy > 0:
            print(f"Truncated {n_truncated_spacy} sentences that were longer than {MAX_SEQ_LEN_SPACY} characters.")
        if n_truncated_sentence_transformer > 0:
            print(
                f"Truncated {n_truncated_sentence_transformer} sentences that were longer than {MAX_SEQ_LEN_SENTENCE_TRANSFORMER} characters."
            )
```

Let's see what this looks like on a small example!

```python
chunker = ChunkingUDF()

df_chunk = (
    df_lang.with_column("chunks", chunker(col("warc_content")))
    # and we want to see each object's fields as their own columns
    .with_column("text_chunk", col("chunks").get("text"))
    .with_column("text_index", col("chunks").get("chunk_id"))
    .select("WARC-Record-ID", "WARC-Target-URI", "WARC-Date", "text_chunk", "text_index")
)
df_chunk.show()
```


## Text Embedding

Now that we have nice sentence-chunked text, we can produce embeddings! Daft makes it very easy to run models on your data. We can make another class-based UDF to use a locally-running model (from the `sentence-transformers` library) to compute embeddings.

```python
from sentence_transformers import SentenceTransformer


@daft.cls(max_concurrency=1, use_process=True)
class EmbedderUDF:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = SentenceTransformer(EMBEDDING_MODEL_NAME).to(self.device)
        self.model = self.model.eval()
        self.model.compile()

    @daft.method.batch(
        return_dtype=daft.DataType.embedding(daft.DataType.float32(), EMBEDDING_SIZE),
        batch_size=EMBEDDING_BATCH_SIZE,
    )
    def embed_text(self, texts):
        with torch.inference_mode():
            embeddings = self.model.encode(
                texts,
                batch_size=EMBEDDING_BATCH_SIZE,
                output_value="sentence_embedding",
                precision="float32",
                show_progress_bar=False,
                convert_to_numpy=True,
            )
        return embeddings
```

We can use this on our small example to see what the embeddings column looks like.

```python
(
    df_chunk.with_column("embedding", EmbedderUDF().embed_text(col("text_chunk")))
    .select("WARC-Record-ID", "WARC-Target-URI", "text_chunk", "text_index", "embedding")
    .show()
)
```



# Putting Everything Together

Now that we've built-out each part of our pipeline, we can put everything together!

```python
chunker = ChunkingUDF()

embedder = EmbedderUDF()

df = (
    daft.datasets.common_crawl(
        crawl="CC-MAIN-2025-33",
        segment=None,
        content="text",
        num_files=10,  # INCREASE THIS NUMBER TO RUN ON MORE CRAWL FILES
        # OR REMOVE IT / SET IT TO `None` TO RUN ON ALL FILES!
        in_aws=IN_AWS,
    )
    # only run on actual website text
    .where(col("WARC-Type") == "conversion")
    # UTF-8 decode the text
    .with_column("text", col("warc_content").try_decode("utf-8"))
    .drop_null(col("text"))
    # extract the language & filter english pages only
    .with_column("warc_headers", json_load_warc_headers(col("warc_headers")))
    .with_column("language", col("warc_headers").struct.get("WARC-Identified-Content-Language"))
    .where(col("language") == "eng")
    # chunk text into sentences
    .into_batches(batch_size=EMBEDDING_BATCH_SIZE * 10)
    .with_column("sentences", chunker(col("text")))
    .with_column("text", col("sentences").struct.get("text"))
    .with_column("chunk_id", col("sentences").struct.get("chunk_id"))
    .exclude("sentences")
    # perform text embedding using the GPU
    .into_batches(batch_size=EMBEDDING_BATCH_SIZE)
    .with_column("embedding", embedder.embed_text(col("text")))
    # our final columns
    .select("WARC-Record-ID", "WARC-Target-URI", "WARC-Date", "chunk_id", "text", "embedding")
)
```

We can take a peek to see what this all looks like!

```python
df.show()
```

For a real run, we'd want to actually _save_ this output somewhere! You can do this with the `daft.DataFrame.write_parquet` method. This can take a local path or an S3 key. Daft will write partition files.
```python
start = datetime.now()
output = df.write_parquet("./local_chunked_cc_text_and_embeddings")
end = datetime.now()
print(f"Complete! Took {end-start} -- Wrote output partitions:\n{output}")
```
