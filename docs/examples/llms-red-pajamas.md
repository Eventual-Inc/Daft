# Running LLMs on the Red Pajamas Dataset with Daft

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/embeddings/daft_tutorial_embeddings_stackexchange.ipynb)

In this example, we'll load the Red Pajamas dataset and perform similarity search on Stack Exchange questions using language models and embeddings.

Daft makes working with complex data easy. This demonstration will show an end-to-end example of data processing with embeddings in Daft. We will:

1. Load a large dataset
2. Compute embeddings
3. Use the embeddings for semantic similarity
4. Inspect the results and then and write them out

**Scenario**: There are a lot of questions on StackExchange. Unfortunately, the vast majority of questions do not have high visibility or good answers. But, a similar question with a high quality answer may already exist elsewhere on the site.

We would like to go through all the questions on StackExchange and **associate each question with another question that is highly rated**.

## Step 0: Dependencies and configuration

```bash
pip install "daft[aws]" sentence-transformers accelerate
```

## Step 1: Load the dataset

We will use a sample of the **StackExchange crawl from the [RedPajamas dataset](https://www.google.com/url?q=https%3A%2F%2Fhuggingface.co%2Fdatasets%2Ftogethercomputer%2FRedPajama-Data-1T)**. It is 75GB of jsonl files.

**Note**: This demo runs best on a cluster with many GPUs available. Information on how to connect Daft to a cluster is available on our [scaling up guide](../distributed.md).

If running on a single node, you can use the provided subsample of the data, which is 75MB in size. If you like, you can also truncate either dataset to a desired number of rows using `df.limit`.

```python
import daft

SAMPLE_DATA_PATH = "s3://daft-public-data/redpajama-1t-sample/stackexchange_sample.jsonl"
IO_CONFIG = daft.io.IOConfig(
    s3=daft.io.S3Config(anonymous=True, region_name="us-west-2")
)  # Use anonymous-mode for accessing AWS S3

df = daft.read_json(SAMPLE_DATA_PATH, io_config=IO_CONFIG)
```

## Step 2: Compute embeddings

We can see there is a text column that holds the question answer text and a meta column with metadata.

Let's **compute the embeddings of our text**. We start by putting our model (SentenceTransformers) into a **[Daft User-Defined Function (UDF)](../custom-code/udfs.md)**.

```python
import torch

MODEL_NAME = "all-MiniLM-L6-v2"
device = "cuda" if torch.cuda.is_available() else "cpu"


@daft.udf(return_dtype=daft.DataType.python())
class EncodingUDF:
    def __init__(self):
        from sentence_transformers import SentenceTransformer

        self.model = SentenceTransformer(MODEL_NAME, device=device)

    def __call__(self, text_col):
        return [self.model.encode(text, convert_to_tensor=True) for text in text_col.to_pylist()]
```

Then, we can just call the UDF to run the model.

```python
df = df.with_column("embedding", EncodingUDF(df["text"]))
```

Pause and notice how easy it was to write this.

In particular, we are not forced to do any sort of unwieldy type coercion on the embedding result; instead, we can return the result as-is, whatever it is, even when running Daft in a cluster. Daft dataframes can hold a wide range of data types and also grants you the flexibility of Python's dynamic typing when necessary.

Next, let's also **extract the URL and score**:

```python
df = df.select(
    df["embedding"],
    df["meta"].struct.get("url"),
    df["meta"].struct.get("question_score"),
)
```

and wait for all the results to finish computing:

```python
df = df.collect()
embeddings_df = df
print("Embeddings complete!")
```

## Step 3: Semantic similarity

Let's **get the top questions**. We will use `df.sort` to sort by score and then `df.limit` to grab some fraction of the top.

```python
import math

NUM_TOP_QUESTIONS = math.ceil(math.sqrt(len(df)))

top_questions = (df.sort(df["question_score"], desc=True).limit(NUM_TOP_QUESTIONS)).to_pydict()
```

Now we will **take each regular question and find a related top question**. For this we will need to do a similarity search. Let's do that within a Daft UDF.

```python
@daft.udf(
    return_dtype=daft.DataType.struct(
        {
            "related_top_question": daft.DataType.string(),
            "similarity": daft.DataType.float64(),
        }
    )
)
def similarity_search(embedding_col, top_embeddings, top_urls):
    if len(embedding_col) == 0:
        return []

    from sentence_transformers import util

    # Tensor prep
    query_embedding_t = torch.stack(embedding_col.to_pylist())
    if torch.cuda.is_available():
        query_embedding_t = query_embedding_t.to("cuda")
        top_embeddings = top_embeddings.to("cuda")

    # Do semantic search
    results = util.semantic_search(query_embedding_t, top_embeddings, top_k=1)

    # Extract URL and score from search results
    results = [res[0] for res in results]
    results = [
        {
            "related_top_question": top_urls[res["corpus_id"]],
            "similarity": res["score"],
        }
        for res in results
    ]
    return results


import torch

df = df.with_column(
    "search_result",
    similarity_search(
        df["embedding"],
        top_embeddings=torch.stack(top_questions["embedding"]),
        top_urls=top_questions["url"],
    ),
)

df = df.select(
    df["url"],
    df["question_score"],
    df["search_result"]["related_top_question"].alias("related_top_question"),
    df["search_result"]["similarity"].alias("similarity"),
)
```

## Step 4: Inspect and write results

Did the matching work well? Let's take a peek at our best results to see if they make sense.

```python
df = df.where(df["similarity"] < 0.99)  # To ignore duplicate questions.
df = df.sort(df["similarity"], desc=True)
df.show()
```

On the left hand side is an average question without much activity. The link in the right hand side contains a similar question that already has some high quality answers. Success!

Finally, we will probably want to save the results for future use. Let's write them out to parquet files locally.

```python
df.write_parquet("question_matches.pq").to_pydict()
```

## Conclusion

We have shown a simple example of a complex data processing workflow. It involved typical tabular operations like sort and filter. But, we also had to do interleave some pretty interesting things with complex data: we created, stored, and searched across embeddings.

**Daft** is a data processing framework that allows you to do express these things easily, while also scaling up to large clusters right out of the box.
