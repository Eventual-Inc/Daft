# Daft's Four UDF Pattern Tutorial

[Run this tutorial on Google Colab](https://colab.research.google.com/github/Eventual-Inc/Daft/blob/main/tutorials/udf_patterns/udf_patterns.ipynb)

One notebook, four patterns, a single cohesive dataset. Each section starts with a problem, shows the pattern that solves it, runs the code, and tells you when to reach for that pattern again.

| I need to... | Pattern | Decorator |
|---|---|---|
| Transform each row with custom logic | Row-wise | `@daft.func` |
| Produce multiple rows from one input | Generator | `@daft.func` + `Iterator[T]` |
| Call external services concurrently | Async | `@daft.func` + `async def` |
| Reuse expensive resources across rows | Stateful | `@daft.cls` + `__init__` |

## Setup

All you need is `daft`. Install it if you haven't already.

```bash
pip install daft aiohttp
```

```python
import daft
```

## Our Dataset

We'll work with a small dataset of support tickets throughout the notebook. Each pattern solves a different problem on this same data.

```python
tickets = daft.from_pydict({
    "ticket_id": [1, 2, 3, 4, 5],
    "customer_email": [
        "Alice+work@Gmail.COM",
        "  bob@example.org  ",
        "CAROL+spam@Yahoo.com",
        "dave.jones@Company.IO",
        "EVE+newsletter@Outlook.COM",
    ],
    "subject": [
        "Can't log in after password reset",
        "Billing charged twice this month",
        "Feature request: dark mode and custom themes for the dashboard",
        "App crashes on startup every time I open the settings page since the last update",
        "How do I export my data to CSV or JSON format for external analysis tools",
    ],
    "body": [
        "I reset my password yesterday but the new password doesn't work. I've tried clearing cookies and using incognito mode. Still locked out. This is blocking my work.",
        "I was charged $49.99 twice on March 1st. Transaction IDs: TXN-001 and TXN-002. Please refund the duplicate charge. I've attached my bank statement as proof.",
        "Would love to see dark mode added to the dashboard. Also, custom color themes would be great for accessibility. Several team members have mentioned this. It would really help with long coding sessions late at night.",
        "Since updating to v2.3.1, the app crashes immediately when I navigate to Settings > Account > Preferences. I'm on macOS 14.2, Chrome 120. Console shows a null reference error in the preferences module. Happens every single time.",
        "I need to export my usage data for the last 6 months. The dashboard only shows charts but I need the raw numbers in CSV or JSON. Is there an API endpoint for this? Our analytics team needs this data for a quarterly review next week.",
    ],
    "webhook_url": [
        "https://httpbin.org/status/200",
        "https://httpbin.org/status/200",
        "https://httpbin.org/status/404",
        "https://httpbin.org/status/200",
        "https://httpbin.org/status/500",
    ],
})

tickets.show()
```

---

## Pattern 1: Row-wise -- clean every row with zero setup

**Problem**: You need custom logic on every row -- normalize an email, parse a field, validate an input -- and the built-in expressions don't cover it.

**Pattern**: `@daft.func` with type hints. One row in, one row out. Write regular Python. Daft handles the rest.

```python
@daft.func
def normalize_email(raw: str) -> str:
    """Strip whitespace, lowercase, remove plus-aliases."""
    local, domain = raw.strip().lower().split("@")
    local = local.split("+")[0]  # remove +alias
    return f"{local}@{domain}"


cleaned = tickets.select(
    tickets["ticket_id"],
    tickets["customer_email"],
    normalize_email(tickets["customer_email"]).alias("clean_email"),
)
cleaned.show()
```

That's it. No `return_dtype`, no schema declarations, no special imports. Daft infers the return type from your type hint.

Let's do another one -- categorize tickets by keyword matching:

```python
@daft.func
def categorize_ticket(subject: str) -> str:
    """Assign a category based on keywords in the subject line."""
    subject_lower = subject.lower()
    if any(kw in subject_lower for kw in ["crash", "error", "bug", "broken"]):
        return "bug"
    elif any(kw in subject_lower for kw in ["feature", "request", "add", "would like"]):
        return "feature_request"
    elif any(kw in subject_lower for kw in ["billing", "charge", "payment", "refund"]):
        return "billing"
    elif any(kw in subject_lower for kw in ["login", "password", "log in", "locked"]):
        return "auth"
    else:
        return "general"


categorized = tickets.select(
    tickets["ticket_id"],
    tickets["subject"],
    categorize_ticket(tickets["subject"]).alias("category"),
)
categorized.show()
```

**When to use it**: Your function takes a single row and returns a single value. The logic is pure Python -- no external services, no model weights, no variable-length output. This is the pattern for data cleaning, validation, and field-level transformations.

---

## Pattern 2: Generator -- one input becomes many rows

**Problem**: Each input row needs to produce a *variable number* of output rows. Splitting a document into chunks, tokenizing text into sentences, expanding nested records. You don't know ahead of time how many outputs each input will produce.

**Pattern**: `@daft.func` returning `Iterator[T]`. Yield as many rows as the data demands.

```python
from typing import Iterator


@daft.func
def split_into_sentences(text: str) -> Iterator[str]:
    """Split text into sentences. Each sentence becomes its own row."""
    import re
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    for sentence in sentences:
        if sentence:
            yield sentence


sentences = tickets.select(
    tickets["ticket_id"],
    split_into_sentences(tickets["body"]).alias("sentence"),
)
sentences.show()
```

Notice that `ticket_id` is automatically broadcast to match the number of sentences each ticket body produces. No `explode()` needed.

Here's a more practical example -- chunking text for a RAG pipeline:

```python
@daft.func
def chunk_text(text: str, max_words: int = 20) -> Iterator[str]:
    """Split text into word-count-limited chunks for embedding."""
    words = text.split()
    for i in range(0, len(words), max_words):
        chunk = " ".join(words[i:i + max_words])
        yield chunk


chunks = tickets.select(
    tickets["ticket_id"],
    chunk_text(tickets["body"]).alias("chunk"),
)
chunks.show()
```

**When to use it**: One-to-many transformations. Document chunking for RAG pipelines, audio segmentation, tokenization, expanding nested structures -- anywhere `yield` is the natural way to express "this input produces N outputs." No intermediate lists, no `explode()`, no memory spike from materializing everything at once.

---

## Pattern 3: Async -- hit APIs without waiting in line

**Problem**: Your function calls an external service -- a webhook, a geocoder, a model endpoint. Sequential execution means each row waits for the previous one to finish. At 10,000 rows, that's hours of idle waiting.

**Pattern**: `async def` with `@daft.func`. Daft overlaps the I/O automatically.

```python
@daft.func
async def ping_webhook(url: str) -> int:
    """Hit a webhook URL and return the HTTP status code."""
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            return resp.status


webhook_results = tickets.select(
    tickets["ticket_id"],
    tickets["webhook_url"],
    ping_webhook(tickets["webhook_url"]).alias("status_code"),
)
webhook_results.show()
```

You write `async def` and `await` exactly as you would outside Daft. The concurrency is automatic -- no thread pools, no executor boilerplate.

Here's another example -- looking up ticket categories from an external API:

```python
@daft.func
async def lookup_response_time_sla(category: str) -> str:
    """Simulate an async SLA lookup based on ticket category.

    In production, this would hit an internal API or database.
    Here we simulate the async I/O with a local mapping.
    """
    import asyncio
    # Simulate network latency
    await asyncio.sleep(0.1)
    sla_map = {
        "bug": "4 hours",
        "billing": "2 hours",
        "auth": "1 hour",
        "feature_request": "5 business days",
        "general": "24 hours",
    }
    return sla_map.get(category, "24 hours")


# First categorize, then look up SLAs
with_sla = tickets.select(
    tickets["ticket_id"],
    tickets["subject"],
    categorize_ticket(tickets["subject"]).alias("category"),
).select(
    daft.col("ticket_id"),
    daft.col("subject"),
    daft.col("category"),
    lookup_response_time_sla(daft.col("category")).alias("sla"),
)
with_sla.show()
```

**When to use it**: Any I/O-bound workload -- API calls, database lookups, webhook triggers, model endpoints behind a REST API. Write standard `async`/`await` Python. Daft handles the concurrency.

---

## Pattern 4: Stateful -- load a resource once, use it on every row

**Problem**: Loading an expensive resource for every row (or every batch) is killing your pipeline. A 2 GB model, a database connection pool, an API client with auth -- you need to initialize once and reuse across rows.

**Pattern**: `@daft.cls` with `__init__` for setup and `__call__` for processing. The resource loads once per worker and stays in memory for every row that worker handles.

No other distributed data framework has this.

```python
@daft.cls
class KeywordExtractor:
    """Extract keywords from text using a pre-loaded keyword set.

    In production, __init__ would load a model, open a DB connection,
    or authenticate with an API. The key: it runs once per worker.
    """
    def __init__(self):
        # This runs ONCE per worker -- not once per row.
        # Imagine loading a spaCy model or a TF-IDF vectorizer here.
        self.keywords = {
            "urgent": ["crash", "locked", "blocking", "immediately", "every time", "broken"],
            "billing": ["charged", "refund", "payment", "transaction", "invoice"],
            "feature": ["would love", "would be great", "add", "request", "suggestion"],
            "data": ["export", "csv", "json", "api", "raw", "download"],
        }
        print("KeywordExtractor initialized (runs once per worker)")

    def __call__(self, text: str) -> str:
        """This runs on every row, reusing the keyword set from __init__."""
        text_lower = text.lower()
        matched = []
        for category, terms in self.keywords.items():
            if any(term in text_lower for term in terms):
                matched.append(category)
        return ", ".join(matched) if matched else "none"


extractor = KeywordExtractor()

extracted = tickets.select(
    tickets["ticket_id"],
    tickets["subject"],
    extractor(tickets["body"]).alias("matched_categories"),
)
extracted.show()
```

Notice the print statement -- `"KeywordExtractor initialized"` appears once, not five times. That's the point. `__init__` runs once per worker. `__call__` runs on every row.

In production, this pattern is how you'd load a sentiment model, an embedding model, or any resource that's expensive to initialize:

```python
# Production example -- what this pattern looks like with a real model.
# (Not runnable without transformers installed -- shown for illustration.)
#
# @daft.cls(gpus=1)
# class SentimentClassifier:
#     def __init__(self):
#         from transformers import pipeline
#         self.pipe = pipeline(
#             "sentiment-analysis",
#             model="distilbert-base-uncased-finetuned-sst-2-english",
#             device="cuda",
#         )
#         # Model loaded once. 2GB stays in GPU memory.
#
#     def __call__(self, text: str) -> str:
#         return self.pipe(text)[0]["label"]
#
# classifier = SentimentClassifier()
# df.select(classifier(df["text"]).alias("sentiment"))
```

**When to use it**: Any workload with expensive initialization -- model loading, database connection pools, API clients with authentication. `__init__` runs once per worker; `__call__` runs on every row. Add `gpus=1` for GPU allocation, `max_concurrency` to cap parallel instances, `use_process=True` to escape the GIL.

---

## Bonus: Batch processing with `@daft.func.batch`

When you need to operate on entire columns at once (NumPy vectorization, pandas operations, bulk API calls), use the batch variant.

```python
@daft.func.batch(return_dtype=daft.DataType.int64())
def word_count_batch(texts: daft.Series) -> list:
    """Count words in each text -- operating on the entire batch at once."""
    return [len(text.split()) for text in texts.to_pylist()]


word_counts = tickets.select(
    tickets["ticket_id"],
    tickets["subject"],
    word_count_batch(tickets["body"]).alias("body_word_count"),
)
word_counts.show()
```

Batch UDFs receive `daft.Series` objects instead of individual values. Use them when your operation benefits from vectorization (NumPy, pandas) or when an external library expects arrays.

---

## Putting It All Together

Let's build a mini pipeline that chains multiple UDF patterns on our ticket data:

```python
# Pipeline: normalize emails, categorize, extract keywords, count words

pipeline_result = tickets.select(
    tickets["ticket_id"],
    normalize_email(tickets["customer_email"]).alias("email"),
    tickets["subject"],
    categorize_ticket(tickets["subject"]).alias("category"),
    extractor(tickets["body"]).alias("keywords"),
    word_count_batch(tickets["body"]).alias("body_words"),
)

pipeline_result.show()
```

Row-wise, stateful, and batch UDFs -- all composing in a single `.select()`. This same code runs locally on your laptop or distributed across a Ray cluster. Set `daft.set_runner_ray("ray://cluster:10001")` and nothing else changes.

---

## Pick Your Pattern

| I need to... | Pattern | Decorator | Example in this notebook |
|---|---|---|---|
| Transform each row with custom logic | Row-wise | `@daft.func` | `normalize_email`, `categorize_ticket` |
| Produce multiple rows from one input | Generator | `@daft.func` + `Iterator[T]` | `split_into_sentences`, `chunk_text` |
| Call external services concurrently | Async | `@daft.func` + `async def` | `ping_webhook`, `lookup_response_time_sla` |
| Reuse expensive resources across rows | Stateful | `@daft.cls` + `__init__` | `KeywordExtractor` |
| Vectorized batch operations | Batch | `@daft.func.batch` | `word_count_batch` |

Your Python. Daft's scale.

---

**Resources:**
- [`@daft.func` docs](https://docs.daft.ai/en/stable/custom-code/func/)
- [`@daft.cls` docs](https://docs.daft.ai/en/stable/custom-code/cls/)
- [Migration guide](https://docs.daft.ai/en/stable/custom-code/migration/) (if you're on the legacy `@daft.udf` API)
