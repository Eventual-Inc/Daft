# Filtering, Classifying, and Scoring Rows with Jev

Daft's `jev` functions let you ask plain-English questions about each row of a DataFrame: does this ticket come from an angry customer, which team should handle it, how luxurious is this product? Questions are answered by TypeSafe's Jev model, and the functions are modelled after DuckDB's `jev` community extension.

| Function                                        | Answers                                                       | Returns                      |
| ----------------------------------------------- | ------------------------------------------------------------- | ---------------------------- |
| [`jev`](#filtering-with-jev)                    | Does the row satisfy a condition?                             | `Boolean`                    |
| [`jev_prob`](#probabilities-with-jev_prob)      | How likely is it that the row satisfies a condition?          | `Float64` in [0, 1]          |
| [`jev_choice`](#classification-with-jev_choice) | Which of these options best answers a question about the row? | `String`                     |
| [`jev_score`](#scoring-with-jev_score)          | Where does the row fall on an ordered rubric?                 | `Float64` in [0, levels - 1] |

!!! tip "Choosing the Right Approach"
    - Use `jev` and `jev_prob` to filter or rank rows by a condition that is easy to state but hard to code
    - Use `jev_choice` and `jev_score` to label rows without hosting a model; use [`classify_text`](classify.md#text-classification) to run a classifier locally
    - Use [`prompt`](prompt.md) when you need free-form text, explanations, or structured outputs

## Setup

Install Daft with the `jev` extra and set your TypeSafe API key:

```bash
pip install "daft[jev]"
export TYPESAFE_API_KEY="..."
```

Unlike the other AI functions, the `jev` functions don't use a [provider](providers.md). Each function takes its settings as keyword arguments, falling back to environment variables:

| Argument   | Environment variable     | Default                   |
| ---------- | ------------------------ | ------------------------- |
| `api_key`  | `TYPESAFE_API_KEY`       | None; required            |
| `model`    | `TYPESAFE_DEFAULT_MODEL` | `jev-latest`              |
| `base_url` | `TYPESAFE_BASE_URL`      | `https://api.typesafe.ai` |
| `timeout`  |                          | 60 seconds per request    |

Settings are read when you build the expression, so workers on a distributed cluster don't need the environment variables.

## Filtering with `jev`

The `jev` function returns `True` for rows that satisfy a plain-English condition, which makes it a natural fit for `where`. To judge several columns at once, pack them into a struct with `to_struct`; the field names are visible to the model.

```python
import daft
from daft.functions import jev, to_struct

df = daft.from_pydict({
    "first": ["Hans", "Mei", "Giulia", "Kwame"],
    "last": ["Müller", "Chen", "Rossi", "Mensah"],
})

df = df.where(
    jev(to_struct(daft.col("first"), daft.col("last")), "the name is European")
)

df.show()
```

A row counts as `True` when its probability is at least `threshold`, which defaults to 0.5. Raise it to keep only confident matches:

```python
df.where(jev(daft.col("ticket"), "the customer is angry", threshold=0.9))
```

## Probabilities with `jev_prob`

The `jev_prob` function returns the probability behind `jev`, so you can rank rows or pick a threshold after looking at the data. `jev(row, condition, threshold)` is equivalent to `jev_prob(row, condition) >= threshold`.

```python
import daft
from daft.functions import jev_prob

df = daft.from_pydict({
    "ticket": [
        "This is the third time you've double charged me. I am furious and cancelling today!",
        "Thanks so much, the new dashboard is lovely.",
        "I was billed twice for my subscription this month.",
        "The app crashes every time I try to log in.",
    ],
})

df = df.with_column("p_angry", jev_prob(daft.col("ticket"), "the customer is angry"))

df.sort("p_angry", desc=True).show()
```

## Classification with `jev_choice`

The `jev_choice` function picks the most probable of a list of options as the answer to a question about each row. It takes between 1 and 255 options, and duplicate options are dropped.

```python
import daft
from daft.functions import jev_choice

df = daft.from_pydict({
    "ticket": [
        "I was billed twice for my subscription this month.",
        "The app crashes every time I try to log in.",
        "Someone logged into my account from another country.",
        "Do you offer discounts for teams of 50 or more?",
    ],
})

df = df.with_column(
    "team",
    jev_choice(
        daft.col("ticket"),
        "which team should handle this?",
        ["billing", "technical", "security", "sales"],
    ),
)

df.show()
```

## Scoring with `jev_score`

The `jev_score` function places each row on an ordered rubric of 2 to 10 levels, listed lowest first. The result is the index of a level, weighted by the model's probability for each level, so it can fall between levels: with the levels below, a score of 2.5 sits between `premium` and `luxury`.

```python
import daft
from daft.functions import jev_score

df = daft.from_pydict({
    "product": [
        "Plastic garden stool",
        "Solid oak dining chair",
        "Hand-stitched Italian leather armchair",
    ],
})

df = df.with_column(
    "tier",
    jev_score(
        daft.col("product"),
        "how luxurious is this product?",
        ["budget", "mid-range", "premium", "luxury"],
    ),
)

df.sort("tier", desc=True).show()
```

!!! note "Nulls and Batching"
    Null rows, including `NaN`, are never sent to the model and produce a null result. The remaining rows are sent 25 per request with up to 16 requests in flight, and requests that time out, are rate-limited, or fail on the server are retried.
