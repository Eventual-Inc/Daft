"""Jev Functions.

Plain-English predicates, classification, and scoring over rows, answered by TypeSafe's Jev model.
Modelled after DuckDB's `jev` community extension.
"""

from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any

from daft.datatype import DataType
from daft.dependencies import typesafe_sdk
from daft.expressions import lit
from daft.functions.str import serialize
from daft.udf import cls as daft_cls
from daft.udf import method

if TYPE_CHECKING:
    from collections.abc import Callable

    from daft import Series
    from daft.expressions import Expression

_BATCH_SIZE = 25
_MAX_CONCURRENCY = 16
_MAX_RETRIES = 6  # the SDK retries 408/429/5xx
_BACKOFF_MAX = 8.0
_TIMEOUT = None


@daft_cls(max_concurrency=_MAX_CONCURRENCY, max_retries=0)
class _Jev:
    def __init__(self, api_key: str, model: str, base_url: str, timeout: float):
        retry = typesafe_sdk.RetryPolicy(
            max_retries=_MAX_RETRIES,
            backoff_max=_BACKOFF_MAX,
            timeout=_TIMEOUT,
        )
        self.client = typesafe_sdk.AsyncTypeSafeClient(
            api_key=api_key,
            model=model,
            base_url=base_url,
            timeout=timeout,
            retry=retry,
        )

    async def _ask(
        self,
        rows: Series,
        state: dict[str, Any],
        question: dict[str, Any],
        instructions: Callable[[int], str],
    ) -> list[Any]:
        """Ask `question` about every non-null row in one request; null rows get a null answer."""
        # NaN serializes to JSON null, so it is treated as a null row too.
        parsed = [None if r is None else json.loads(r) for r in rows.to_pylist()]
        present = [r for r in parsed if r is not None]
        if not present:
            return [None] * len(parsed)
        questions: dict[str, Any] = {
            f"r{k}": {**question, "instructions": instructions(k)} for k in range(len(present))
        }
        response = await self.client.system_one(state={**state, "rows": present}, questions=questions)
        if missing := [name for name in questions if name not in response.answers]:
            raise RuntimeError(f"TypeSafe response from model {response.model!r} has no answers for {missing}")
        answers = iter(response.answers[name] for name in questions)
        return [None if r is None else next(answers) for r in parsed]

    @method.batch(return_dtype=DataType.float64(), batch_size=_BATCH_SIZE)
    async def noul(self, rows: Series, condition: str) -> list[float | None]:
        answers = await self._ask(
            rows,
            {"condition": condition},
            {"type": "noul"},
            lambda k: f"Does the record rows[{k}] satisfy the condition stated in condition?",
        )
        return [None if a is None else a.noul for a in answers]

    @method.batch(return_dtype=DataType.string(), batch_size=_BATCH_SIZE)
    async def choice(self, rows: Series, question: str, options: list[str]) -> list[str | None]:
        answers = await self._ask(
            rows,
            {},
            {"type": "choice", "criteria": dict.fromkeys(options)},
            lambda k: f"For the record rows[{k}]: {question}",
        )
        return [None if a is None else a.choice for a in answers]

    @method.batch(return_dtype=DataType.float64(), batch_size=_BATCH_SIZE)
    async def score(self, rows: Series, question: str, levels: list[str]) -> list[float | None]:
        answers = await self._ask(
            rows,
            {},
            {"type": "score", "criteria": levels},
            lambda k: f"Rate the record rows[{k}]: {question}",
        )
        return [None if a is None else a.score for a in answers]


# Returns Any because mypy can't see that @daft.cls methods take and return Expressions.
def _client(api_key: str | None, model: str | None, base_url: str | None, timeout: float) -> Any:
    if not typesafe_sdk.module_available():
        raise ImportError("The jev functions require typesafe-sdk. Please install it with: pip install 'daft[jev]'")
    # Resolved on the driver so every worker uses the same settings, with or without the env vars.
    c = typesafe_sdk.constants
    api_key = api_key or _env(c.API_KEY_ENV)
    if not api_key:
        raise ValueError(
            f"No TypeSafe API key was provided. Pass api_key or set the {c.API_KEY_ENV} environment variable."
        )
    model = model or _env(c.DEFAULT_MODEL_ENV) or c.DEFAULT_MODEL
    base_url = base_url or _env(c.BASE_URL_ENV) or c.DEFAULT_BASE_URL
    return _Jev(api_key, model, base_url, timeout)


def _env(name: str) -> str | None:
    return os.environ.get(name, "").strip() or None


def jev_prob(
    row: Expression,
    condition: str,
    *,
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: float = 60.0,
) -> Expression:
    """Probability that each row satisfies a plain-English condition.

    Args:
        row (Expression): The value to judge. Pass `to_struct(...)` to judge several columns; field names are visible to the model.
        condition (str): A plain-English condition, e.g. "the customer is angry".
        model (str | None): The Jev model. Defaults to `TYPESAFE_DEFAULT_MODEL`, else `jev-latest`.
        api_key (str | None): TypeSafe API key. Defaults to `TYPESAFE_API_KEY`.
        base_url (str | None): API root, e.g. to route through a proxy. Defaults to `TYPESAFE_BASE_URL`, else `https://api.typesafe.ai`.
        timeout (float): Seconds allowed for each request, which carries up to 25 rows. Defaults to 60.

    Returns:
        Expression: A Float64 probability in [0, 1], or null where `row` is null.

    Examples:
        >>> import daft
        >>> from daft.functions import jev_prob
        >>> df = daft.from_pydict({"ticket": ["I was charged twice!!", "Thanks, all sorted."]})
        >>> df = df.with_column("p_angry", jev_prob(df["ticket"], "the customer is angry"))  # doctest: +SKIP
        >>> df.sort("p_angry", desc=True).show()  # doctest: +SKIP
    """
    return _client(api_key, model, base_url, timeout).noul(serialize(row, format="json"), condition)


def jev(
    row: Expression,
    condition: str,
    threshold: float = 0.5,
    *,
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: float = 60.0,
) -> Expression:
    """Whether each row satisfies a plain-English condition, i.e. `jev_prob(row, condition) >= threshold`.

    Args:
        row (Expression): The value to judge. Pass `to_struct(...)` to judge several columns; field names are visible to the model.
        condition (str): A plain-English condition, e.g. "the name is European".
        threshold (float): The minimum probability that counts as true. Defaults to 0.5.
        model (str | None): The Jev model. Defaults to `TYPESAFE_DEFAULT_MODEL`, else `jev-latest`.
        api_key (str | None): TypeSafe API key. Defaults to `TYPESAFE_API_KEY`.
        base_url (str | None): API root, e.g. to route through a proxy. Defaults to `TYPESAFE_BASE_URL`, else `https://api.typesafe.ai`.
        timeout (float): Seconds allowed for each request, which carries up to 25 rows. Defaults to 60.

    Returns:
        Expression: A Boolean, or null where `row` is null.

    Examples:
        >>> import daft
        >>> from daft.functions import jev, to_struct
        >>> df = daft.from_pydict({"first": ["Hans", "Mei"], "last": ["Müller", "Chen"]})
        >>> df.where(jev(to_struct(df["first"], df["last"]), "the name is European")).show()  # doctest: +SKIP
    """
    return jev_prob(row, condition, model=model, api_key=api_key, base_url=base_url, timeout=timeout) >= lit(threshold)


def jev_choice(
    row: Expression,
    question: str,
    options: list[str],
    *,
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: float = 60.0,
) -> Expression:
    """The most probable of `options` as the answer to `question` for each row.

    Args:
        row (Expression): The value to classify. Pass `to_struct(...)` to classify on several columns.
        question (str): A plain-English question, e.g. "which team should handle this?".
        options (list[str]): Between 1 and 255 distinct candidate answers; duplicates are dropped.
        model (str | None): The Jev model. Defaults to `TYPESAFE_DEFAULT_MODEL`, else `jev-latest`.
        api_key (str | None): TypeSafe API key. Defaults to `TYPESAFE_API_KEY`.
        base_url (str | None): API root, e.g. to route through a proxy. Defaults to `TYPESAFE_BASE_URL`, else `https://api.typesafe.ai`.
        timeout (float): Seconds allowed for each request, which carries up to 25 rows. Defaults to 60.

    Returns:
        Expression: A String drawn from `options`, or null where `row` is null.

    Examples:
        >>> import daft
        >>> from daft.functions import jev_choice
        >>> df = daft.from_pydict({"ticket": ["I was charged twice", "The app crashes on login"]})
        >>> teams = ["billing", "technical", "security", "sales"]
        >>> df.with_column(
        ...     "team", jev_choice(df["ticket"], "which team should handle this?", teams)
        ... ).show()  # doctest: +SKIP
    """
    options = list(dict.fromkeys(options))
    if not 1 <= len(options) <= 255:
        raise ValueError(f"jev_choice requires between 1 and 255 options, got {len(options)}")
    return _client(api_key, model, base_url, timeout).choice(serialize(row, format="json"), question, options)


def jev_score(
    row: Expression,
    question: str,
    levels: list[str],
    *,
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: float = 60.0,
) -> Expression:
    """Where each row falls on an ordered rubric, as a probability-weighted level index.

    Args:
        row (Expression): The value to score. Pass `to_struct(...)` to score on several columns.
        question (str): A plain-English question, e.g. "how luxurious is this product?".
        levels (list[str]): Between 2 and 10 rubric levels, lowest first.
        model (str | None): The Jev model. Defaults to `TYPESAFE_DEFAULT_MODEL`, else `jev-latest`.
        api_key (str | None): TypeSafe API key. Defaults to `TYPESAFE_API_KEY`.
        base_url (str | None): API root, e.g. to route through a proxy. Defaults to `TYPESAFE_BASE_URL`, else `https://api.typesafe.ai`.
        timeout (float): Seconds allowed for each request, which carries up to 25 rows. Defaults to 60.

    Returns:
        Expression: A Float64 in [0, len(levels) - 1] that may fall between levels, or null where `row` is null.

    Examples:
        >>> import daft
        >>> from daft.functions import jev_score
        >>> df = daft.from_pydict({"product": ["Plastic stool", "Hand-stitched leather armchair"]})
        >>> levels = ["budget", "mid-range", "premium", "luxury"]
        >>> df = df.with_column(
        ...     "tier", jev_score(df["product"], "how luxurious is this product?", levels)
        ... )  # doctest: +SKIP
        >>> df.sort("tier", desc=True).show()  # doctest: +SKIP
    """
    if not 2 <= len(levels) <= 10:
        raise ValueError(f"jev_score requires between 2 and 10 levels, got {len(levels)}")
    return _client(api_key, model, base_url, timeout).score(serialize(row, format="json"), question, list(levels))
