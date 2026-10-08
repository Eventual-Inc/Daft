"""Jev Integration Tests.

Note:
    These tests require a TYPESAFE_API_KEY environment variable WITH credit.

Usage:
    pytest -m integration ./tests/integration/ai/test_jev.py --credentials
"""

from __future__ import annotations

import os

import pytest

import daft
from daft.functions import jev, jev_choice, jev_prob, jev_score


@pytest.fixture(scope="module", autouse=True)
def skip_no_credential(pytestconfig):
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="Jev integration tests require the `--credentials` flag.")
    if os.environ.get("TYPESAFE_API_KEY") is None:
        pytest.skip(reason="Jev integration tests require the TYPESAFE_API_KEY environment variable.")


@pytest.fixture(scope="module")
def tickets():
    return daft.from_pydict(
        {
            "ticket": [
                "This is the third time you've double charged me. I am furious and cancelling today!",
                "Thanks so much, the new dashboard is lovely.",
                "I was billed twice for my subscription this month.",
                "The app crashes every time I try to log in.",
            ]
        }
    )


@pytest.mark.integration()
def test_jev_prob_separates_packed_rows(tickets):
    out = tickets.select(
        jev_prob(tickets["ticket"], "the customer is angry").alias("p"),
        jev(tickets["ticket"], "the customer is angry").alias("angry"),
    ).to_pydict()

    assert all(0.0 <= p <= 1.0 for p in out["p"])
    assert out["p"][0] > out["p"][1]
    assert out["angry"][0] is True
    assert out["angry"][1] is False


@pytest.mark.integration()
def test_jev_choice(tickets):
    teams = ["billing", "technical", "security", "sales"]
    out = tickets.select(jev_choice(tickets["ticket"], "which team should handle this?", teams)).to_pydict()

    assert out["ticket"][2] == "billing"
    assert out["ticket"][3] == "technical"


@pytest.mark.integration()
def test_jev_score():
    df = daft.from_pydict({"product": ["Plastic garden stool", "Hand-stitched Italian leather armchair"]})
    levels = ["budget", "mid-range", "premium", "luxury"]
    out = df.select(jev_score(df["product"], "how luxurious is this product?", levels)).to_pydict()

    assert all(0.0 <= s <= len(levels) - 1 for s in out["product"])
    assert out["product"][0] < out["product"][1]
