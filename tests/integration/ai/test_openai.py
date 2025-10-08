"""OpenAI Integration Tests.

Note:
    These tests require an OPENAI_API_KEY environment variable WITH credit.

Usage:
    pytest -m integration ./tests/integration/ai/test_openai.py --credentials
"""

from __future__ import annotations

import os
import time

import pytest

import daft
from daft.functions.ai import embed_text


@pytest.fixture(scope="module", autouse=True)
def skip_no_credential(pytestconfig):
    if not pytestconfig.getoption("--credentials"):
        pytest.skip(reason="OpenAI integration tests require the `--credentials` flag.")
    if os.environ.get("OPENAI_API_KEY") is None:
        pytest.skip(reason="OpenAI integration tests require the OPENAI_API_KEY environment variable.")


@pytest.fixture(scope="module", autouse=True)
def session(skip_no_credential):
    """Configures the session to be used for all tests."""
    with daft.session() as session:
        # the key is not explicitly needed, but was added with angry lookup for clarity.
        session.set_provider("openai", api_key=os.environ["OPENAI_API_KEY"])
        yield session


@pytest.mark.integration()
def test_embed_text_sanity_all_models(session):
    """This tests end-to-end doesn't throw for all models. It does not check outputs."""
    from daft.ai.openai.protocols.text_embedder import _models

    df = daft.from_pydict(
        {
            "text": [
                "Alice was beginning to get very tired of sitting by her sister on the bank.",
                "So she was considering in her own mind (as well as she could, for the hot day made her feel very sleepy and stupid),",
                "whether the pleasure of making a daisy-chain would be worth the trouble of getting up and picking the daisies,",
                "when suddenly a White Rabbit with pink eyes ran close by her.",
                "There was nothing so very remarkable in that;",
                "nor did Alice think it so very much out of the way to hear the Rabbit say to itself, 'Oh dear! Oh dear! I shall be late!'",
            ]
        }
    )

    # assert success for all models
    for model in _models.keys():
        df = df.with_column("embedding", embed_text(df["text"], model=model))
        df.collect()
        time.sleep(1)  # self limit to ~1 tps.
