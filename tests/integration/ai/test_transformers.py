"""Transformers integration tests (opt-in).

These tests download and run real HuggingFace models and are intentionally skipped
unless explicitly enabled.
"""

from __future__ import annotations

import os

import pytest

import daft
from daft.functions.ai import prompt


@pytest.fixture(scope="module", autouse=True)
def skip_unless_enabled():
    if os.environ.get("DAFT_HF_INTEGRATION") != "1":
        pytest.skip(reason="Set DAFT_HF_INTEGRATION=1 to run HuggingFace Transformers integration tests.")


@pytest.mark.integration()
def test_transformers_prompt_multimodal_qwen3_vl():
    """End-to-end multimodal prompt using a real HF VLM."""
    pytest.importorskip("transformers")
    torch = pytest.importorskip("torch")
    np = pytest.importorskip("numpy")

    # Avoid running a huge model on CPU-only environments by default.
    has_gpu = bool(
        torch.cuda.is_available() or getattr(torch.backends, "mps", None) and torch.backends.mps.is_available()
    )
    if not has_gpu:
        pytest.skip(reason="Multimodal VLM integration test requires CUDA or MPS.")

    is_cuda = bool(torch.cuda.is_available())
    is_mps = bool(getattr(torch.backends, "mps", None) and torch.backends.mps.is_available())

    model = "Qwen/Qwen3-VL-2B-Instruct"

    # Small test image: red square
    img = np.zeros((64, 64, 3), dtype=np.uint8)
    img[:, :, 0] = 255

    with daft.session() as session:
        session.set_provider("transformers")

        df = daft.from_pydict({"q": ["What color is dominant in this image?"], "img": [img]})
        df = df.with_column(
            "answer",
            prompt(
                [daft.col("q"), daft.col("img")],
                provider="transformers",
                model=model,
                max_new_tokens=16,
                # CUDA: use accelerate dispatching.
                # MPS: use explicit `.to("mps")` path (device_map is primarily for CUDA/accelerate).
                **({"device_map": "auto"} if is_cuda else {}),
                # Make sure we pick a dtype MPS supports (loader defaults float16 for mps/cuda).
                **({"torch_dtype": "float16"} if is_mps else {}),
            ),
        )

        out = df.to_pydict()["answer"][0]
        assert isinstance(out, str)
        assert len(out) > 0
