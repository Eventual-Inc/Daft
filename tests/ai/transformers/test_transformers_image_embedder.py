from __future__ import annotations

import pytest

pytest.importorskip("transformers")
pytest.importorskip("torch")
pytest.importorskip("PIL")


import time
from io import BytesIO
from types import SimpleNamespace

import numpy as np
import torch
from PIL import Image

import daft
from daft.ai.protocols import ImageEmbedderDescriptor
from daft.ai.transformers.protocols.image_embedder import _get_embeddings_from_output
from daft.ai.transformers.provider import TransformersProvider
from daft.ai.typing import EmbeddingDimensions
from daft.datatype import DataType
from daft.functions import decode_image, embed_image
from tests.benchmarks.conftest import IS_CI


def test_transformers_image_embedder_default():
    provider = TransformersProvider()
    descriptor = provider.get_image_embedder()
    assert isinstance(descriptor, ImageEmbedderDescriptor)
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == "openai/clip-vit-base-patch32"
    assert descriptor.get_dimensions() == EmbeddingDimensions(512, dtype=DataType.float32())


@pytest.mark.parametrize(
    "model_name, dimensions, run_model_in_ci",
    [
        ("openai/clip-vit-base-patch32", 512, True),
        ("openai/clip-vit-large-patch14", 768, False),
        ("openai/clip-vit-base-patch16", 512, True),
        ("openai/clip-vit-large-patch14-336", 768, False),
        # Non-CLIP vision backbone: no `get_image_features`, embeddings come from `forward`.
        ("facebook/dinov2-small", 384, False),
    ],
)
def test_transformers_image_embedder_other(model_name, dimensions, run_model_in_ci):
    mock_options = {"arg1": "val1", "arg2": "val2"}

    provider = TransformersProvider()
    descriptor = provider.get_image_embedder(model_name, **mock_options)
    assert isinstance(descriptor, ImageEmbedderDescriptor)
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == model_name
    assert descriptor.get_options() == mock_options

    if not IS_CI or run_model_in_ci:
        retry_delay = 5  # seconds
        max_retries = 5

        # retry instantiation because Hugging Face is flaky
        for attempt in range(max_retries):
            try:
                embedder = descriptor.instantiate()
            except OSError as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    raise e
            else:
                break

        # Test with a variety of image sizes and shapes that should be preprocessed correctly.
        # TODO(desmond): This doesn't work with greyscale images. I wonder if we should require users to cast
        # to RGB or if we want to handle this automagically.
        test_image1 = np.random.randint(0, 255, (100, 100, 3), dtype=np.uint8)
        test_image2 = np.random.randint(0, 255, (500, 10, 3), dtype=np.uint8)
        # test_image3 = np.random.randint(0, 255, (100, 100, 1), dtype=np.uint8)
        test_images = [test_image1, test_image2] * 8

        embeddings = embedder.embed_image(test_images)
        assert len(embeddings) == 16
        assert len(embeddings[0]) == dimensions
    else:
        assert descriptor.get_dimensions() == EmbeddingDimensions(dimensions, dtype=DataType.float32())


def _random_images(count: int) -> list[np.ndarray]:
    return [np.random.randint(0, 255, (32, 32, 3), dtype=np.uint8) for _ in range(count)]


def _stub_embedder(monkeypatch, model):
    """Instantiates a TransformersImageEmbedder against a stub model, without hitting Hugging Face."""
    from daft.ai.transformers.protocols import image_embedder as image_embedder_module

    class _StubModel:
        """Forwards to the stub model, but tolerates the `.to(device)` call like a real nn.Module."""

        def __init__(self, inner):
            self.__dict__["_inner"] = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def __call__(self, *args, **kwargs):
            return self._inner(*args, **kwargs)

        def to(self, device):
            return self

    stub_model = _StubModel(model)

    class _StubAutoModel:
        @staticmethod
        def from_pretrained(model_name_or_path, **kwargs):
            return stub_model

    class _StubAutoProcessor:
        @staticmethod
        def from_pretrained(model_name_or_path, **kwargs):
            def processor(images, return_tensors):
                return {"pixel_values": torch.zeros(len(images), 3, 224, 224)}

            return processor

    monkeypatch.setattr(image_embedder_module, "get_torch_device", lambda: torch.device("cpu"))
    monkeypatch.setattr(image_embedder_module, "AutoModel", _StubAutoModel)
    monkeypatch.setattr(image_embedder_module, "AutoProcessor", _StubAutoProcessor)
    return image_embedder_module.TransformersImageEmbedder("stub-model")


class _CLIPLikeModel:
    def __init__(self, pooled: bool):
        self.pooled = pooled

    def __call__(self, pixel_values):
        raise AssertionError("Expected get_image_features to take precedence over forward")

    def get_image_features(self, pixel_values):
        if self.pooled:
            return _PooledOutput(pixel_values.shape[0], 512)
        return torch.ones(pixel_values.shape[0], 512)


class _PooledOutput:
    def __init__(self, batch_size: int, dim: int):
        # Distinct values so tests can tell which field was used.
        self.pooler_output = torch.full((batch_size, dim), 1.0)
        self.last_hidden_state = torch.full((batch_size, 197, dim), 2.0)


class _DINOv2LikeModel:
    """Generic vision backbone: only `forward`, returns pooled + hidden states."""

    def __call__(self, pixel_values):
        return _PooledOutput(pixel_values.shape[0], 768)


class _HiddenStateOnlyOutput:
    def __init__(self, batch_size: int, dim: int, pooler_is_none: bool):
        if pooler_is_none:
            self.pooler_output = None
        cls_token = torch.full((batch_size, 1, dim), 3.0)
        self.last_hidden_state = torch.cat([cls_token, torch.zeros(batch_size, 196, dim)], dim=1)


class _NoPoolingHeadModel:
    def __init__(self, pooler_is_none: bool):
        self.pooler_is_none = pooler_is_none

    def __call__(self, pixel_values):
        return _HiddenStateOnlyOutput(pixel_values.shape[0], 384, self.pooler_is_none)


class _UnsupportedOutputModel:
    def __call__(self, pixel_values):
        return object()


@pytest.mark.parametrize("pooled", [False, True])
def test_transformers_image_embedder_prefers_get_image_features(monkeypatch, pooled):
    embedder = _stub_embedder(monkeypatch, _CLIPLikeModel(pooled))
    assert embedder.uses_image_features

    embeddings = embedder.embed_image(_random_images(4))
    assert len(embeddings) == 4
    assert len(embeddings[0]) == 512


def test_transformers_image_embedder_falls_back_to_forward(monkeypatch):
    embedder = _stub_embedder(monkeypatch, _DINOv2LikeModel())
    assert not embedder.uses_image_features

    embeddings = embedder.embed_image(_random_images(4))
    assert len(embeddings) == 4
    assert len(embeddings[0]) == 768
    # `pooler_output` wins over the CLS token.
    assert all(value == 1.0 for value in embeddings[0])


@pytest.mark.parametrize("pooler_is_none", [False, True])
def test_transformers_image_embedder_falls_back_to_cls_token(monkeypatch, pooler_is_none):
    embedder = _stub_embedder(monkeypatch, _NoPoolingHeadModel(pooler_is_none))
    assert not embedder.uses_image_features

    embeddings = embedder.embed_image(_random_images(2))
    assert len(embeddings) == 2
    assert len(embeddings[0]) == 384
    assert all(value == 3.0 for value in embeddings[0])


def test_transformers_image_embedder_raises_on_unsupported_output(monkeypatch):
    embedder = _stub_embedder(monkeypatch, _UnsupportedOutputModel())

    with pytest.raises(TypeError, match="Unable to derive image embeddings"):
        embedder.embed_image(_random_images(1))


@pytest.mark.parametrize(
    "output",
    [
        pytest.param(torch.zeros(2, 5, 8), id="3d-tensor"),
        pytest.param(torch.zeros(8), id="1d-tensor"),
        pytest.param([torch.zeros(2, 8)], id="tensor-list"),
        pytest.param(SimpleNamespace(pooler_output=torch.zeros(2, 5, 8)), id="3d-pooler"),
        pytest.param(SimpleNamespace(pooler_output=[[1.0]]), id="non-tensor-pooler"),
        pytest.param(SimpleNamespace(last_hidden_state=torch.zeros(2, 8, 2, 2)), id="spatial-hidden-state"),
        pytest.param(SimpleNamespace(last_hidden_state=torch.zeros(2, 8)), id="2d-hidden-state"),
        pytest.param(SimpleNamespace(last_hidden_state=torch.zeros(2, 0, 8)), id="empty-token-sequence"),
        pytest.param(SimpleNamespace(last_hidden_state=[[1.0]]), id="non-tensor-hidden-state"),
        pytest.param(SimpleNamespace(feature_maps=(torch.zeros(2, 8, 2, 2),)), id="feature-maps"),
    ],
)
def test_transformers_image_embedder_rejects_invalid_output(output):
    with pytest.raises(
        TypeError, match=f"Unable to derive image embeddings from model output of type {type(output).__name__}"
    ):
        _get_embeddings_from_output(output)


@pytest.mark.parametrize("model_type", ["dinov2", "vit", "swin", "clip"])
def test_transformers_image_embedder_local_dataframe(tmp_path, model_type):
    from transformers import (
        AutoModel,
        AutoProcessor,
        CLIPConfig,
        CLIPImageProcessor,
        CLIPProcessor,
        CLIPTokenizer,
        Dinov2Config,
        SwinConfig,
        ViTConfig,
        ViTImageProcessor,
    )

    vision_options = {
        "image_size": 32,
        "patch_size": 16,
        "hidden_size": 24,
        "num_hidden_layers": 1,
        "num_attention_heads": 3,
        "intermediate_size": 48,
    }
    configs = {
        "dinov2": Dinov2Config(**vision_options),
        "vit": ViTConfig(**vision_options),
        "swin": SwinConfig(image_size=32, patch_size=4, embed_dim=12, depths=[1, 1], num_heads=[3, 6], window_size=4),
        "clip": CLIPConfig(
            vision_config=vision_options,
            text_config={
                "vocab_size": 16,
                "hidden_size": 24,
                "num_hidden_layers": 1,
                "num_attention_heads": 3,
                "intermediate_size": 48,
            },
            projection_dim=16,
        ),
    }
    config = configs[model_type]
    model = AutoModel.from_config(config).eval()
    model.save_pretrained(tmp_path)
    if model_type == "clip":
        vocab_file = tmp_path / "vocab.json"
        merges_file = tmp_path / "merges.txt"
        vocab_file.write_text('{"<|startoftext|>": 0, "<|endoftext|>": 1}')
        merges_file.write_text("#version: 0.2\n")
        processor = CLIPProcessor(
            image_processor=CLIPImageProcessor(size={"shortest_edge": 32}, crop_size={"height": 32, "width": 32}),
            tokenizer=CLIPTokenizer(str(vocab_file), str(merges_file)),
        )
        dimensions = 16
    else:
        processor = ViTImageProcessor(size={"height": 32, "width": 32})
        dimensions = 24
    processor.save_pretrained(tmp_path)

    images = [Image.new("RGB", (37, 19), "red"), Image.new("RGB", (16, 41), "blue")]
    image_bytes = []
    for image in images:
        buffer = BytesIO()
        image.save(buffer, format="PNG")
        image_bytes.append(buffer.getvalue())

    loaded_processor = AutoProcessor.from_pretrained(tmp_path, use_fast=True, local_files_only=True)
    pixel_values = loaded_processor(images=images, return_tensors="pt")["pixel_values"]
    with torch.inference_mode():
        if model_type == "clip":
            expected = model.get_image_features(pixel_values)
            if not isinstance(expected, torch.Tensor):
                expected = expected.pooler_output
        else:
            expected = model(pixel_values).pooler_output

    df = daft.from_pydict({"image_bytes": image_bytes}).with_column("image", decode_image(daft.col("image_bytes")))
    df = df.with_column("emb", embed_image(daft.col("image"), provider="transformers", model=str(tmp_path)))
    expected_dtype = DataType.embedding(DataType.float32(), dimensions)
    assert df.schema()["emb"].dtype == expected_dtype
    df.collect()
    actual = np.asarray(df.to_pydict()["emb"])
    assert actual.shape == (2, dimensions)
    assert np.isfinite(actual).all()
    np.testing.assert_allclose(actual, expected.numpy(), rtol=1e-4, atol=1e-5)
