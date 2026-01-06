"""Tests for Transformers configuration and partitioning logic.

Tests the typing module, PartitionedConfig, and partition_options().
"""

from __future__ import annotations

import pickle

import pytest

from daft.ai.transformers.protocols.prompter.model_loader import (
    PartitionedConfig,
    TransformersPrompterModelLoader,
)
from daft.ai.transformers.typing import (
    CHAT_TEMPLATE_KEYS,
    MODEL_LOADING_KEYS,
    UDF_KEYS,
)

# Check if transformers is available
transformers = pytest.importorskip("transformers")


def test_model_loading_keys_are_frozenset():
    """Key sets should be immutable."""
    assert isinstance(MODEL_LOADING_KEYS, frozenset)
    assert isinstance(CHAT_TEMPLATE_KEYS, frozenset)
    assert isinstance(UDF_KEYS, frozenset)


def test_model_loading_keys_contents():
    """Verify expected model loading keys are present."""
    expected = {
        "trust_remote_code",
        "torch_dtype",
        "device_map",
        "quantization_config",
        "attn_implementation",
    }
    assert expected.issubset(MODEL_LOADING_KEYS)


def test_chat_template_keys_contents():
    """Verify expected chat template keys are present."""
    expected = {
        "tokenize",
        "add_generation_prompt",
        "return_tensors",
        "return_dict",
        "continue_final_message",
    }
    assert expected.issubset(CHAT_TEMPLATE_KEYS)


def test_udf_keys_contents():
    """Verify expected UDF keys are present."""
    expected = {"concurrency", "num_gpus", "max_retries", "on_error", "batch_size"}
    assert expected == UDF_KEYS


def test_key_sets_are_disjoint():
    """Key sets should not overlap."""
    sets = [MODEL_LOADING_KEYS, CHAT_TEMPLATE_KEYS, UDF_KEYS]
    for i, s1 in enumerate(sets):
        for s2 in sets[i + 1 :]:
            overlap = s1 & s2
            assert len(overlap) == 0, f"Keys overlap: {overlap}"


def test_create_partitioned_config():
    """Test basic creation."""
    config = PartitionedConfig(
        model_kwargs={"trust_remote_code": True},
        processor_kwargs={"trust_remote_code": True},
        generation_kwargs={"max_new_tokens": 100},
        chat_template_kwargs={"tokenize": True, "add_generation_prompt": True},
        quantization_kwargs=None,
    )

    assert config.model_kwargs == {"trust_remote_code": True}
    assert config.generation_kwargs == {"max_new_tokens": 100}
    assert config.chat_template_kwargs == {"tokenize": True, "add_generation_prompt": True}
    assert config.quantization_kwargs is None


def test_partitioned_config_with_quantization():
    """Test creation with quantization kwargs."""
    config = PartitionedConfig(
        model_kwargs={},
        processor_kwargs={},
        generation_kwargs={},
        chat_template_kwargs={},
        quantization_kwargs={"load_in_4bit": True},
    )

    assert config.quantization_kwargs == {"load_in_4bit": True}


def test_partitioned_config_is_picklable():
    """PartitionedConfig must be serializable for distributed execution."""
    config = PartitionedConfig(
        model_kwargs={"trust_remote_code": True, "torch_dtype": "float16"},
        processor_kwargs={"trust_remote_code": True},
        generation_kwargs={"max_new_tokens": 100, "temperature": 0.7},
        chat_template_kwargs={"tokenize": True, "return_dict": True},
        quantization_kwargs={"load_in_4bit": True},
    )

    pickled = pickle.dumps(config)
    unpickled = pickle.loads(pickled)

    assert unpickled.model_kwargs == config.model_kwargs
    assert unpickled.generation_kwargs == config.generation_kwargs
    assert unpickled.chat_template_kwargs == config.chat_template_kwargs
    assert unpickled.quantization_kwargs == config.quantization_kwargs


def test_partition_empty_options():
    """Test partitioning with no options."""
    config = TransformersPrompterModelLoader.partition_options({})

    assert config.model_kwargs == {"trust_remote_code": True}
    assert config.processor_kwargs == {"trust_remote_code": True}
    assert config.generation_kwargs == {}
    assert config.chat_template_kwargs == {
        "tokenize": True,
        "add_generation_prompt": True,
        "return_dict": True,
        "return_tensors": "pt",
    }
    assert config.quantization_kwargs is None


def test_partition_generation_options():
    """Test that generation options are correctly extracted."""
    config = TransformersPrompterModelLoader.partition_options(
        {
            "max_new_tokens": 100,
            "temperature": 0.7,
            "top_p": 0.9,
            "do_sample": True,
        }
    )

    assert config.generation_kwargs["max_new_tokens"] == 100
    assert config.generation_kwargs["temperature"] == 0.7
    assert config.generation_kwargs["top_p"] == 0.9
    assert config.generation_kwargs["do_sample"] is True


def test_partition_model_loading_options():
    """Test that model loading options are correctly extracted."""
    config = TransformersPrompterModelLoader.partition_options(
        {
            "trust_remote_code": False,
            "torch_dtype": "bfloat16",
            "device_map": "auto",
            "attn_implementation": "flash_attention_2",
        }
    )

    assert config.model_kwargs["trust_remote_code"] is False
    assert config.model_kwargs["torch_dtype"] == "bfloat16"
    assert config.model_kwargs["device_map"] == "auto"
    assert config.model_kwargs["attn_implementation"] == "flash_attention_2"


def test_partition_chat_template_options():
    """Test that chat template options override defaults."""
    config = TransformersPrompterModelLoader.partition_options(
        {
            "tokenize": False,  # Override default
            "continue_final_message": True,  # Add new option
        }
    )

    assert config.chat_template_kwargs["tokenize"] is False
    assert config.chat_template_kwargs["continue_final_message"] is True
    assert config.chat_template_kwargs["add_generation_prompt"] is True


def test_partition_quantization_dict():
    """Test that quantization config dict is extracted."""
    config = TransformersPrompterModelLoader.partition_options(
        {
            "quantization_config": {
                "load_in_4bit": True,
                "bnb_4bit_compute_dtype": "float16",
            },
        }
    )

    assert config.quantization_kwargs == {
        "load_in_4bit": True,
        "bnb_4bit_compute_dtype": "float16",
    }


def test_partition_quantization_config_object():
    """Test that BitsAndBytesConfig is converted to dict."""
    try:
        from transformers import BitsAndBytesConfig

        bnb_config = BitsAndBytesConfig(load_in_4bit=True)
        config = TransformersPrompterModelLoader.partition_options(
            {
                "quantization_config": bnb_config,
            }
        )

        assert isinstance(config.quantization_kwargs, dict)
        assert config.quantization_kwargs.get("load_in_4bit") is True
    except ImportError:
        pytest.skip("bitsandbytes not installed")


def test_partition_quantization_invalid_type():
    """Test that invalid quantization config raises TypeError."""
    with pytest.raises(TypeError, match="quantization_config must be dict"):
        TransformersPrompterModelLoader.partition_options(
            {
                "quantization_config": "invalid",
            }
        )


def test_partition_mixed_options():
    """Test partitioning with mixed option types."""
    config = TransformersPrompterModelLoader.partition_options(
        {
            # Model loading
            "trust_remote_code": True,
            "torch_dtype": "float16",
            # Generation
            "max_new_tokens": 50,
            "temperature": 0.5,
            # Chat template
            "add_generation_prompt": False,
            # Quantization
            "quantization_config": {"load_in_8bit": True},
        }
    )

    assert config.model_kwargs["trust_remote_code"] is True
    assert config.model_kwargs["torch_dtype"] == "float16"

    assert config.generation_kwargs["max_new_tokens"] == 50
    assert config.generation_kwargs["temperature"] == 0.5

    assert config.chat_template_kwargs["add_generation_prompt"] is False
    assert config.quantization_kwargs == {"load_in_8bit": True}
    assert config.processor_kwargs["trust_remote_code"] is True


def test_partition_does_not_mutate_input():
    """Verify that partition_options doesn't mutate the input dict."""
    original = {
        "max_new_tokens": 100,
        "trust_remote_code": True,
    }
    original_copy = dict(original)

    TransformersPrompterModelLoader.partition_options(original)

    assert original == original_copy


def test_partition_unknown_keys_go_to_generation():
    """Unknown keys should be passed through to generation config."""
    config = TransformersPrompterModelLoader.partition_options(
        {
            "some_future_param": "value",
            "another_unknown": 42,
        }
    )

    assert config.generation_kwargs["some_future_param"] == "value"
    assert config.generation_kwargs["another_unknown"] == 42


def test_get_generation_config_keys_returns_frozenset():
    """Key derivation should return frozenset."""
    keys = TransformersPrompterModelLoader.get_generation_config_keys()
    assert isinstance(keys, frozenset)


def test_get_generation_config_keys_not_empty():
    """Should return some keys (may include 'kwargs' if using **kwargs signature)."""
    keys = TransformersPrompterModelLoader.get_generation_config_keys()
    assert len(keys) >= 1


def test_get_generation_config_keys_is_cached():
    """Key derivation should be cached (class variable)."""
    keys1 = TransformersPrompterModelLoader.get_generation_config_keys()
    keys2 = TransformersPrompterModelLoader.get_generation_config_keys()
    assert keys1 is keys2


def test_generation_config_accepts_common_params():
    """Verify GenerationConfig accepts common generation parameters."""
    from transformers import GenerationConfig

    config = GenerationConfig(
        max_new_tokens=100,
        temperature=0.7,
        top_p=0.9,
        top_k=50,
        do_sample=True,
    )

    assert config.max_new_tokens == 100
    assert config.temperature == 0.7
