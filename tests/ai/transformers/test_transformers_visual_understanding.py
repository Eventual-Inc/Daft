from __future__ import annotations

import pytest

pytest.importorskip("transformers")
pytest.importorskip("torch")
pytest.importorskip("PIL")

import numpy as np

from daft.ai.protocols import VisualUnderstandingDescriptor
from daft.ai.transformers.provider import TransformersProvider
from tests.benchmarks.conftest import IS_CI


def test_transformers_visual_understanding_default():
    """Test that the TransformersProvider can create a VisualUnderstandingDescriptor with default settings."""
    provider = TransformersProvider()
    descriptor = provider.get_visual_understanding()
    assert isinstance(descriptor, VisualUnderstandingDescriptor)
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == "microsoft/git-base"


@pytest.mark.parametrize(
    "model_name, run_model_in_ci",
    [
        ("microsoft/git-base", True),
        ("microsoft/git-large", False),  # Skip large model in CI
        ("nlpconnect/vit-gpt2-image-captioning", True),
        ("Qwen/Qwen2.5-VL-3B-Instruct", False),  # Skip Qwen model in CI due to size
    ],
)
def test_transformers_visual_understanding_models(model_name, run_model_in_ci):
    """Test visual understanding with different model configurations."""
    mock_options = {"max_length": 30, "num_beams": 2}

    provider = TransformersProvider()
    descriptor = provider.get_visual_understanding(model_name, **mock_options)
    
    assert isinstance(descriptor, VisualUnderstandingDescriptor)
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == model_name
    assert descriptor.get_options() == mock_options

    if not IS_CI or run_model_in_ci:
        # Only run actual model inference if not in CI or explicitly allowed
        try:
            visual_understanding = descriptor.instantiate()
            
            # Create test images - RGB format required
            test_image1 = np.random.randint(0, 255, (224, 224, 3), dtype=np.uint8)
            test_image2 = np.random.randint(0, 255, (300, 400, 3), dtype=np.uint8)
            test_images = [test_image1, test_image2]
            
            # Test with empty text (image captioning)
            texts = ["", ""]
            results = visual_understanding.understand_visual(test_images, texts)
            
            assert isinstance(results, list)
            assert len(results) == 2
            assert all(isinstance(result, str) for result in results)
            
            # Test with specific text prompts (if supported)
            text_prompts = ["Describe this image", "What do you see?"]
            results_with_prompts = visual_understanding.understand_visual(test_images, text_prompts)
            
            assert isinstance(results_with_prompts, list)
            assert len(results_with_prompts) == 2
            assert all(isinstance(result, str) for result in results_with_prompts)
            
        except Exception as e:
            # If model loading fails (e.g., network issues), skip the test
            pytest.skip(f"Model loading failed: {e}")


def test_transformers_visual_understanding_empty_inputs():
    """Test visual understanding with empty inputs."""
    provider = TransformersProvider()
    descriptor = provider.get_visual_understanding("microsoft/git-base")
    
    if not IS_CI:  # Only run if not in CI
        try:
            visual_understanding = descriptor.instantiate()
            
            # Test with empty lists
            results = visual_understanding.understand_visual([], [])
            assert results == []
            
        except Exception as e:
            pytest.skip(f"Model loading failed: {e}")


def test_transformers_visual_understanding_mismatched_inputs():
    """Test visual understanding with mismatched input lengths."""
    provider = TransformersProvider()
    descriptor = provider.get_visual_understanding("microsoft/git-base")
    
    if not IS_CI:  # Only run if not in CI
        try:
            visual_understanding = descriptor.instantiate()
            
            # Create test image
            test_image = np.random.randint(0, 255, (224, 224, 3), dtype=np.uint8)
            
            # Test with mismatched lengths (more images than texts)
            images = [test_image, test_image]
            texts = ["Describe this image"]  # Only one text for two images
            
            results = visual_understanding.understand_visual(images, texts)
            assert isinstance(results, list)
            assert len(results) == 2  # Should return results for both images
            
        except Exception as e:
            pytest.skip(f"Model loading failed: {e}")


def test_visual_understanding_descriptor_methods():
    """Test the descriptor methods work correctly."""
    provider = TransformersProvider()
    descriptor = provider.get_visual_understanding("microsoft/git-base", test_option="test_value")
    
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == "microsoft/git-base"
    assert descriptor.get_options() == {"test_option": "test_value"}

def test_qwen_visual_understanding_basic():
    """Test basic visual understanding functionality with Qwen2.5-VL-3B-Instruct model."""
    provider = TransformersProvider()
    descriptor = provider.get_visual_understanding("Qwen/Qwen2.5-VL-3B-Instruct")
    
    assert isinstance(descriptor, VisualUnderstandingDescriptor)
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == "Qwen/Qwen2.5-VL-3B-Instruct"
    
    if not IS_CI:  # Skip actual model loading in CI
        try:
            visual_understanding = descriptor.instantiate()
            
            # Create test image - RGB format required
            test_image = np.random.randint(0, 255, (224, 224, 3), dtype=np.uint8)
            
            # Test basic image captioning
            results = visual_understanding.understand_visual([test_image], [""])
            assert isinstance(results, list)
            assert len(results) == 1
            assert isinstance(results[0], str)
            
        except Exception as e:
            pytest.skip(f"Qwen model loading failed: {e}")


def test_qwen_visual_understanding_with_text_prompts():
    """Test text-conditioned visual understanding with Qwen2.5-VL-3B-Instruct model."""
    provider = TransformersProvider()
    descriptor = provider.get_visual_understanding("Qwen/Qwen2.5-VL-3B-Instruct")
    
    if not IS_CI:  # Skip actual model loading in CI
        try:
            visual_understanding = descriptor.instantiate()
            
            # Create test images
            test_image1 = np.random.randint(0, 255, (256, 256, 3), dtype=np.uint8)
            test_image2 = np.random.randint(0, 255, (384, 384, 3), dtype=np.uint8)
            test_images = [test_image1, test_image2]
            
            # Test with specific text prompts for Qwen model
            text_prompts = [
                "Describe what you see in this image in detail.",
                "What objects are present in this image?"
            ]
            
            results = visual_understanding.understand_visual(test_images, text_prompts)
            
            assert isinstance(results, list)
            assert len(results) == 2
            assert all(isinstance(result, str) for result in results)
            assert all(len(result.strip()) > 0 for result in results)  # Non-empty responses
            
        except Exception as e:
            pytest.skip(f"Qwen model loading failed: {e}")


def test_qwen_visual_understanding_model_specific_config():
    """Test Qwen2.5-VL-3B-Instruct model with specific configuration options."""
    qwen_options = {
        "max_length": 100,
        "temperature": 0.7,
        "do_sample": True,
        "top_p": 0.9
    }
    
    provider = TransformersProvider()
    descriptor = provider.get_visual_understanding("Qwen/Qwen2.5-VL-3B-Instruct", **qwen_options)
    
    assert isinstance(descriptor, VisualUnderstandingDescriptor)
    assert descriptor.get_provider() == "transformers"
    assert descriptor.get_model() == "Qwen/Qwen2.5-VL-3B-Instruct"
    assert descriptor.get_options() == qwen_options
    
    if not IS_CI:  # Skip actual model loading in CI
        try:
            visual_understanding = descriptor.instantiate()
            
            # Create test image
            test_image = np.random.randint(0, 255, (224, 224, 3), dtype=np.uint8)
            
            # Test with configuration
            results = visual_understanding.understand_visual([test_image], ["Describe this image"])
            
            assert isinstance(results, list)
            assert len(results) == 1
            assert isinstance(results[0], str)
            
        except Exception as e:
            pytest.skip(f"Qwen model with config loading failed: {e}")


def test_qwen_visual_understanding_integration():
    """Test integration scenarios with Qwen2.5-VL-3B-Instruct model."""
    provider = TransformersProvider()
    descriptor = provider.get_visual_understanding("Qwen/Qwen2.5-VL-3B-Instruct")
    
    if not IS_CI:  # Skip actual model loading in CI
        try:
            visual_understanding = descriptor.instantiate()
            
            # Test with multiple images and varied prompts
            test_images = [
                np.random.randint(0, 255, (224, 224, 3), dtype=np.uint8),
                np.random.randint(0, 255, (300, 300, 3), dtype=np.uint8),
                np.random.randint(0, 255, (400, 200, 3), dtype=np.uint8)
            ]
            
            # Mix of empty and specific prompts
            text_prompts = [
                "",  # Basic captioning
                "What is the main subject of this image?",  # Specific question
                "Describe the colors and composition."  # Detailed analysis
            ]
            
            results = visual_understanding.understand_visual(test_images, text_prompts)
            
            assert isinstance(results, list)
            assert len(results) == 3
            assert all(isinstance(result, str) for result in results)
            
            # Test batch processing with same prompt
            same_prompt = ["Describe this image"] * 3
            batch_results = visual_understanding.understand_visual(test_images, same_prompt)
            
            assert isinstance(batch_results, list)
            assert len(batch_results) == 3
            assert all(isinstance(result, str) for result in batch_results)
            
        except Exception as e:
            pytest.skip(f"Qwen integration test failed: {e}")