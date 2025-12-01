# =============================================================================
# Colab Pydantic Pickle Test Script
# =============================================================================
#
# This script tests fixes for the Pydantic cloudpickle serialization issue in
# Google Colab. See: https://github.com/Eventual-Inc/Daft/issues/5696
#
# HOW TO USE IN COLAB:
# --------------------
# Run these commands in a Colab cell:
#
#   !pip install daft pydantic
#   !curl -s "https://gist.githubusercontent.com/ykdojo/5fe97bc6514342988cceb54cd68330ed/raw/colab_test_create_model.py?nocache=$RANDOM" -o test.py && python test.py
#
# TO UPDATE THE GIST (run from repo root):
#   gh gist edit 5fe97bc6514342988cceb54cd68330ed colab_test_create_model.py
#
# =============================================================================

# Full integration test with Daft's prompt() function

# First, patch Daft with the fix
from __future__ import annotations

from typing import get_args, get_origin

from pydantic import BaseModel, create_model


def clean_pydantic_model(model_cls, cleaned_cache=None):
    """Recursively clean a Pydantic model and all referenced models."""
    if cleaned_cache is None:
        cleaned_cache = {}

    if model_cls in cleaned_cache:
        return cleaned_cache[model_cls]

    # Collect referenced models
    referenced_models = []
    for field_name, field_info in model_cls.model_fields.items():
        annotation = field_info.annotation
        origin = get_origin(annotation)
        if origin is not None:
            for arg in get_args(annotation):
                if isinstance(arg, type) and issubclass(arg, BaseModel):
                    if arg not in cleaned_cache and arg not in referenced_models:
                        referenced_models.append(arg)
        elif isinstance(annotation, type) and issubclass(annotation, BaseModel):
            if annotation not in cleaned_cache and annotation not in referenced_models:
                referenced_models.append(annotation)

    # Clean referenced models first
    for ref_model in referenced_models:
        clean_pydantic_model(ref_model, cleaned_cache)

    # Build with simple tuples (annotation, default)
    field_definitions = {}
    for field_name, field_info in model_cls.model_fields.items():
        annotation = field_info.annotation
        origin = get_origin(annotation)
        if origin is not None:
            new_args = []
            for arg in get_args(annotation):
                if isinstance(arg, type) and issubclass(arg, BaseModel) and arg in cleaned_cache:
                    new_args.append(cleaned_cache[arg])
                else:
                    new_args.append(arg)
            annotation = origin[tuple(new_args)] if len(new_args) > 1 else origin[new_args[0]]
        elif isinstance(annotation, type) and issubclass(annotation, BaseModel) and annotation in cleaned_cache:
            annotation = cleaned_cache[annotation]

        default = ... if field_info.is_required() else field_info.default
        field_definitions[field_name] = (annotation, default)

    cleaned = create_model(model_cls.__name__, **field_definitions)
    cleaned_cache[model_cls] = cleaned
    return cleaned


# Detect Colab environment
def _is_colab():
    try:
        import google.colab  # noqa: F401

        return True
    except ImportError:
        return False


print("=== Colab Detection Verification ===")
is_colab = _is_colab()
print(f"Running in Colab: {is_colab}")
if is_colab:
    print("✓ Colab detected - this is where the pickle issue occurs")
else:
    print("✗ Not in Colab (expected if running locally)")
print()

import daft.functions.ai as ai_module

# Monkey-patch daft.functions.ai to use the cleaning function (for testing)
original_prompt = ai_module.prompt


def patched_prompt(messages, return_format=None, **kwargs):
    if return_format is not None:
        return_format = clean_pydantic_model(return_format)
    return original_prompt(messages, return_format=return_format, **kwargs)


ai_module.prompt = patched_prompt

print("=== Full Daft Integration Test ===")
print()

# Now test with Daft
import daft
from daft.functions.ai import prompt


class Result(BaseModel):
    sentiment: str
    confidence: float


df = daft.from_pydict({"text": ["I love this!", "This is terrible"]})

print("Creating DataFrame with prompt()...")
try:
    df = df.with_column(
        "analysis", prompt(["Analyze:", df["text"]], model="gpt-4o-mini", provider="openai", return_format=Result)
    )
    print("SUCCESS - DataFrame created without pickle error!")
    print()
    print("Note: Actual API call would require OPENAI_API_KEY")
except Exception as e:
    print(f"FAILED: {e}")
