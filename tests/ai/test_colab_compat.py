from __future__ import annotations

import cloudpickle
import pytest

pytest.importorskip("pydantic")

from pydantic import BaseModel

from daft.pickle._colab_compat import IS_COLAB, clean_pydantic_model


class SimpleModel(BaseModel):
    """Simple model with primitive fields."""

    name: str
    value: int
    score: float


class Child(BaseModel):
    """Child model for nested testing."""

    name: str
    age: int


class Parent(BaseModel):
    """Parent model with nested children."""

    name: str
    children: list[Child]


class TreeNode(BaseModel):
    """Self-referential model for tree structures."""

    value: str
    children: list[TreeNode] | None = None


class SingleChildTree(BaseModel):
    """Direct self-reference (non-container) to trigger recursion edge case."""

    value: str
    child: SingleChildTree | None = None


class ModelWithDefaults(BaseModel):
    """Model with required and optional fields."""

    required_field: str
    optional_field: str = "default_value"
    optional_int: int = 42


def test_is_colab_returns_false_outside_colab():
    """Verify IS_COLAB is False when not running in Google Colab."""
    assert IS_COLAB is False


def test_cleaned_model_has_clean_namespace():
    """Verify cleaned model is created in a clean module namespace.

    This is the core of the fix: models defined in Colab have __module__ = '__main__'
    which captures Colab's polluted globals. The cleaned model gets created in
    _colab_compat's namespace via create_model(), avoiding the pollution.
    """
    # Original model is defined in this test file's module
    original_module = SimpleModel.__module__

    cleaned = clean_pydantic_model(SimpleModel)

    # Cleaned model should be in the _colab_compat module's namespace
    assert cleaned.__module__ == "daft.pickle._colab_compat"
    assert cleaned.__module__ != original_module

    # They should be different classes
    assert cleaned is not SimpleModel


def test_clean_simple_model():
    """Test cleaning a simple model with primitive fields."""
    cleaned = clean_pydantic_model(SimpleModel)

    assert cleaned.__name__ == "SimpleModel"
    assert set(cleaned.model_fields.keys()) == {"name", "value", "score"}

    # Verify the cleaned model works
    instance = cleaned(name="test", value=10, score=0.5)
    assert instance.name == "test"
    assert instance.value == 10
    assert instance.score == 0.5


def test_clean_nested_model():
    """Test cleaning a model with nested Pydantic models."""
    cleaned = clean_pydantic_model(Parent)

    assert cleaned.__name__ == "Parent"
    assert set(cleaned.model_fields.keys()) == {"name", "children"}

    # Verify the cleaned model works with nested data
    instance = cleaned(
        name="parent",
        children=[{"name": "child1", "age": 5}, {"name": "child2", "age": 8}],
    )
    assert instance.name == "parent"
    assert len(instance.children) == 2
    assert instance.children[0].name == "child1"


def test_clean_self_referential_model_with_children_list():
    """Test cleaning a self-referential model (tree structure)."""
    cleaned = clean_pydantic_model(TreeNode)

    assert cleaned.__name__ == "TreeNode"
    assert set(cleaned.model_fields.keys()) == {"value", "children"}

    # Verify the cleaned model works with nested tree structure
    instance = cleaned(
        value="root",
        children=[
            {"value": "child1", "children": None},
            {"value": "child2", "children": [{"value": "grandchild", "children": None}]},
        ],
    )
    assert instance.value == "root"
    assert len(instance.children) == 2
    assert instance.children[1].children[0].value == "grandchild"


def test_clean_self_referential_model():
    """Self references outside containers should not recurse forever."""
    SingleChildTree.model_rebuild()
    cleaned = clean_pydantic_model(SingleChildTree)

    assert cleaned.__name__ == "SingleChildTree"
    instance = cleaned(value="root", child={"value": "leaf", "child": None})
    assert instance.value == "root"
    assert instance.child.value == "leaf"


def test_clean_model_preserves_defaults():
    """Test that cleaning preserves field defaults."""
    cleaned = clean_pydantic_model(ModelWithDefaults)

    # Check required field is still required
    assert cleaned.model_fields["required_field"].is_required() is True

    # Check optional fields have correct defaults
    assert cleaned.model_fields["optional_field"].is_required() is False
    assert cleaned.model_fields["optional_field"].default == "default_value"
    assert cleaned.model_fields["optional_int"].default == 42

    # Verify defaults work when instantiating
    instance = cleaned(required_field="test")
    assert instance.required_field == "test"
    assert instance.optional_field == "default_value"
    assert instance.optional_int == 42


def test_clean_model_caching_within_call():
    """Test that caching works for nested models within a single call."""
    # When cleaning Parent, Child should be cleaned once and reused
    # We can verify this by checking that nested model cleaning doesn't
    # cause issues with self-referential models (which would infinite loop without caching)
    cleaned = clean_pydantic_model(TreeNode)

    # If caching didn't work, this would have caused infinite recursion
    # The fact that it completes successfully proves caching works
    assert cleaned.__name__ == "TreeNode"

    # Verify the model is functional
    instance = cleaned(value="root", children=[{"value": "child", "children": None}])
    assert instance.value == "root"


def test_cleaned_model_is_picklable():
    """Test that cleaned models can be pickled with cloudpickle."""
    cleaned = clean_pydantic_model(SimpleModel)

    # Should not raise
    pickled = cloudpickle.dumps(cleaned)
    unpickled = cloudpickle.loads(pickled)

    # Verify the unpickled model works
    instance = unpickled(name="test", value=10, score=0.5)
    assert instance.name == "test"
    assert instance.value == 10
    assert instance.score == 0.5


def test_cleaned_nested_model_is_picklable():
    """Test that cleaned nested models can be pickled."""
    cleaned = clean_pydantic_model(Parent)

    pickled = cloudpickle.dumps(cleaned)
    unpickled = cloudpickle.loads(pickled)

    instance = unpickled(
        name="parent",
        children=[{"name": "child", "age": 5}],
    )
    assert instance.name == "parent"
    assert instance.children[0].name == "child"


def test_cleaned_self_referential_model_is_picklable():
    """Test that cleaned self-referential models can be pickled."""
    cleaned = clean_pydantic_model(TreeNode)

    pickled = cloudpickle.dumps(cleaned)
    unpickled = cloudpickle.loads(pickled)

    instance = unpickled(
        value="root",
        children=[{"value": "child", "children": None}],
    )
    assert instance.value == "root"
    assert instance.children[0].value == "child"


def test_cleaned_direct_self_referential_model_is_picklable():
    """Test that cleaned direct self-referential models can be pickled."""
    SingleChildTree.model_rebuild()
    cleaned = clean_pydantic_model(SingleChildTree)

    pickled = cloudpickle.dumps(cleaned)
    unpickled = cloudpickle.loads(pickled)

    instance = unpickled(value="root", child={"value": "leaf", "child": None})
    assert instance.value == "root"
    assert instance.child.value == "leaf"
