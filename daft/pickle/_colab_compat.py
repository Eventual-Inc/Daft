"""Google Colab compatibility utilities for Pydantic model serialization.

See: https://github.com/Eventual-Inc/Daft/issues/5696
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pydantic import BaseModel


def _is_colab() -> bool:
    """Check if running in Google Colab."""
    try:
        import google.colab  # noqa: F401

        return True
    except ImportError:
        return False


# Cache the Colab check at module load time
IS_COLAB = _is_colab()


def clean_pydantic_model(model_cls: type[BaseModel], _cleaned_cache: dict[type, type] | None = None) -> type[BaseModel]:
    """Recursively recreate a Pydantic model in a clean namespace.

    This is needed for Google Colab compatibility. When a Pydantic model is defined
    in a Colab notebook, cloudpickle captures the notebook's globals (including
    IPython/ZMQ internals) when serializing. These internals contain unpicklable
    objects like ZMQ socket handles.

    By recreating the model with create_model(), we build a fresh namespace dict
    with only the necessary fields, avoiding the polluted globals from Colab.

    This function recursively cleans any nested Pydantic models referenced in
    field annotations (e.g., list[Item] where Item is another BaseModel).

    See: https://github.com/Eventual-Inc/Daft/issues/5696
    """
    import types
    from typing import Union, get_args, get_origin

    from pydantic import BaseModel, create_model

    if _cleaned_cache is None:
        _cleaned_cache = {}

    # Return cached version if already cleaned
    if model_cls in _cleaned_cache:
        return _cleaned_cache[model_cls]

    # First pass: collect all referenced Pydantic models (excluding self-references)
    referenced_models = []
    for field_info in model_cls.model_fields.values():
        annotation = field_info.annotation
        origin = get_origin(annotation)
        if origin is not None:
            for arg in get_args(annotation):
                if isinstance(arg, type) and issubclass(arg, BaseModel):
                    if arg is not model_cls and arg not in _cleaned_cache and arg not in referenced_models:
                        referenced_models.append(arg)
        elif isinstance(annotation, type) and issubclass(annotation, BaseModel):
            if annotation is not model_cls and annotation not in _cleaned_cache and annotation not in referenced_models:
                referenced_models.append(annotation)

    # Recursively clean referenced models first
    for ref_model in referenced_models:
        clean_pydantic_model(ref_model, _cleaned_cache)

    # Build field definitions with cleaned types
    field_definitions: dict[str, Any] = {}
    for field_name, field_info in model_cls.model_fields.items():
        annotation = field_info.annotation

        # Replace any referenced Pydantic models with their cleaned versions
        origin = get_origin(annotation)
        if origin is not None:
            new_args = []
            for arg in get_args(annotation):
                if isinstance(arg, type) and issubclass(arg, BaseModel) and arg in _cleaned_cache:
                    new_args.append(_cleaned_cache[arg])
                else:
                    new_args.append(arg)
            # Handle UnionType (X | Y syntax) specially - it's not subscriptable
            if origin is types.UnionType or origin is Union:
                annotation = Union[tuple(new_args)]
            elif len(new_args) > 1:
                annotation = origin[tuple(new_args)]
            else:
                annotation = origin[new_args[0]]
        elif isinstance(annotation, type) and issubclass(annotation, BaseModel) and annotation in _cleaned_cache:
            annotation = _cleaned_cache[annotation]

        # Use ... for required fields, or the default value
        default = ... if field_info.is_required() else field_info.default
        field_definitions[field_name] = (annotation, default)

    # Recreate the model with create_model
    cleaned = create_model(model_cls.__name__, **field_definitions)
    _cleaned_cache[model_cls] = cleaned
    return cleaned
