from __future__ import annotations

from typing import Any

# Ray is required for this module but imported locally in functions that use it


def _supports_label_selector_option() -> bool:
    try:
        from ray._private import ray_option_utils as _rou
    except Exception:
        return False
    # Ray recognizes option keys via these dictionaries
    return (
        "label_selector" in getattr(_rou, "task_options", {})
        or "label_selector" in getattr(_rou, "actor_options", {})
        or "label_selector" in getattr(_rou, "valid_options", {})
    )


def _parse_label_selector_to_strategy(label_selector: dict[str, str]) -> Any:
    """Convert Daft's string-based label selector into Ray's NodeLabelSchedulingStrategy.

    Supported syntaxes per Ray tests/docs:
    - Equals: {"key": "value"}
    - Not equals: {"key": "!value"}
    - In: {"key": "in(v1, v2)"}
    - Not in: {"key": "!in(v1, v2)"}
    """
    from ray.util.scheduling_strategies import In, NodeLabelSchedulingStrategy, NotIn

    hard_map: dict[str, Any] = {}
    for k, v in label_selector.items():
        s = v.strip()
        if s.startswith("!in(") and s.endswith(")"):
            values = [x.strip() for x in s[len("!in(") : -1].split(",") if x.strip()]
            hard_map[k] = NotIn(*values)  # codespell: ignore
        elif s.startswith("in(") and s.endswith(")"):
            values = [x.strip() for x in s[len("in(") : -1].split(",") if x.strip()]
            hard_map[k] = In(*values)  # codespell: ignore
        elif s.startswith("!"):
            hard_map[k] = NotIn(s[1:])  # codespell: ignore
        else:
            hard_map[k] = In(s)  # codespell: ignore

    return NodeLabelSchedulingStrategy(hard=hard_map)


def normalize_ray_options_with_label_selector(options: dict[str, Any]) -> dict[str, Any]:
    """Normalize Ray .options(**kwargs) for cross-version compatibility.

    - If current Ray supports "label_selector" option, return as-is.
    - Otherwise, convert {label_selector: {...}} to scheduling_strategy=NodeLabelSchedulingStrategy
      and drop the unsupported "label_selector" key.
    """
    if not options:
        return options

    if "label_selector" not in options:
        return options

    if _supports_label_selector_option():
        # Newer Ray versions accept label_selector directly.
        return options

    # Older Ray: convert to NodeLabelSchedulingStrategy and remove label_selector.
    label_selector = options.get("label_selector") or {}
    if not isinstance(label_selector, dict):
        # Invalid format; leave options untouched to let Ray raise errors
        return options

    strategy = _parse_label_selector_to_strategy(label_selector)
    # Avoid conflicting strategies: override existing scheduling_strategy if any
    options = {k: v for k, v in options.items() if k != "label_selector"}
    options["scheduling_strategy"] = strategy
    return options
