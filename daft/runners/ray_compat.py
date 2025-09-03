from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


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


def validate_and_normalize_ray_options(options: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize the ray options.

    For the label_selector option:
    - If current Ray supports "label_selector" option, return as-is.
    - Otherwise, convert {label_selector: {...}} to scheduling_strategy=NodeLabelSchedulingStrategy
      and drop the unsupported "label_selector" key.

    For the runtime_env option:
    - Currently only Conda Env is allowed to be configured.
    """
    if not options:
        return options

    # label selector
    if not _supports_label_selector_option() and (label_selector := options.get("label_selector")):
        if not isinstance(label_selector, dict):
            raise ValueError(f"label_selector must be a dict, but got '{type(label_selector)}'")

        # Newer Ray versions accept label_selector directly, but older versions of Ray need to convert
        # it to NodeLabelSchedulingStrategy

        strategy = _parse_label_selector_to_strategy(label_selector)
        # Avoid conflicting strategies: override existing scheduling_strategy if any
        options = {k: v for k, v in options.items() if k != "label_selector"}
        options["scheduling_strategy"] = strategy

    # runtime env
    if runtime_env := options.get("runtime_env"):
        if not isinstance(runtime_env, dict):
            raise ValueError(f"runtime_env must be a dict, but got '{type(runtime_env)}'")

        # TODO(zhenchao) Support more runtime env configuration items if necessary
        if set(runtime_env.keys()) != {"conda"}:
            raise ValueError(
                f"The conda environment is only allowed to be configured through runtime_env in ray_options, "
                f"but got '{runtime_env}'"
            )

    return options
