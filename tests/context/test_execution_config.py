from __future__ import annotations

import subprocess
import sys

import pytest

import daft


def test_set_execution_config_max_limit_tasks_zero_rejected():
    # Setting to 0 should be rejected by the Python config layer
    with pytest.raises(ValueError):
        daft.set_execution_config(max_limit_tasks_submittable_in_parallel=0)


def test_env_var_zero_rejected_defaults_to_1():
    # When env var is 0, from_env should fall back to default (1)
    script = """
import daft
print(daft.context.get_context().daft_execution_config.max_limit_tasks_submittable_in_parallel)
"""
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, env={"DAFT_MAX_LIMIT_TASKS_SUBMITTABLE_IN_PARALLEL": "0"}
    )
    assert result.stdout.decode().strip() == "1"
