from __future__ import annotations

import pytest

import daft
from tests.conftest import get_tests_daft_runner_name
from tests.integration.ray.autoscaling_cluster import autoscaling_cluster_context

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        get_tests_daft_runner_name() != "ray",
        reason="Fractional GPU tests require Ray runner",
    ),
]


@pytest.mark.parametrize("gpus", [0.0, 0, 0.5, 1.0, 1, 2])
def test_cls_accepts_fractional_gpus_on_ray_cluster(gpus):
    head_resources = {"CPU": 0, "GPU": 0}
    worker_node_types = {
        "worker": {
            "resources": {"CPU": 2, "GPU": 2},
            "node_config": {},
            "min_workers": 0,
            "max_workers": 1,
        }
    }

    with autoscaling_cluster_context(head_resources, worker_node_types):
        df = daft.from_pydict({"a": ["x", "y", "z"]})

        @daft.cls(gpus=gpus)
        class Repeat:
            def __init__(self, n: int):
                self.n = n

            def __call__(self, x) -> str:
                return x * self.n

        repeat_2 = Repeat(2)
        result = df.select(repeat_2(df["a"]))
        assert result.to_pydict() == {"a": ["xx", "yy", "zz"]}
