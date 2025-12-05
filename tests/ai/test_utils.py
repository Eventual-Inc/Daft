from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from daft.ai.utils import get_gpu_udf_options, get_http_udf_options, get_torch_device


def test_get_torch_device_cuda():
    mock_torch = MagicMock()
    mock_torch.cuda.is_available.return_value = True
    mock_torch.device = lambda x: x
    
    with patch.dict(sys.modules, {"torch": mock_torch}):
        assert get_torch_device() == "cuda"


def test_get_torch_device_mps():
    mock_torch = MagicMock()
    mock_torch.cuda.is_available.return_value = False
    mock_torch.backends.mps.is_available.return_value = True
    mock_torch.device = lambda x: x
    
    with patch.dict(sys.modules, {"torch": mock_torch}):
        assert get_torch_device() == "mps"


def test_get_torch_device_cpu():
    mock_torch = MagicMock()
    mock_torch.cuda.is_available.return_value = False
    # Simulate mps not available or not present
    mock_torch.backends.mps.is_available.return_value = False
    mock_torch.device = lambda x: x
    
    with patch.dict(sys.modules, {"torch": mock_torch}):
        assert get_torch_device() == "cpu"


@patch("daft.ai.utils.get_or_infer_runner_type")
def test_get_gpu_udf_options_native_with_gpus(mock_get_runner):
    mock_get_runner.return_value = "native"
    with patch("daft.internal.gpu.cuda_visible_devices", return_value=[0, 1]):
        options = get_gpu_udf_options()
        assert options.concurrency == 2
        assert options.num_gpus == 1


@patch("daft.ai.utils.get_or_infer_runner_type")
def test_get_gpu_udf_options_native_no_gpus(mock_get_runner):
    mock_get_runner.return_value = "native"
    with patch("daft.internal.gpu.cuda_visible_devices", return_value=[]):
        options = get_gpu_udf_options()
        assert options.concurrency is None
        assert options.num_gpus is None


@patch("daft.ai.utils.get_or_infer_runner_type")
def test_get_gpu_udf_options_ray_with_gpus(mock_get_runner):
    mock_get_runner.return_value = "ray"
    mock_ray = MagicMock()
    mock_ray.nodes.return_value = [
        {"Resources": {"GPU": 2.0}},
        {"Resources": {"GPU": 0.0}},
        {"Resources": {}},  # No resources
        {},  # Empty node
    ]
    
    with patch.dict(sys.modules, {"ray": mock_ray}):
        options = get_gpu_udf_options()
        assert options.concurrency == 2  # 1 node has 2 GPUs? No, logic sums them up?
        # Logic:
        # num_gpus = 0
        # for node in ray.nodes(): ... num_gpus += int(node["Resources"]["GPU"])
        # Here num_gpus = 2 + 0 = 2.
        # If num_gpus > 0: return UDFOptions(concurrency=num_gpus, num_gpus=1)
        assert options.concurrency == 2
        assert options.num_gpus == 1


@patch("daft.ai.utils.get_or_infer_runner_type")
def test_get_gpu_udf_options_ray_no_gpus(mock_get_runner):
    mock_get_runner.return_value = "ray"
    mock_ray = MagicMock()
    mock_ray.nodes.return_value = [
        {"Resources": {"CPU": 1.0}},
    ]
    
    with patch.dict(sys.modules, {"ray": mock_ray}):
        options = get_gpu_udf_options()
        assert options.concurrency is None
        assert options.num_gpus is None


@patch("daft.ai.utils.get_or_infer_runner_type")
def test_get_gpu_udf_options_invalid_runner(mock_get_runner):
    mock_get_runner.return_value = "invalid"
    with pytest.raises(ValueError, match="Invalid runner type"):
        get_gpu_udf_options()


@patch("daft.ai.utils.get_or_infer_runner_type")
def test_get_http_udf_options_native(mock_get_runner):
    mock_get_runner.return_value = "native"
    options = get_http_udf_options()
    assert options.concurrency == 1
    assert options.num_gpus is None


@patch("daft.ai.utils.get_or_infer_runner_type")
def test_get_http_udf_options_ray_with_nodes(mock_get_runner):
    mock_get_runner.return_value = "ray"
    mock_ray = MagicMock()
    mock_ray.nodes.return_value = [
        {"Resources": {"CPU": 4.0}},
        {"Resources": {"CPU": 8.0}},
        {"Resources": {"GPU": 1.0}}, # No CPU?
    ]
    
    # Logic: counts nodes with CPU > 0
    # Node 1: CPU=4 > 0 -> count
    # Node 2: CPU=8 > 0 -> count
    # Node 3: No CPU key -> skip
    
    with patch.dict(sys.modules, {"ray": mock_ray}):
        options = get_http_udf_options()
        assert options.concurrency == 2
        assert options.num_gpus is None


@patch("daft.ai.utils.get_or_infer_runner_type")
def test_get_http_udf_options_ray_no_nodes(mock_get_runner):
    mock_get_runner.return_value = "ray"
    mock_ray = MagicMock()
    mock_ray.nodes.return_value = []
    
    with patch.dict(sys.modules, {"ray": mock_ray}):
        options = get_http_udf_options()
        assert options.concurrency == 1  # Fallback
        assert options.num_gpus is None


@patch("daft.ai.utils.get_or_infer_runner_type")
def test_get_http_udf_options_invalid_runner(mock_get_runner):
    mock_get_runner.return_value = "invalid"
    with pytest.raises(ValueError, match="Invalid runner type"):
        get_http_udf_options()

