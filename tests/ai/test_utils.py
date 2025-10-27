from __future__ import annotations

import pytest


def test_get_torch_device_cuda():
    """Test CUDA device selection when available."""
    torch = pytest.importorskip("torch")
    from unittest.mock import patch

    from daft.ai.utils import get_torch_device

    with patch.object(torch.cuda, "is_available", return_value=True):
        device = get_torch_device()
        assert device.type == "cuda"


def test_get_torch_device_mps():
    """Test MPS device selection when CUDA unavailable but MPS available."""
    torch = pytest.importorskip("torch")
    from unittest.mock import MagicMock, patch

    from daft.ai.utils import get_torch_device

    # Mock MPS availability
    mock_mps = MagicMock()
    mock_mps.is_available.return_value = True

    with patch.object(torch.cuda, "is_available", return_value=False):
        with patch.object(torch.backends, "mps", mock_mps, create=True):
            device = get_torch_device()
            assert device.type == "mps"


def test_get_torch_device_cpu():
    """Test CPU device selection when no GPU available."""
    torch = pytest.importorskip("torch")
    from unittest.mock import patch

    from daft.ai.utils import get_torch_device

    with patch.object(torch.cuda, "is_available", return_value=False):
        # MPS might or might not exist depending on platform
        device = get_torch_device()
        # Should be either cpu or mps depending on platform
        assert device.type in ("cpu", "mps")
