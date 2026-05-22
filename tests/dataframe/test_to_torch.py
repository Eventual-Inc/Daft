from __future__ import annotations

import pytest

pytest.importorskip("torch")
import torch

import daft
from daft.dataframe.to_torch import DaftTorchDataLoader


def test_to_torch_dataloader_batches():
    df = daft.from_pydict({"x": [1, 2, 3, 4], "y": [5, 6, 7, 8]})
    loader = df.to_torch_dataloader(batch_size=2)

    assert isinstance(loader, DaftTorchDataLoader)
    assert not isinstance(loader, torch.utils.data.DataLoader)

    batches = list(loader)
    assert len(batches) == 2
    for batch in batches:
        assert set(batch.keys()) == {"x", "y"}
        assert isinstance(batch["x"], torch.Tensor)
        assert isinstance(batch["y"], torch.Tensor)
        assert batch["x"].shape == (2,)
        assert batch["y"].shape == (2,)

    assert torch.equal(batches[0]["x"], torch.tensor([1, 2]))
    assert torch.equal(batches[0]["y"], torch.tensor([5, 6]))
    assert torch.equal(batches[1]["x"], torch.tensor([3, 4]))
    assert torch.equal(batches[1]["y"], torch.tensor([7, 8]))


def test_to_torch_dataloader_drop_last():
    df = daft.from_pydict({"x": [1, 2, 3]})
    loader = df.to_torch_dataloader(batch_size=2, drop_last=True)

    batches = list(loader)
    assert len(batches) == 1
    assert batches[0]["x"].shape == (2,)
    assert torch.equal(batches[0]["x"], torch.tensor([1, 2]))


def test_to_torch_dataloader_invalid_batch_size():
    df = daft.from_pydict({"x": [1]})
    with pytest.raises(ValueError, match="batch_size must be greater than 0"):
        df.to_torch_dataloader(batch_size=0)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="pin_memory requires CUDA")
def test_to_torch_dataloader_pin_memory():
    df = daft.from_pydict({"x": [1.0, 2.0]})
    loader = df.to_torch_dataloader(batch_size=2, pin_memory=True)
    batch = next(iter(loader))
    assert batch["x"].is_pinned()
