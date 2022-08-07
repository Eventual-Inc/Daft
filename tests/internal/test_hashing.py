import mmh3
import numpy as np
import pytest

from daft.internal import hashing


@pytest.mark.parametrize("size", [(i + 1) * 1000 + i for i in range(5)])
def test_murmur_32_buffer(size: int) -> None:
    test_input = np.random.randint(0, 255, size, dtype=np.uint8)
    hashing_out = hashing.murmur3_hash_32_buffer(test_input)
    mmh3_out = mmh3.hash_from_buffer(test_input, signed=False)
    assert hashing_out == mmh3_out
