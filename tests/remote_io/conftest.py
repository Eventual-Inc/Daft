from __future__ import annotations

import io
from typing import Generator, TypeVar

import numpy as np
import pytest
from PIL import Image

T = TypeVar("T")

YieldFixture = Generator[T, None, None]


@pytest.fixture(scope="session")
def image_data() -> YieldFixture[bytes]:
    """A small bit of fake image JPEG data"""
    bio = io.BytesIO()
    image = Image.fromarray(np.ones((3, 3)).astype(np.uint8))
    image.save(bio, format="JPEG")
    return bio.getvalue()
