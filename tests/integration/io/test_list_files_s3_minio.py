from __future__ import annotations

import pytest

from .conftest import minio_create_bucket


@pytest.mark.integration()
def test_flat_directory_listing(minio_io_config):
    with minio_create_bucket(minio_io_config) as fs:
        fs
