from __future__ import annotations

import datetime

import pytest

import daft


@pytest.mark.integration()
def test_read_blob_http_populates_last_modified(mock_http_image_urls, image_data):
    """HTTP `Last-Modified` header → `last_modified` column via get_file_metadata override."""
    url = mock_http_image_urls[0]
    df = daft.read_blob(url)
    data = df.to_pydict()
    assert data["content"] == [image_data]
    assert data["size"] == [len(image_data)]
    ts = data["last_modified"][0]
    assert ts is not None
    assert isinstance(ts, datetime.datetime)
    assert ts.tzinfo is not None
