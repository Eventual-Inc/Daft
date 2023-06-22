from __future__ import annotations

import io

import numpy as np
import pytest
from PIL import Image

import daft


@pytest.fixture(scope="session")
def image_data():
    """A small bit of fake image JPEG data"""
    bio = io.BytesIO()
    image = Image.fromarray(np.ones((3, 3)).astype(np.uint8))
    image.save(bio, format="JPEG")
    return bio.getvalue()


@pytest.fixture(scope="function")
def minio_session(minio_config, image_data):
    """Populates the minio session with some fake data"""
    s3 = minio_config.boto3_resource()

    # Add some images into `s3://image-bucket`
    bucket = s3.Bucket("image-bucket")
    bucket.create()
    for i in range(10):
        bucket.put_object(Body=image_data, Key=f"{i}.jpeg")

    yield minio_config

    bucket.objects.all().delete()
    bucket.delete()


def test_url_download(minio_session, image_data):
    import s3fs

    fs = s3fs.S3FileSystem(
        endpoint_url=minio_session.endpoint,
        key=minio_session.key_id,
        password=minio_session.access_key,
    )

    data = {"urls": [f"s3://image-bucket/{i}.jpeg" for i in range(3)]}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(fs=fs))

    assert df.to_pydict() == {**data, "data": [image_data, image_data, image_data]}
