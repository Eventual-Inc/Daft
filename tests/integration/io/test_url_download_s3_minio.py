from __future__ import annotations

import daft


def test_url_download_minio_custom_s3fs(minio_s3_config, minio_image_data_fixture, image_data):
    import s3fs

    urls = minio_image_data_fixture
    fs = s3fs.S3FileSystem(
        endpoint_url=minio_s3_config.endpoint,
        key=minio_s3_config.key_id,
        password=minio_s3_config.access_key,
    )

    data = {"urls": urls}
    df = daft.from_pydict(data)
    df = df.with_column("data", df["urls"].url.download(fs=fs))

    assert df.to_pydict() == {**data, "data": [image_data for _ in range(len(urls))]}
