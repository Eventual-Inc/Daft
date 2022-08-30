from typing import Optional

from pydantic import BaseSettings


class _DaftSettings(BaseSettings):
    DAFT_CLUSTER_HEAD_ADDR: str = "ray://default-ray-head-svc.ray:10001"
    DAFT_DATAREPOS_BUCKET: str = "eventual-data-test-bucket"
    DAFT_DATAREPOS_PREFIX: str = "datarepos"

    DAFT_PACKAGE_ZIP_S3_LOCATION: Optional[str] = "s3://eventual-release-artifacts-bucket/daft_package-amd64/latest.zip"
    DAFT_RUNNER: str = "PY"
    CI: bool = False


DaftSettings = _DaftSettings()
