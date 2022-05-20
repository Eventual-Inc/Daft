from typing import Optional

from pydantic import BaseSettings

class DaftSettings(BaseSettings):
    DAFT_CLUSTER_NAME: Optional[str] = None
    DAFT_CLUSTER_HEAD_ADDR: Optional[str] = None
    DAFT_DATAREPOS_BUCKET: str = "eventual-data-test-bucket"
    DAFT_DATAREPOS_PREFIX: str = "datarepos"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
