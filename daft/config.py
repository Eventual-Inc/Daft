from typing import Optional

from pydantic import BaseSettings


class DaftSettings(BaseSettings):
    DAFT_CLUSTER_HEAD_ADDR: str = "ray://default-ray-head-svc.ray:10001"
    DAFT_DATAREPOS_BUCKET: str = "eventual-data-test-bucket"
    DAFT_DATAREPOS_PREFIX: str = "datarepos"

    # TODO(jaychia): These should be refactored into a remote zip file
    # The working directory that contains Daft
    DAFT_WORKDIR: Optional[str] = None
    # A requirements.txt file that Daft uses to specify all app requirements
    DAFT_REQUIREMENTS_TXT: Optional[str] = None
