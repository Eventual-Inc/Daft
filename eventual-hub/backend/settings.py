from typing import Dict, Optional

from pkg_resources import require
from pydantic import BaseSettings, validator


class Settings(BaseSettings):

    environment: str

    # Auth0 API settings
    # https://auth0.com/blog/build-and-secure-fastapi-server-with-auth0/
    auth0_domain: Optional[str]
    auth0_api_audience: Optional[str]
    auth0_issuer: Optional[str]
    auth0_algorithm: Optional[str]

    # Admin token to be used in Authorization header when hitting Jupyterhub
    jupyterhub_admin_token: str
    jupyterhub_service_address: str

    # Path to Kuberay client configuration
    kuberay_client_config_path: str


settings = Settings()
