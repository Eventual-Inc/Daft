from pkg_resources import require
from pydantic import BaseSettings, validator

from typing import Dict, Optional

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

    @validator('environment')
    def auth0_vars_required_if_running_in_cluster(cls, v: str, values: Dict[str, str], **kwargs):
        if v != "local_dev":
            for required_key in [
                "auth0_domain",
                "auth0_api_audience",
                "auth0_issuer",
                "auth0_algorithm",
            ]:
                if required_key not in values or values[required_key] is None:
                    raise ValueError(f"Required setting: {required_key}")
        return v
