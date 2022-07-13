from pydantic import BaseSettings


class Settings(BaseSettings):

    # Auth0 API settings
    # https://auth0.com/blog/build-and-secure-fastapi-server-with-auth0/
    auth0_domain: str
    auth0_api_audience: str
    auth0_issuer: str
    auth0_algorithm: str

    # Admin token to be used in Authorization header when hitting Jupyterhub
    jupyterhub_admin_token: str
    jupyterhub_service_address: str

    # Path to Kuberay client configuration
    kuberay_client_config_path: str


settings = Settings()
