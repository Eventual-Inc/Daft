import jwt

from settings import Settings

from pydantic import BaseModel
from fastapi.exceptions import HTTPException

from typing import Callable


AUTH0_EMAIL_KEY = "https://auth.eventualcomputing.com/claims/email"


class DecodedToken(BaseModel):
    email: str


def get_token_verifier() -> Callable[[str], DecodedToken]:
    settings = Settings()
    if settings.environment == "local_dev":
        return LocalDevTokenVerifier(settings)
    return TokenVerifier(settings)


class TokenVerifier:

    def __init__(self, settings: Settings):
        assert self.settings.auth0_domain is not None
        assert self.settings.auth0_algorithm is not None
        assert self.settings.auth0_api_audience is not None
        assert self.settings.auth0_issuer is not None
        self.settings = settings

    def __call__(self, token: str) -> DecodedToken:
        jwks_url = f'https://{self.settings.auth0_domain}/.well-known/jwks.json'
        jwks_client = jwt.PyJWKClient(jwks_url)

        try:
            signing_key = jwks_client.get_signing_key_from_jwt(token).key
        except jwt.exceptions.PyJWKClientError as error:
            raise HTTPException(403, detail=error.__str__())
        except jwt.exceptions.DecodeError as error:
            raise HTTPException(403, detail=error.__str__())

        try:
            payload = jwt.decode(
                token,
                signing_key,
                algorithms=self.settings.auth0_algorithm,
                audience=self.settings.auth0_api_audience,
                issuer=self.settings.auth0_issuer,
            )
        except Exception as error:
            raise HTTPException(403, detail=error.__str__())

        return DecodedToken(email=payload[AUTH0_EMAIL_KEY])


class LocalDevTokenVerifier:

    def __init__(self, settings: Settings):
        assert settings.environment == "local_dev"
        self.settings = settings

    def __call__(self, token: str) -> DecodedToken:
        return DecodedToken(email="dummy@dummy.com")
