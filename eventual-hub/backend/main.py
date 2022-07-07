from fastapi import Depends, FastAPI, Response, status
from fastapi.security import HTTPBearer

from auth import VerifyToken

app = FastAPI()
token_auth_scheme = HTTPBearer()


@app.get("/api")
async def root():
    return {"message": "Hello World"}


@app.get("/api/private")
async def private_endpoint(response: Response, token: str = Depends(token_auth_scheme)):
    result = VerifyToken(token.credentials).verify()
    if result.get("status"):
       response.status_code = status.HTTP_400_BAD_REQUEST
       return result
    return {"message": "Nice and authenticated"}
