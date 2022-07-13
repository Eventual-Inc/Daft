from typing import List

import requests
from fastapi import Depends, FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer
from pydantic import BaseModel

from .auth import VerifyToken
from .models import (RayCluster, RayClusterInfo, RayClusterType,
                     UserNotebookDetails)
from .settings import settings
from .utils import kuberay, kubernetes

app = FastAPI()
origins = [
    "https://app.eventualcomputing.com",
    "https://localhost:3000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

token_auth_scheme = HTTPBearer()

JUPYTERHUB_SERVICE_ADDR = settings.jupyterhub_service_address
JUPYTERHUB_TOKEN = settings.jupyterhub_admin_token
AUTH0_EMAIL_KEY = "https://auth.eventualcomputing.com/claims/email"
TLS_CRT = "/var/run/secrets/certs/tls/tls.crt"
TLS_KEY = "/var/run/secrets/certs/tls/tls.key"
TLS_VERIFY_CRT = "/var/run/secrets/certs/tls/ca.crt"


class LaunchNotebookRequest(BaseModel):
    image: str = "jupyter/singleuser:latest"


@app.post("/api/notebooks")
async def launch_notebook_server(
    item: LaunchNotebookRequest, response: Response, token: str = Depends(token_auth_scheme)
):
    result = VerifyToken(token.credentials).verify()
    if result.get("status"):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return result

    if AUTH0_EMAIL_KEY not in result:
        response.status_code = status.HTTP_500_INTERNAL_ERROR
        return {"error": "Internal Error: Auth0 token missing email"}

    email = result[AUTH0_EMAIL_KEY]

    # HACK(jaychia): Ensure that user is created, probably a better way to do this here?
    create_user = requests.post(
        f"{JUPYTERHUB_SERVICE_ADDR}/users/{email}",
        headers={"Authorization": f"Bearer {JUPYTERHUB_TOKEN}"},
        cert=(TLS_CRT, TLS_KEY),
        verify=False,
    )

    create_server_response = requests.post(
        f"{JUPYTERHUB_SERVICE_ADDR}/users/{email}/server",
        headers={"Authorization": f"Bearer {JUPYTERHUB_TOKEN}"},
        cert=(TLS_CRT, TLS_KEY),
        verify=False,
    )
    if create_server_response.status_code // 100 != 2:
        response.status_code = status.HTTP_500_INTERNAL_ERROR
        return create_server_response.json()

    user_response = requests.get(
        f"{JUPYTERHUB_SERVICE_ADDR}/users/{email}",
        headers={"Authorization": f"Bearer {JUPYTERHUB_TOKEN}"},
        cert=(TLS_CRT, TLS_KEY),
        verify=False,
    )
    if user_response.status_code != 200:
        response.status_code = status.HTTP_500_INTERNAL_ERROR
        return user_response.json()

    return user_response.json()["server"]


@app.get("/api/notebooks")
async def get_notebook_server(response: Response, token: str = Depends(token_auth_scheme)) -> UserNotebookDetails:
    result = VerifyToken(token.credentials).verify()
    if result.get("status"):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return result

    if AUTH0_EMAIL_KEY not in result:
        response.status_code = status.HTTP_500_INTERNAL_ERROR
        return {"error": "Internal Error: Auth0 token missing email"}

    email = result[AUTH0_EMAIL_KEY]

    # HACK(jaychia): Ensure that user is created, probably a better way to do this here?
    create_user = requests.post(
        f"{JUPYTERHUB_SERVICE_ADDR}/users/{email}",
        headers={"Authorization": f"Bearer {JUPYTERHUB_TOKEN}"},
        cert=(TLS_CRT, TLS_KEY),
        verify=False,
    )

    user_response = requests.get(
        f"{JUPYTERHUB_SERVICE_ADDR}/users/{email}",
        headers={"Authorization": f"Bearer {JUPYTERHUB_TOKEN}"},
        cert=(TLS_CRT, TLS_KEY),
        verify=False,
    )
    user_response.raise_for_status()

    servers = user_response.json().get("servers", {})
    server = servers.get("", {})

    return UserNotebookDetails(
        url=server.get("url"),
        pending=server.get("pending"),
        ready=server.get("ready", False),
    )


@app.delete("/api/notebooks")
async def delete_notebook_server(response: Response, token: str = Depends(token_auth_scheme)):
    result = VerifyToken(token.credentials).verify()
    if result.get("status"):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return result

    if AUTH0_EMAIL_KEY not in result:
        response.status_code = status.HTTP_500_INTERNAL_ERROR
        return {"error": "Internal Error: Auth0 token missing email"}

    email = result[AUTH0_EMAIL_KEY]

    # HACK(jaychia): Ensure that user is created, probably a better way to do this here?
    create_user = requests.post(
        f"{JUPYTERHUB_SERVICE_ADDR}/users/{email}",
        headers={"Authorization": f"Bearer {JUPYTERHUB_TOKEN}"},
        cert=(TLS_CRT, TLS_KEY),
        verify=False,
    )

    delete_server_response = requests.delete(
        f"{JUPYTERHUB_SERVICE_ADDR}/users/{email}/server",
        headers={"Authorization": f"Bearer {JUPYTERHUB_TOKEN}"},
        cert=(TLS_CRT, TLS_KEY),
        verify=False,
    )
    delete_server_response.raise_for_status()
    return "ok"


class LaunchRayClusterRequest(BaseModel):
    name: str
    cluster_type: RayClusterType


@app.post("/api/rayclusters", status_code=status.HTTP_201_CREATED)
async def launch_ray_cluster(
    item: LaunchRayClusterRequest, response: Response, token: str = Depends(token_auth_scheme)
) -> RayCluster:
    result = VerifyToken(token.credentials).verify()
    if result.get("status"):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return result

    if AUTH0_EMAIL_KEY not in result:
        response.status_code = status.HTTP_500_INTERNAL_ERROR
        return {"error": "Internal Error: Auth0 token missing email"}

    email = result[AUTH0_EMAIL_KEY]

    # Get user organization's namespace
    namespace = "default"

    return await kubernetes.passthrough_status_code(response)(kuberay.launch_ray_cluster)(
        name=item.name, namespace=namespace, cluster_type=item.cluster_type
    )


@app.get("/api/rayclusters/list", status_code=status.HTTP_200_OK)
async def list_ray_clusters(response: Response, token: str = Depends(token_auth_scheme)) -> List[RayCluster]:
    result = VerifyToken(token.credentials).verify()
    if result.get("status"):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return result

    if AUTH0_EMAIL_KEY not in result:
        response.status_code = status.HTTP_500_INTERNAL_ERROR
        return {"error": "Internal Error: Auth0 token missing email"}

    email = result[AUTH0_EMAIL_KEY]

    # Get user organization's namespace
    namespace = "default"

    return await kubernetes.passthrough_status_code(response)(kuberay.list_ray_clusters)(namespace=namespace)


@app.get("/api/rayclusters", status_code=status.HTTP_200_OK)
async def get_ray_cluster(name: str, response: Response, token: str = Depends(token_auth_scheme)) -> RayClusterInfo:
    result = VerifyToken(token.credentials).verify()
    if result.get("status"):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return result

    if AUTH0_EMAIL_KEY not in result:
        response.status_code = status.HTTP_500_INTERNAL_ERROR
        return {"error": "Internal Error: Auth0 token missing email"}

    email = result[AUTH0_EMAIL_KEY]

    # Get user organization's namespace
    namespace = "default"

    return await kubernetes.passthrough_status_code(response)(kuberay.get_ray_cluster)(name=name, namespace=namespace)


class DeleteRayClusterRequest(BaseModel):
    name: str


@app.delete("/api/rayclusters", status_code=status.HTTP_200_OK)
async def delete_ray_cluster(
    item: DeleteRayClusterRequest, response: Response, token: str = Depends(token_auth_scheme)
):
    result = VerifyToken(token.credentials).verify()
    if result.get("status"):
        response.status_code = status.HTTP_400_BAD_REQUEST
        return result

    if AUTH0_EMAIL_KEY not in result:
        response.status_code = status.HTTP_500_INTERNAL_ERROR
        return {"error": "Internal Error: Auth0 token missing email"}

    email = result[AUTH0_EMAIL_KEY]

    # Get user organization's namespace
    namespace = "default"

    return await kubernetes.passthrough_status_code(response)(kuberay.delete_ray_cluster)(
        name=item.name, namespace=namespace
    )
