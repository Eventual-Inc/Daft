# flake8: noqa

# Jupyterhub configuration: https://jupyterhub.readthedocs.io/en/stable/getting-started/config-basics.html#generate-a-default-config-file

import os

# Networking
c.JupyterHub.bind_url = "http://:8081"
c.JupyterHub.hub_bind_url = "http://:8081"
c.JupyterHub.hub_connect_url = "https://jupyterhub:8081"
c.JupyterHub.base_url = "/jupyter/"

# Authentication
if "AUTH0_CLIENT_ID" in os.environ:
    assert "AUTH0_CLIENT_ID" in os.environ
    assert "AUTH0_CLIENT_SECRET" in os.environ
    assert "AUTH0_CALLBACK_URL" in os.environ
    from oauthenticator.auth0 import Auth0OAuthenticator

    c.JupyterHub.authenticator_class = Auth0OAuthenticator
    c.Auth0OAuthenticator.client_id = os.environ["AUTH0_CLIENT_ID"]
    c.Auth0OAuthenticator.client_secret = os.environ["AUTH0_CLIENT_SECRET"]
    c.Auth0OAuthenticator.oauth_callback_url = os.environ["AUTH0_CALLBACK_URL"]
    c.Auth0OAuthenticator.scope = ["openid", "email"]
else:
    c.JupyterHub.authenticator_class = "jupyterhub.auth.DummyAuthenticator"

# Spawner
c.JupyterHub.spawner_class = "kubespawner.KubeSpawner"
c.KubeSpawner.start_timeout = 60 * 10
c.KubeSpawner.k8s_api_request_timeout = 30

# Proxy
c.JupyterHub.proxy_class = "kubespawner.proxy.KubeIngressProxy"
c.Proxy.should_start = False
c.KubeIngressProxy.extra_annotations = {
    "kubernetes.io/ingress.class": "eventual-hub",
    "traefik.ingress.kubernetes.io/router.entrypoints": "https",
    "traefik.ingress.kubernetes.io/service.serversscheme": "https",
    "traefik.ingress.kubernetes.io/service.serverstransport": "eventual-hub-proxy@file",
}

# Database connection if running in a cluster environment
if "JUPYTERHUB_DB_SERVICE_HOST" in os.environ:
    assert "JUPYTERHUB_DB_SERVICE_HOST" in os.environ
    assert "JUPYTERHUB_DB_USERNAME" in os.environ
    assert "JUPYTERHUB_DB_PASSWORD" in os.environ
    assert "JUPYTERHUB_DB_TABLE" in os.environ
    c.JupyterHub.db_url = (
        f"postgresql://{os.environ['JUPYTERHUB_DB_USERNAME']}:{os.environ['JUPYTERHUB_DB_PASSWORD']}"
        f"@{os.environ['JUPYTERHUB_DB_SERVICE_HOST']}:5432/{os.environ['JUPYTERHUB_DB_TABLE']}?sslmode=require"
    )

# Miscellaneous
c.JupyterHub.cleanup_servers = False
c.JupyterHub.concurrent_spawn_limit = 1
c.JupyterHub.internal_ssl = True
c.JupyterHub.external_ssl_authorities = {
    name: {
        "cert": f"/srv/jupyterhub/certs/{name}/tls.crt",
        "key": f"/srv/jupyterhub/certs/{name}/tls.key",
    }
    for name in (
        "hub-ca",
        "notebooks-ca",
        "proxy-api-ca",
        "proxy-client-ca",
        "services-ca",
    )
}
c.JupyterHub.recreate_internal_certs = True
c.JupyterHub.trusted_alt_names = ["DNS:jupyterhub"]

# Grant our backend admin access
# https://jupyterhub.readthedocs.io/en/stable/reference/rest.html#updating-to-admin-services
c.JupyterHub.services = [
    {
        # give the token a name
        "name": "eventual-backend",
        "api_token": os.environ["JUPYTERHUB_ADMIN_TOKEN"],
    },
]
c.JupyterHub.load_roles = [
    {
        "name": "eventual-backend-admin-role",
        "scopes": [
            # specify the permissions the token should have
            "admin:users",
            "admin:servers",
        ],
        "services": [
            # assign the service the above permissions
            "eventual-backend",
        ],
    }
]
